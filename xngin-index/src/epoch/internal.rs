use super::atomic::{Owned, Shared};
use super::collector::{Collector, LocalHandle};
use super::guard::{unprotected, Guard};
use super::list::{Entry, IsElement};
use super::list::{IterError, List};
use super::queue::Queue;
use super::{AtomicEpoch, Epoch};
use crossbeam_utils::CachePadded;
use memoffset::offset_of;
use std::cell::{Cell, UnsafeCell};
use std::fmt;
use std::marker::PhantomData;
use std::mem::{self, ManuallyDrop, MaybeUninit};
use std::num::Wrapping;
use std::ptr;
use std::sync::atomic::{self, Ordering};

const MAX_OBJECTS: usize = 64;

pub(crate) struct Bag {
    deferreds: [Deferred; MAX_OBJECTS],
    len: usize,
}

unsafe impl Send for Bag {}

impl Bag {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) unsafe fn try_push(&mut self, deferred: Deferred) -> Result<(), Deferred> {
        if self.len < MAX_OBJECTS {
            self.deferreds[self.len] = deferred;
            self.len += 1;
            Ok(())
        } else {
            Err(deferred)
        }
    }

    fn seal(self, epoch: Epoch) -> SealedBag {
        SealedBag { epoch, _bag: self }
    }
}

impl Default for Bag {
    fn default() -> Self {
        Bag {
            len: 0,
            deferreds: [Deferred::NO_OP; MAX_OBJECTS],
        }
    }
}

impl Drop for Bag {
    fn drop(&mut self) {
        for deferred in &mut self.deferreds[..self.len] {
            let no_op = Deferred::NO_OP;
            let owned_deferred = mem::replace(deferred, no_op);
            owned_deferred.call();
        }
    }
}

impl fmt::Debug for Bag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bag")
            .field("deferreds", &&self.deferreds[..self.len])
            .finish()
    }
}

#[derive(Default, Debug)]
struct SealedBag {
    epoch: Epoch,
    _bag: Bag,
}

unsafe impl Sync for SealedBag {}

impl SealedBag {
    fn is_expired(&self, global_epoch: Epoch) -> bool {
        global_epoch.wrapping_sub(self.epoch) >= 2
    }
}

pub(crate) struct Global {
    locals: List<Local>,
    queue: Queue<SealedBag>,
    pub(crate) epoch: CachePadded<AtomicEpoch>,
}

impl Global {
    const COLLECT_STEPS: usize = 8;

    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            locals: List::new(),
            queue: Queue::new(),
            epoch: CachePadded::new(AtomicEpoch::new(Epoch::starting())),
        }
    }

    pub(crate) fn push_bag(&self, bag: &mut Bag, guard: &Guard) {
        let bag = mem::replace(bag, Bag::new());
        atomic::fence(Ordering::SeqCst);
        let epoch = self.epoch.load(Ordering::Relaxed);
        self.queue.push(bag.seal(epoch), guard);
    }

    #[cold]
    pub(crate) fn collect(&self, guard: &Guard) {
        let global_epoch = self.try_advance(guard);
        let steps = Self::COLLECT_STEPS;
        for _ in 0..steps {
            match self.queue.try_pop_if(
                |sealed_bag: &SealedBag| sealed_bag.is_expired(global_epoch),
                guard,
            ) {
                None => break,
                Some(sealed_bag) => drop(sealed_bag),
            }
        }
    }

    #[cold]
    pub(crate) fn try_advance(&self, guard: &Guard) -> Epoch {
        let global_epoch = self.epoch.load(Ordering::Relaxed);
        atomic::fence(Ordering::SeqCst);
        for local in self.locals.iter(guard) {
            match local {
                Err(IterError::Stalled) => {
                    return global_epoch;
                }
                Ok(local) => {
                    let local_epoch = local.epoch.load(Ordering::Relaxed);
                    if local_epoch.is_pinned() && local_epoch.unpinned() != global_epoch {
                        return global_epoch;
                    }
                }
            }
        }
        atomic::fence(Ordering::Acquire);

        let new_epoch = global_epoch.successor();
        self.epoch.store(new_epoch, Ordering::Release);
        new_epoch
    }
}

pub(crate) struct Local {
    entry: Entry,
    epoch: AtomicEpoch,
    collector: UnsafeCell<ManuallyDrop<Collector>>,
    pub(crate) bag: UnsafeCell<Bag>,
    guard_count: Cell<usize>,
    handle_count: Cell<usize>,
    pin_count: Cell<Wrapping<usize>>,
}

impl Local {
    const PINNINGS_BETWEEN_COLLECT: usize = 128;

    pub(crate) fn register(collector: &Collector) -> LocalHandle {
        unsafe {
            let local = Owned::new(Local {
                entry: Entry::default(),
                epoch: AtomicEpoch::new(Epoch::starting()),
                collector: UnsafeCell::new(ManuallyDrop::new(collector.clone())),
                bag: UnsafeCell::new(Bag::new()),
                guard_count: Cell::new(0),
                handle_count: Cell::new(1),
                pin_count: Cell::new(Wrapping(0)),
            })
            .into_shared(unprotected());
            collector.global.locals.insert(local, unprotected());
            LocalHandle {
                local: local.as_raw(),
            }
        }
    }

    #[inline]
    pub(crate) fn collector(&self) -> &Collector {
        let c = self.collector.get();
        unsafe { &*c }
    }

    #[inline]
    pub(crate) fn global(&self) -> &Global {
        &self.collector().global
    }

    #[inline]
    pub(crate) fn is_pinned(&self) -> bool {
        self.guard_count.get() > 0
    }

    pub(crate) unsafe fn defer(&self, mut deferred: Deferred, guard: &Guard) {
        let bag = &mut *self.bag.get();
        while let Err(d) = bag.try_push(deferred) {
            self.global().push_bag(bag, guard);
            deferred = d;
        }
    }

    pub(crate) fn flush(&self, guard: &Guard) {
        let bag = unsafe { &mut *self.bag.get() };
        if !bag.is_empty() {
            self.global().push_bag(bag, guard);
        }
        self.global().collect(guard);
    }

    #[inline]
    pub(crate) fn pin(&self) -> Guard {
        let guard = Guard { local: self };
        let guard_count = self.guard_count.get();
        self.guard_count.set(guard_count.checked_add(1).unwrap());
        if guard_count == 0 {
            let global_epoch = self.global().epoch.load(Ordering::Relaxed);
            let new_epoch = global_epoch.pinned();
            if cfg!(any(target_arch = "x86", target_arch = "x86_64")) {
                let current = Epoch::starting();
                let res = self.epoch.compare_exchange(
                    current,
                    new_epoch,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
                debug_assert!(res.is_ok(), "participant was expected to be unpinned");
                atomic::compiler_fence(Ordering::SeqCst);
            } else {
                self.epoch.store(new_epoch, Ordering::Relaxed);
                atomic::fence(Ordering::SeqCst);
            }
            let count = self.pin_count.get();
            self.pin_count.set(count + Wrapping(1));
            if count.0 % Self::PINNINGS_BETWEEN_COLLECT == 0 {
                self.global().collect(&guard);
            }
        }
        guard
    }

    #[inline]
    pub(crate) fn unpin(&self) {
        let guard_count = self.guard_count.get();
        self.guard_count.set(guard_count - 1);
        if guard_count == 1 {
            self.epoch.store(Epoch::starting(), Ordering::Release);
            if self.handle_count.get() == 0 {
                self.finalize();
            }
        }
    }

    #[inline]
    pub(crate) fn repin(&self) {
        let guard_count = self.guard_count.get();
        if guard_count == 1 {
            let epoch = self.epoch.load(Ordering::Relaxed);
            let global_epoch = self.global().epoch.load(Ordering::Relaxed).pinned();
            if epoch != global_epoch {
                self.epoch.store(global_epoch, Ordering::Release);
            }
        }
    }

    #[inline]
    pub(crate) fn acquire_handle(&self) {
        let handle_count = self.handle_count.get();
        debug_assert!(handle_count >= 1);
        self.handle_count.set(handle_count + 1);
    }

    #[inline]
    pub(crate) fn release_handle(&self) {
        let guard_count = self.guard_count.get();
        let handle_count = self.handle_count.get();
        debug_assert!(handle_count >= 1);
        self.handle_count.set(handle_count - 1);
        if guard_count == 0 && handle_count == 1 {
            self.finalize();
        }
    }

    #[cold]
    fn finalize(&self) {
        debug_assert_eq!(self.guard_count.get(), 0);
        debug_assert_eq!(self.handle_count.get(), 0);
        self.handle_count.set(1);
        unsafe {
            let guard = &self.pin();
            let bag = &mut *self.bag.get();
            self.global().push_bag(bag, guard);
        }
        self.handle_count.set(0);
        unsafe {
            let c = self.collector.get();
            let collector: Collector = ptr::read(&*(*c));
            self.entry.delete(unprotected());
            drop(collector);
        }
    }
}

impl_sized_pointable!(Local);

impl IsElement<Local> for Local {
    fn entry_of(local: &Local) -> &Entry {
        unsafe {
            let entry_ptr = (local as *const Local as *const u8)
                .add(offset_of!(Local, entry))
                .cast::<Entry>();
            &*entry_ptr
        }
    }

    unsafe fn element_of(entry: &Entry) -> &Local {
        let local_ptr = (entry as *const Entry as *const u8)
            .sub(offset_of!(Local, entry))
            .cast::<Local>();
        &*local_ptr
    }

    unsafe fn finalize(entry: &Entry, guard: &Guard) {
        guard.defer_destroy(Shared::from(Self::element_of(entry) as *const _))
    }
}

const DATA_WORDS: usize = 3;
type Data = [usize; DATA_WORDS];

pub(crate) struct Deferred {
    call: unsafe fn(*mut u8),
    data: MaybeUninit<Data>,
    _marker: PhantomData<*mut ()>,
}

impl fmt::Debug for Deferred {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("Deferred { .. }")
    }
}

impl Deferred {
    pub(crate) const NO_OP: Self = {
        fn no_op_call(_raw: *mut u8) {}
        Self {
            call: no_op_call,
            data: MaybeUninit::uninit(),
            _marker: PhantomData,
        }
    };

    pub(crate) fn new<F: FnOnce()>(f: F) -> Self {
        let size = mem::size_of::<F>();
        let align = mem::align_of::<F>();
        unsafe {
            if size <= mem::size_of::<Data>() && align <= mem::align_of::<Data>() {
                let mut data = MaybeUninit::<Data>::uninit();
                ptr::write(data.as_mut_ptr().cast::<F>(), f);

                unsafe fn call<F: FnOnce()>(raw: *mut u8) {
                    let f: F = ptr::read(raw.cast::<F>());
                    f();
                }

                Deferred {
                    call: call::<F>,
                    data,
                    _marker: PhantomData,
                }
            } else {
                let b: Box<F> = Box::new(f);
                let mut data = MaybeUninit::<Data>::uninit();
                ptr::write(data.as_mut_ptr().cast::<Box<F>>(), b);

                unsafe fn call<F: FnOnce()>(raw: *mut u8) {
                    let b: Box<F> = ptr::read(raw.cast::<Box<F>>());
                    (*b)();
                }

                Deferred {
                    call: call::<F>,
                    data,
                    _marker: PhantomData,
                }
            }
        }
    }

    #[inline]
    pub(crate) fn call(mut self) {
        let call = self.call;
        unsafe { call(self.data.as_mut_ptr().cast::<u8>()) };
    }
}
