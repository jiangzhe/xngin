use super::atomic::{Atomic, Pointable, Shared};
use super::guard::{unprotected, Guard};
use std::marker::PhantomData;
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub(crate) struct Entry {
    next: Atomic<Entry>,
}

impl_sized_pointable!(Entry);

pub(crate) trait IsElement<T> {
    fn entry_of(_: &T) -> &Entry;

    unsafe fn element_of(_: &Entry) -> &T;

    unsafe fn finalize(_: &Entry, _: &Guard);
}

#[derive(Debug)]
pub(crate) struct List<T, C: IsElement<T> = T> {
    head: Atomic<Entry>,
    _marker: PhantomData<(T, C)>,
}

pub(crate) struct Iter<'g, T, C: IsElement<T>> {
    guard: &'g Guard,
    pred: &'g Atomic<Entry>,
    curr: Shared<'g, Entry>,
    head: &'g Atomic<Entry>,
    _marker: PhantomData<(&'g T, C)>,
}

#[derive(PartialEq, Debug)]
pub(crate) enum IterError {
    Stalled,
}

impl Default for Entry {
    fn default() -> Self {
        Self {
            next: Atomic::null(),
        }
    }
}

impl Entry {
    pub(crate) unsafe fn delete(&self, guard: &Guard) {
        self.next.fetch_or(1, Ordering::Release, guard);
    }
}

impl<T: Pointable, C: IsElement<T>> List<T, C> {
    pub(crate) fn new() -> Self {
        Self {
            head: Atomic::null(),
            _marker: PhantomData,
        }
    }

    pub(crate) unsafe fn insert<'g>(&'g self, container: Shared<'g, T>, guard: &'g Guard) {
        let to = &self.head;
        let entry: &Entry = C::entry_of(container.deref());
        let entry_ptr = Shared::from(entry as *const _);
        let mut next = to.load(Ordering::Relaxed, guard);
        loop {
            entry.next.store(next, Ordering::Relaxed);
            match to.compare_exchange_weak(
                next,
                entry_ptr,
                Ordering::Release,
                Ordering::Relaxed,
                guard,
            ) {
                Ok(_) => break,
                Err(err) => next = err.current,
            }
        }
    }

    pub(crate) fn iter<'g>(&'g self, guard: &'g Guard) -> Iter<'g, T, C> {
        Iter {
            guard,
            pred: &self.head,
            curr: self.head.load(Ordering::Acquire, guard),
            head: &self.head,
            _marker: PhantomData,
        }
    }
}

impl<T, C: IsElement<T>> Drop for List<T, C> {
    fn drop(&mut self) {
        unsafe {
            let guard = unprotected();
            let mut curr = self.head.load(Ordering::Relaxed, guard);
            while let Some(c) = curr.as_ref() {
                let succ = c.next.load(Ordering::Relaxed, guard);
                assert_eq!(succ.tag(), 1);
                C::finalize(curr.deref(), guard);
                curr = succ;
            }
        }
    }
}

impl<'g, T: 'g, C: IsElement<T>> Iterator for Iter<'g, T, C> {
    type Item = Result<&'g T, IterError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(c) = unsafe { self.curr.as_ref() } {
            let succ = c.next.load(Ordering::Acquire, self.guard);
            if succ.tag() == 1 {
                let succ = succ.with_tag(0);
                debug_assert!(self.curr.tag() == 0);
                let succ = match self.pred.compare_exchange(
                    self.curr,
                    succ,
                    Ordering::Acquire,
                    Ordering::Acquire,
                    self.guard,
                ) {
                    Ok(_) => {
                        unsafe {
                            C::finalize(self.curr.deref(), self.guard);
                        }
                        succ
                    }
                    Err(e) => e.current,
                };
                if succ.tag() != 0 {
                    self.pred = self.head;
                    self.curr = self.head.load(Ordering::Acquire, self.guard);
                    return Some(Err(IterError::Stalled));
                }
                self.curr = succ;
                continue;
            }
            self.pred = &c.next;
            self.curr = succ;
            return Some(Ok(unsafe { C::element_of(c) }));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::epoch::{self, Collector, Owned};
    use memoffset::offset_of;

    impl_sized_pointable!(A);

    #[derive(Default)]
    struct A {
        entry: Entry,
        data: usize,
    }

    impl IsElement<A> for A {
        fn entry_of(a: &A) -> &Entry {
            let entry_ptr = ((a as *const A as usize) + offset_of!(A, entry)) as *const Entry;
            unsafe { &*entry_ptr }
        }

        unsafe fn element_of(entry: &Entry) -> &A {
            let elem_ptr = ((entry as *const Entry as usize) - offset_of!(A, entry)) as *const A;
            &*elem_ptr
        }

        unsafe fn finalize(entry: &Entry, guard: &Guard) {
            guard.defer_destroy(Shared::from(Self::element_of(entry) as *const _));
        }
    }

    #[test]
    fn test_list() {
        let collector = Collector::default();
        let handle = collector.register();
        let ls: List<A, A> = List::new();
        let guard = handle.pin();

        let elem1 = Owned::new(A::default()).into_shared(&guard);
        let elem2 = Owned::new(A::default()).into_shared(&guard);
        unsafe {
            ls.insert(elem1, &guard);
            ls.insert(elem2, &guard);
        }

        unsafe {
            A::entry_of(elem1.as_ref().unwrap()).delete(&guard);
            A::entry_of(elem2.as_ref().unwrap()).delete(&guard);
        }
    }
}
