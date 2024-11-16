use super::atomic::{Atomic, Owned, Shared};
use super::guard::{unprotected, Guard};
use crossbeam_utils::CachePadded;
use std::mem::MaybeUninit;
use std::sync::atomic::Ordering;

pub(crate) struct Queue<T> {
    head: CachePadded<Atomic<Node<T>>>,
    tail: CachePadded<Atomic<Node<T>>>,
}

unsafe impl<T: Send> Sync for Queue<T> {}
unsafe impl<T: Send> Send for Queue<T> {}

impl<T> Queue<T> {
    pub(crate) fn new() -> Queue<T> {
        let q = Queue {
            head: CachePadded::new(Atomic::null()),
            tail: CachePadded::new(Atomic::null()),
        };
        let sentinel = Owned::new(Node {
            data: MaybeUninit::uninit(),
            next: Atomic::null(),
        });
        unsafe {
            let guard = unprotected();
            let sentinel = sentinel.into_shared(guard);
            q.head.store(sentinel, Ordering::Relaxed);
            q.tail.store(sentinel, Ordering::Relaxed);
            q
        }
    }

    #[inline(always)]
    fn push_internal(
        &self,
        onto: Shared<'_, Node<T>>,
        new: Shared<'_, Node<T>>,
        guard: &Guard,
    ) -> bool {
        let o = unsafe { onto.deref() };
        let next = o.next.load(Ordering::Acquire, guard);
        if unsafe { next.as_ref().is_some() } {
            let _ =
                self.tail
                    .compare_exchange(onto, next, Ordering::Release, Ordering::Relaxed, guard);
            false
        } else {
            let result = o
                .next
                .compare_exchange(
                    Shared::null(),
                    new,
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                )
                .is_ok();
            if result {
                let _ = self.tail.compare_exchange(
                    onto,
                    new,
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                );
            }
            result
        }
    }

    pub(crate) fn push(&self, t: T, guard: &Guard) {
        let new = Owned::new(Node {
            data: MaybeUninit::new(t),
            next: Atomic::null(),
        });
        let new = new.into_shared(guard);
        loop {
            let tail = self.tail.load(Ordering::Acquire, guard);
            if self.push_internal(tail, new, guard) {
                break;
            }
        }
    }

    #[inline(always)]
    fn pop_if_internal<F>(&self, condition: F, guard: &Guard) -> Result<Option<T>, ()>
    where
        // T: Sync,
        F: Fn(&T) -> bool,
    {
        let head = self.head.load(Ordering::Acquire, guard);
        let h = unsafe { head.deref() };
        let next = h.next.load(Ordering::Acquire, guard);
        match unsafe { next.as_ref() } {
            Some(n) if condition(unsafe { &*n.data.as_ptr() }) => unsafe {
                self.head
                    .compare_exchange(head, next, Ordering::Release, Ordering::Relaxed, guard)
                    .map(|_| {
                        let tail = self.tail.load(Ordering::Relaxed, guard);
                        if head == tail {
                            let _ = self.tail.compare_exchange(
                                tail,
                                next,
                                Ordering::Release,
                                Ordering::Relaxed,
                                guard,
                            );
                        }
                        guard.defer_destroy(head);
                        Some(n.data.as_ptr().read())
                    })
                    .map_err(|_| ())
            },
            None | Some(_) => Ok(None),
        }
    }

    pub(crate) fn try_pop_if<F>(&self, condition: F, guard: &Guard) -> Option<T>
    where
        T: Sync,
        F: Fn(&T) -> bool,
    {
        loop {
            if let Ok(head) = self.pop_if_internal(&condition, guard) {
                return head;
            }
        }
    }

    pub(crate) fn try_pop(&self, guard: &Guard) -> Option<T> {
        loop {
            if let Ok(head) = self.pop_if_internal(|_| true, guard) {
                return head;
            }
        }
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        unsafe {
            let guard = unprotected();

            while self.try_pop(guard).is_some() {}

            // Destroy the remaining sentinel node.
            let sentinel = self.head.load(Ordering::Relaxed, guard);
            if let Some(node) = sentinel.try_into_owned() {
                drop(node);
            }
        }
    }
}

struct Node<T> {
    data: MaybeUninit<T>,
    next: Atomic<Node<T>>,
}

impl_sized_pointable!(Node<T>);

#[cfg(test)]
mod tests {
    use super::Queue;
    use crate::epoch;

    #[test]
    fn test_queue() {
        let queue = Queue::new();
        let guard = epoch::pin();
        queue.push(1, &guard);
        queue.push(2, &guard);
        drop(guard);

        let g2 = epoch::pin();
        assert!(queue.try_pop_if(|elem| *elem < 10, &g2).is_some());
        assert!(queue.try_pop_if(|elem| *elem > 10, &g2).is_none());
    }
}
