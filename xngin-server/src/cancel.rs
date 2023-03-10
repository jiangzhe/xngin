use event_listener::{Event, EventListener};
use futures_lite::Stream;
use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use xngin_protocol::mysql::error::{Error, Result};

/// Cancellation represents a handle that can cancel future and stream
/// processing.
#[derive(Debug, Clone)]
pub struct Cancellation {
    inner: Arc<Inner>,
}

impl Default for Cancellation {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Cancellation {
    #[inline]
    pub fn new() -> Self {
        Cancellation {
            inner: Arc::new(Inner::new()),
        }
    }

    #[inline]
    pub fn cancel(&self, err: Error) {
        self.inner.cancel(err)
    }

    #[inline]
    pub fn select_stream<T, S>(&self, stream: S) -> CancellableStream<S>
    where
        S: Stream<Item = T>,
    {
        let listener = self.inner.event.listen();
        let cancelled = self.inner.cancelled();
        CancellableStream {
            stream,
            listener,
            cancelled,
        }
    }

    #[inline]
    pub fn cancelled(&self) -> bool {
        self.inner.cancelled()
    }

    #[inline]
    pub fn err(&self) -> Option<&Error> {
        self.inner.err()
    }
}

#[derive(Debug)]
struct Inner {
    flag: AtomicBool,
    event: Event,
    err: AtomicPtr<Error>,
}

impl Inner {
    #[inline]
    const fn new() -> Self {
        Inner {
            flag: AtomicBool::new(false),
            event: Event::new(),
            err: AtomicPtr::new(ptr::null_mut()),
        }
    }

    #[inline]
    fn cancelled(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }

    #[inline]
    fn cancel(&self, err: Error) {
        if self
            .flag
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let boxed = Box::new(err);
            let new = Box::leak(boxed);
            self.err.store(new, Ordering::Release);
            self.event.notify(usize::MAX)
        }
    }

    #[inline]
    fn err(&self) -> Option<&Error> {
        let err_ptr = self.err.load(Ordering::Acquire);
        if err_ptr.is_null() {
            return None;
        }
        // safety:
        //
        // err_ptr won't be changed if set, so the pointer is always valid
        unsafe { Some(&*err_ptr) }
    }
}

unsafe impl Send for Inner {}
unsafe impl Sync for Inner {}

pin_project! {
    pub struct CancellableStream<S> {
        #[pin]
        stream: S,
        #[pin]
        listener: EventListener,
        cancelled: bool,
    }
}

impl<T, S> Stream for CancellableStream<S>
where
    S: Stream<Item = T>,
{
    type Item = Result<T>;
    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<T>>> {
        let this = self.project();
        if *this.cancelled {
            // returns None as the stream is cancelled.
            return Poll::Ready(None);
        }
        if this.listener.poll(cx).is_ready() {
            *this.cancelled = true;
            return Poll::Ready(Some(Err(Error::Cancelled())));
        }
        this.stream.poll_next(cx).map(|v| v.map(Ok))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::single_thread_executor;
    use async_io::Timer;
    use futures_lite::StreamExt;
    use std::time::Duration;

    #[test]
    fn test_cancel_stream() {
        let ex = single_thread_executor();
        async_io::block_on(async {
            let (tx, rx) = flume::unbounded();
            // send 10 items in one thread
            ex.spawn(async move {
                for _ in 0..10 {
                    Timer::after(Duration::from_millis(10)).await;
                    let _ = tx.send_async(()).await;
                }
            })
            .detach();
            // cancel in another thread
            let cancel = Cancellation::default();
            {
                let cancel = cancel.clone();
                ex.spawn(async move {
                    Timer::after(Duration::from_millis(50)).await;
                    cancel.cancel(Error::Cancelled());
                })
                .detach();
            }
            // select stream with cancellation
            let stream = cancel.select_stream(rx.into_stream());
            // last element is guaranteed to be cancelled
            let cancelled = stream
                .filter(|v| matches!(v, Err(Error::Cancelled())))
                .next()
                .await;
            assert!(cancelled.is_some());
            assert!(matches!(cancel.err(), Some(Error::Cancelled())));
        })
    }
}
