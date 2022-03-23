use event_listener::{Event, EventListener};
use futures_lite::Stream;
use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug)]
pub enum Cancellable<T> {
    Ready(T),
    Cancelled,
}

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
    pub fn cancel(&self) {
        self.inner.cancel()
    }

    #[inline]
    pub fn select_stream<T, S>(&self, stream: S) -> CancellableStream<S>
    where
        S: Stream<Item = T>,
    {
        CancellableStream {
            stream,
            listener: self.inner.event.listen(),
            cancelled: false,
        }
    }
}

#[derive(Debug)]
struct Inner {
    flag: AtomicBool,
    event: Event,
}

impl Inner {
    #[inline]
    const fn new() -> Self {
        Inner {
            flag: AtomicBool::new(false),
            event: Event::new(),
        }
    }

    #[inline]
    fn cancel(&self) {
        if self
            .flag
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.event.notify(usize::MAX)
        }
    }
}

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
    type Item = Cancellable<T>;
    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Cancellable<T>>> {
        let this = self.project();
        if *this.cancelled {
            // returns None as the stream is cancelled.
            return Poll::Ready(None);
        }
        if this.listener.poll(cx).is_ready() {
            *this.cancelled = true;
            return Poll::Ready(Some(Cancellable::Cancelled));
        }
        this.stream.poll_next(cx).map(|v| v.map(Cancellable::Ready))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::StreamExt;
    use std::time::Duration;

    #[test]
    fn test_cancel_stream() {
        smol::block_on(async {
            let (tx, rx) = flume::unbounded();
            // send 10 items in one thread
            smol::spawn(async move {
                for _ in 0..10 {
                    smol::Timer::after(Duration::from_millis(10)).await;
                    let _ = tx.send_async(()).await;
                }
            })
            .detach();
            // cancel in another thread
            let cancel = Cancellation::new();
            {
                let cancel = cancel.clone();
                smol::spawn(async move {
                    smol::Timer::after(Duration::from_millis(50)).await;
                    cancel.cancel();
                })
                .detach();
            }
            // select stream with cancellation
            let stream = cancel.select_stream(rx.into_stream());
            // last element is guaranteed to be cancelled
            let cancelled = stream
                .filter(|v| matches!(v, Cancellable::Cancelled))
                .next()
                .await;
            assert!(cancelled.is_some());
        })
    }
}
