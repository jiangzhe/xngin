use crate::cancel::{Cancellable, Cancellation};
use crate::error::{Error, Result};
use flume::{Receiver, Sender};
use futures_lite::Stream;
use xngin_storage::block::Block;

/// Buffer of input data blocks backed by [`flume::Receiver`].
#[derive(Clone)]
pub struct InputBuffer {
    rx: Receiver<Block>,
}

impl InputBuffer {
    #[inline]
    pub fn to_stream(
        &self,
        cancel: &Cancellation,
    ) -> Result<impl Stream<Item = Cancellable<Block>>> {
        let rx = self.rx.clone();
        Ok(cancel.select_stream(rx.into_stream()))
    }
}

/// Buffer of output data blocks backed by [`flume::Sender`].
#[derive(Clone)]
pub struct OutputBuffer {
    pub expected_len: usize,
    tx: Sender<Block>,
}

impl OutputBuffer {
    #[inline]
    pub async fn send(&self, block: Block) -> Result<()> {
        self.tx
            .send_async(block)
            .await
            .map_err(|_| Error::BufferClosed)
    }
}

#[inline]
pub fn bounded(buf_size: usize, block_len: usize) -> (InputBuffer, OutputBuffer) {
    let (tx, rx) = flume::bounded(buf_size);
    (
        InputBuffer { rx },
        OutputBuffer {
            expected_len: block_len,
            tx,
        },
    )
}

#[inline]
pub fn unbounded(block_len: usize) -> (InputBuffer, OutputBuffer) {
    let (tx, rx) = flume::unbounded();
    (
        InputBuffer { rx },
        OutputBuffer {
            expected_len: block_len,
            tx,
        },
    )
}
