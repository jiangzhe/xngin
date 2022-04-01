pub mod proj;

use crate::buf::OutputBuffer;
use crate::cancel::Cancellation;
use crate::error::{Error, Result};
use async_executor::Executor;
use async_trait::async_trait;
use flume::{Receiver, Sender};
use proj::ProjExec;
use std::sync::Arc;
use xngin_storage::block::Block;

pub enum Exec {
    Proj(ProjExec),
    // TableScan(TableScanExec),
}

pub struct ExecContext {
    pub(crate) executor: Arc<Executor<'static>>,
    pub(crate) cancel: Cancellation,
}

impl ExecContext {
    /// Proxy result to output buffer of downstream.
    #[inline]
    pub(crate) fn proxy_res(&self, res_rx: Receiver<Result<Block>>, out_buf: &OutputBuffer) {
        let executor = self.executor.clone();
        let cancel = self.cancel.clone();
        let out_buf = out_buf.clone();
        executor
            .spawn(async move {
                while let Ok(res) = res_rx.recv_async().await {
                    match res {
                        Ok(res) => {
                            if out_buf.send(res).await.is_err() {
                                return;
                            }
                        }
                        Err(e) => {
                            cancel.cancel(e);
                            return;
                        }
                    }
                }
            })
            .detach();
    }

    /// Create workers by given parallelism, and returns task dispatcher and result channel.
    #[inline]
    pub(crate) fn dispatch<W: Work>(
        &self,
        parallel: usize,
    ) -> (Dispatcher<W>, Receiver<W::Output>) {
        let (work_tx, work_rx) = flume::bounded::<W>(parallel);
        let (res_tx, res_rx) = flume::bounded(parallel);
        for _ in 0..parallel {
            let work_rx = work_rx.clone();
            let res_tx = res_tx.clone();
            self.executor
                .spawn(async move {
                    while let Ok(work) = work_rx.recv_async().await {
                        let res = work.run().await;
                        if res_tx.send_async(res).await.is_err() {
                            return;
                        }
                    }
                })
                .detach();
        }
        (Dispatcher { tx: work_tx }, res_rx)
    }
}

#[async_trait]
pub trait Executable {
    async fn exec(&mut self, ctx: &ExecContext) -> Result<()>;
}

pub struct Dispatcher<W> {
    tx: Sender<W>,
}

impl<W: Work> Dispatcher<W> {
    /// Dispatch the work to underlying executor in background.
    /// Client can fetch result from the background task queue.
    #[inline]
    pub async fn dispatch(&self, work: W) -> Result<()> {
        self.tx
            .send_async(work)
            .await
            .map_err(|_| Error::DispatchError)
    }
}

/// Single unit of computation.
#[async_trait]
pub trait Work: Sized + Send + Sync + 'static {
    type Output: Send + Sync;

    /// run the work to get output
    async fn run(self) -> Self::Output;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buf;
    use crate::cancel::Cancellable;
    use crate::tests::single_thread_executor;
    use futures_lite::future;
    use futures_lite::StreamExt;
    use xngin_compute::eval::QueryEvalPlan;
    use xngin_datatype::PreciseType;
    use xngin_expr::infer::fix_rec;
    use xngin_expr::{Const, Expr, FuncKind, QueryID};
    use xngin_storage::block::Attr;
    use xngin_storage::codec::{Codec, FlatCodec};

    #[test]
    fn test_proj_exec() {
        let size = 1024i32;
        let codec1 = Codec::Flat(FlatCodec::from((0..size).into_iter().map(|i| i as i64)));
        let attr1 = Attr {
            ty: PreciseType::i64(),
            codec: codec1,
            psma: None,
        };
        let attr2 = attr1.to_owned();
        let block = Block {
            len: size as usize,
            attrs: vec![attr1, attr2],
        };
        let col1 = Expr::query_col(QueryID::from(0), 0);
        // select c1 + 1
        let es = vec![Expr::func(
            FuncKind::Add,
            vec![col1.clone(), Expr::new_const(Const::I64(1))],
        )];
        let eval_plan = build_plan(es);
        let (upstream_in, upstream_out) = buf::unbounded(1024);
        let (downstream_in, downstream_out) = buf::unbounded(1024);
        let mut proj_exec = ProjExec::new(Arc::new(eval_plan), upstream_in, downstream_out, 1);
        // setup execution context
        let executor = single_thread_executor();
        let cancel = Cancellation::new();
        let ctx = ExecContext { executor, cancel };
        future::block_on(async move {
            upstream_out.send(block).await.unwrap();
            // end upstream
            drop(upstream_out);
            proj_exec.exec(&ctx).await.unwrap();
            let mut stream = downstream_in.to_stream(&ctx.cancel).unwrap();
            let res = stream.next().await.unwrap();
            if let Cancellable::Ready(res) = res {
                assert_eq!(1024, res.len);
                let res = res.attrs;
                assert_eq!(1, res.len());
                assert_eq!(PreciseType::i64(), res[0].ty);
                let f = res[0].codec.as_flat().unwrap();
                let (bm, data) = f.view::<i64>();
                assert!(bm.is_none());
                let expected: Vec<_> = (0..1024).into_iter().map(|i| (i + 1) as i64).collect();
                assert_eq!(&expected, data);
            } else {
                panic!("failed")
            }
        })
    }

    #[inline]
    fn build_plan(mut exprs: Vec<Expr>) -> QueryEvalPlan {
        for e in &mut exprs {
            fix_rec(e, |_, _| Some(PreciseType::i64())).unwrap();
        }
        QueryEvalPlan::new(&exprs).unwrap()
    }
}
