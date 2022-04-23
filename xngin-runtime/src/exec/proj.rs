use crate::buf::{InputBuffer, OutputBuffer};
use crate::cancel::Cancellable;
use crate::error::{Error, Result};
use crate::exec::{ExecContext, Executable, Work};
use async_trait::async_trait;
use futures_lite::StreamExt;
use std::sync::Arc;
use xngin_compute::eval::QueryEvalPlan;
use xngin_storage::block::Block;

pub struct ProjExec {
    eval_plan: Arc<QueryEvalPlan>,
    in_buf: InputBuffer,
    out_buf: Option<OutputBuffer>,
    parallel: usize,
}

impl ProjExec {
    #[inline]
    pub fn new(
        eval_plan: Arc<QueryEvalPlan>,
        in_buf: InputBuffer,
        out_buf: OutputBuffer,
        parallel: usize,
    ) -> Self {
        ProjExec {
            eval_plan,
            in_buf,
            out_buf: Some(out_buf),
            parallel,
        }
    }

    #[inline]
    fn new_work(&self, block: Block) -> ProjWork {
        ProjWork {
            eval_plan: self.eval_plan.clone(),
            block,
        }
    }
}

#[async_trait]
impl Executable for ProjExec {
    #[inline]
    async fn exec(&mut self, ctx: &ExecContext) -> Result<()> {
        let out_buf = self.out_buf.take().ok_or(Error::RerunNotAllowed)?;
        let (dispatcher, res_rx) = ctx.dispatch::<ProjWork>(self.parallel);
        // proxy work to downstream
        ctx.proxy_res(res_rx, &out_buf);
        // dispatch work to workers
        let mut in_stream = self.in_buf.to_stream(&ctx.cancel)?;
        while let Some(Cancellable::Ready(input)) = in_stream.next().await {
            let work = self.new_work(input);
            dispatcher.dispatch(work).await?;
        }
        Ok(())
    }
}

struct ProjWork {
    eval_plan: Arc<QueryEvalPlan>,
    block: Block,
}

#[async_trait]
impl Work for ProjWork {
    type Output = Result<Block>;

    #[inline]
    async fn run(self) -> Result<Block> {
        let n_records = self.block.n_records;
        self.eval_plan
            .eval(&self.block)
            .map(|data| Block::new(n_records, data))
            .map_err(Into::into)
    }
}
