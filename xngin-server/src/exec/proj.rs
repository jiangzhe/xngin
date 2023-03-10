use crate::chan::{InputChannel, OutputChannel};
use crate::exec::{ExecContext, Executable, Work};
use async_trait::async_trait;
use futures_lite::StreamExt;
use std::sync::Arc;
use xngin_compute::eval::QueryEvalPlan;
use xngin_protocol::mysql::error::{Error, Result};
use xngin_storage::block::Block;

pub struct ProjExec {
    eval_plan: Arc<QueryEvalPlan>,
    input: InputChannel,
    out: Option<OutputChannel>,
    parallel: usize,
}

impl ProjExec {
    #[inline]
    pub fn new(
        eval_plan: Arc<QueryEvalPlan>,
        input: InputChannel,
        out: OutputChannel,
        parallel: usize,
    ) -> Self {
        ProjExec {
            eval_plan,
            input,
            out: Some(out),
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
        let out = self.out.take().ok_or(Error::InvalidExecutorState())?;
        let (dispatcher, res_rx) = ctx.dispatch::<ProjWork>(self.parallel);
        // proxy work to downstream
        ctx.proxy_res(res_rx, out);
        // dispatch work to workers
        let mut in_stream = self.input.to_stream(&ctx.cancel)?;
        loop {
            match in_stream.next().await {
                Some(Ok(input)) => {
                    let work = self.new_work(input);
                    dispatcher.dispatch(work).await?;
                }
                Some(Err(e)) => return Err(e),
                None => return Ok(()),
            }
        }
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
            .map_err(|e| Error::RuntimeError(Box::new(e.to_string())))
    }
}
