pub mod buf;
pub mod cancel;
pub mod digraph;
pub mod error;
pub mod exec;
pub mod phy;
pub mod res;

#[cfg(test)]
pub(crate) mod tests {
    use async_executor::Executor;
    use futures_lite::future;
    use std::panic::catch_unwind;
    use std::sync::Arc;
    use std::thread;

    pub(crate) fn single_thread_executor() -> Arc<Executor<'static>> {
        let ex = Arc::new(Executor::new());
        {
            let ex = ex.clone();
            thread::Builder::new()
                .name(format!("async-executor"))
                .spawn(move || loop {
                    catch_unwind(|| async_io::block_on(ex.run(future::pending::<()>()))).ok();
                })
                .expect("cannot spawn executor thread");
        }
        ex
    }
}
