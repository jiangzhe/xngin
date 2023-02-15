use crate::error::Result;
use crate::mysql::conn::MyConn;
use crate::mysql::serde::{SerdeCtx, SerdeMode};
use crate::mysql::server::session::Session;
use crate::mysql::server::Server;
use async_executor::Executor;
use async_net::TcpListener;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub type Task<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

pub trait TaskBuilder {
    /// schedule task with given executor.
    /// The task should be run asynchronously and should not
    /// block current thread.
    fn build<'a>(&mut self, server: &'a Server, ex: Arc<Executor<'a>>) -> Result<Task<'a>>;

    /// Returns task name.
    fn name(&self) -> &str;
}

const DEFAULT_READ_BUF_SIZE: usize = 16 * 1024;
const DEFAULT_WRITE_BUF_SIZE: usize = 16 * 1024;

#[derive(Debug, Clone)]
pub struct ServeTCP {
    addr: String,
    read_buf_size: usize,
    write_buf_size: usize,
}

impl ServeTCP {
    #[inline]
    pub fn new<T: Into<String>>(addr: T) -> Self {
        ServeTCP {
            addr: addr.into(),
            read_buf_size: DEFAULT_READ_BUF_SIZE,
            write_buf_size: DEFAULT_WRITE_BUF_SIZE,
        }
    }

    #[inline]
    pub fn read_buf_size(mut self, read_buf_size: usize) -> Self {
        self.read_buf_size = read_buf_size;
        self
    }

    #[inline]
    pub fn write_buf_size(mut self, write_buf_size: usize) -> Self {
        self.write_buf_size = write_buf_size;
        self
    }

    #[inline]
    pub async fn listen_and_serve<'a>(
        self,
        server: &'a Server,
        ex: Arc<Executor<'a>>,
    ) -> Result<()> {
        log::debug!("start listen to {}", self.addr);
        let listener = TcpListener::bind(&self.addr).await?;
        loop {
            let (conn, peer_addr) = listener.accept().await?;
            let conn_id = server.conn_id_gen.fetch_add(1, Ordering::SeqCst);
            log::debug!(
                "Accept connection from {}, assign connection id {}",
                peer_addr,
                conn_id
            );
            let conn = MyConn::new(
                conn_id,
                conn,
                SerdeCtx::default().with_mode(SerdeMode::Server),
            );
            let session = Session::new(server, conn);
            ex.spawn(session.start()).detach();
        }
    }
}

impl TaskBuilder for ServeTCP {
    #[inline]
    fn name(&self) -> &str {
        "TCP Server"
    }

    #[inline]
    fn build<'a>(&mut self, server: &'a Server, ex: Arc<Executor<'a>>) -> Result<Task<'a>> {
        let s = self.clone();
        let res = Box::pin(s.listen_and_serve(server, ex));
        Ok(res)
    }
}
