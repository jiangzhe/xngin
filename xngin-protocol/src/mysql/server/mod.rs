pub mod principal;
pub mod session;
pub mod signal;
pub mod task;
// pub mod resultset;

use crate::error::{Error, Result};
use async_executor::Executor;
use async_io::block_on;
use easy_parallel::Parallel;
use flume::{Receiver, Sender};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use task::TaskBuilder;

const DEFAULT_SERVER_THREADS: usize = 1;

pub struct ServerSpec {
    pub version: String,
    pub protocol_version: u8,
}

impl Default for ServerSpec {
    #[inline]
    fn default() -> Self {
        ServerSpec {
            version: String::from("mysql-8.0.30-xngin"),
            protocol_version: 10,
        }
    }
}

pub struct Server {
    spec: ServerSpec,
    worker_threads: usize,
    // auto-incremental connection id, starts from 1.
    conn_id_gen: AtomicU32,
    stop_signal: Option<Receiver<()>>,
}

impl Server {
    #[inline]
    pub fn start<F>(self, builders: Vec<Box<dyn TaskBuilder>>, process_stop_signal: F) -> Result<()>
    where
        F: FnOnce(Sender<()>) -> Result<()>,
    {
        let (stop_signal, stop_notify) = flume::unbounded();
        // dedicate one thread to handle signal and stop server gracefully.
        process_stop_signal(stop_signal)?;
        // setup worker threads
        let ex = Arc::new(Executor::new());
        let worker_threads = self.worker_threads;
        let (_, res) = Parallel::new()
            .each(0..worker_threads, |i| {
                log::debug!("start worker thread {}", i);
                block_on(ex.run(stop_notify.recv_async()))
            })
            .finish(|| {
                for mut builder in builders {
                    log::debug!("schedule task {}", builder.name());
                    let task = builder.build(&self, Arc::clone(&ex))?;
                    ex.spawn(task).detach();
                }
                assert!(block_on(ex.run(stop_notify.recv_async())).is_err());
                Ok(())
            });
        res
    }

    /// Signal to notify if server is stopped.
    #[inline]
    pub fn stop_signal(&self) -> Result<Receiver<()>> {
        self.stop_signal
            .as_ref()
            .cloned()
            .ok_or(Error::ServerNotStarted)
    }
}

impl Default for Server {
    #[inline]
    fn default() -> Self {
        Server {
            spec: ServerSpec::default(),
            worker_threads: DEFAULT_SERVER_THREADS,
            conn_id_gen: AtomicU32::new(1),
            stop_signal: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buf::ByteBuffer;
    use crate::mysql::conn::TcpClientOpts;
    use crate::mysql::server::task::ServeTCP;
    use async_io::{block_on, Timer};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_server_start() -> Result<()> {
        let (tx, rx) = flume::unbounded::<()>();
        let handle = thread::spawn(move || {
            let server = Server::default();
            let _ = server.start(vec![Box::new(ServeTCP::new("127.0.0.1:23306"))], |signal| {
                thread::spawn(move || {
                    let _ = rx.recv();
                    drop(signal);
                });
                Ok(())
            });
        });
        thread::sleep(Duration::from_millis(100));

        block_on(async {
            let read_buf = ByteBuffer::with_capacity(4096);
            let mut write_buf = vec![0u8; 4096];
            let mut tries = 0;
            loop {
                let _ = Timer::after(Duration::from_millis(100)).await;
                if let Ok(_) = TcpClientOpts::new("127.0.0.1:23306")
                    .username("root")
                    .password("password")
                    .connect(&read_buf, &mut write_buf, None)
                    .await
                {
                    break;
                }
                tries += 1;
                if tries >= 5 {
                    return Err(Error::IOError(std::io::ErrorKind::ConnectionRefused));
                }
            }
            Ok(())
        })?;
        drop(tx);
        handle.join().unwrap();
        Ok(())
    }
}
