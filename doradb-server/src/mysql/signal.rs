use flume::Sender;
use signal_hook::consts::*;
use signal_hook::iterator::Signals;
use std::thread;
use doradb_protocol::mysql::error::Result;

#[inline]
pub fn subscribe_stop_signal(tx: Sender<()>) -> Result<()> {
    let mut signals = Signals::new([SIGTERM, SIGINT, SIGQUIT])?;
    thread::spawn(move || {
        let handle = signals.handle();
        let _ = signals.forever().next();
        handle.close();
        drop(tx)
    });
    Ok(())
}
