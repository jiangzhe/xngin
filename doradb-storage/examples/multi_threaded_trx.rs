//! Multi-threaded transaction processing.
//! This example runs empty transactions via multiple threads. 
//! Its goal is to testing system bottleneck on starting and committing transactions.
use doradb_storage::prelude::*;
use doradb_storage::trx::redo::{RedoBin, RedoLogger};
use clap::Parser;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use std::io::BufWriter;
use std::fs::File;
use std::io::Write;

fn main() {
    let args = Args::parse();
    let stop = Arc::new(AtomicBool::new(false));
    let trx_sys = TransactionSystem::new_static();
    if args.gc_enabled {
        trx_sys.start_gc_thread();
    }
    if let Some(log_file) = &args.log_file {
        let logger = CtsOnlyRedoLogger::new(log_file, args.buf_size);
        trx_sys.set_redo_logger(Box::new(logger));
    }
    {
        let mut total_trx_count = 0;
        let mut handles = vec![];
        let start = Instant::now();
        for _ in 1..args.threads {
            let stop = Arc::clone(&stop);
            let handle =
                std::thread::spawn(move || worker(trx_sys, args.count, stop));
            handles.push(handle);
        }
        total_trx_count += worker(trx_sys, args.count, stop);
        for handle in handles {
            total_trx_count += handle.join().unwrap();
        }
        let dur = start.elapsed();
        println!(
            "{:?} transactions cost {:?} microseconds, avg {:?} trx/s",
            total_trx_count,
            dur.as_micros(),
            total_trx_count as f64 * 1_000_000_000f64 / dur.as_nanos() as f64
        );
    }
    unsafe { TransactionSystem::drop_static(trx_sys); }
}

#[inline]
fn worker(trx_sys: &TransactionSystem, max_count: usize, stop: Arc<AtomicBool>) -> usize {
    let mut count = 0;
    let stop = &*stop;
    while !stop.load(Ordering::Relaxed) {
        let trx = trx_sys.new_trx();
        trx_sys.commit(trx);
        count += 1;
        if count == max_count {
            stop.store(true, Ordering::SeqCst); // notify others to stop.
            break;
        }
    }
    count
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// thread number to run transactions
    #[arg(short, long, default_value = "4")]
    threads: usize,

    /// Number of transactions at least one thread should complete
    #[arg(short, long, default_value = "100000")]
    count: usize,

    /// path of redo log file
    #[arg(short, long, default_value = "redo.log")]
    log_file: Option<String>,

    /// size of redo log buffer
    #[arg(short, long, default_value = "4096")]
    buf_size: usize,

    /// whether to enable GC
    #[arg(short, long)]
    gc_enabled: bool,
}

struct CtsOnlyRedoLogger {
    writer: BufWriter<File>,
}

impl CtsOnlyRedoLogger {
    #[inline]
    fn new(log_file: &str, buf_size: usize) -> Self {
        let f = File::create(log_file).expect("fail to create log file");
        let writer = BufWriter::with_capacity(buf_size, f);
        CtsOnlyRedoLogger {writer}
    }
}

impl RedoLogger for CtsOnlyRedoLogger {
    #[inline]
    fn write(&mut self, cts: TrxID, _redo_bin: RedoBin) {
        write!(self.writer, "{}\n", cts).unwrap();
    }

    #[inline]
    fn sync(&mut self) {
        self.writer.flush().unwrap();
    }
}