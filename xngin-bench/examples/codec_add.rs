use criterion::{criterion_group, criterion_main, Criterion};
use pprof::criterion::{PProfProfiler, Output};
use xngin_compute::binary::{BinaryEval, AddI32};
use xngin_storage::codec::{Codec, FlatCodec, OwnFlat};
use std::sync::Arc;

fn bench_add(c: &mut Criterion) {
    let size = 4096;
    let c1 = Codec::Flat(FlatCodec::Owned(Arc::new(OwnFlat::from((0..size as i32).into_iter()))));
    let c2 = Codec::Flat(FlatCodec::Owned(Arc::new(OwnFlat::from((0..size as i32).into_iter()))));
    c.bench_function(&format!("flat_codec_add_{}", size), |b| {
        b.iter(|| {
            let add = AddI32;
            let _ = add.binary_eval(&c1, &c2).unwrap();
        })
    });
}

criterion_group!(
    name = bench_add_group;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_add);
criterion_main!(bench_add_group);
