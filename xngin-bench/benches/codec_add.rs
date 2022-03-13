use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::iter::FromIterator;
use xngin_compute::BinaryEval;
use xngin_compute::arith::AddI32;
use xngin_storage::codec::{Codec, FlatCodec, OwnFlat};

fn bench_add(c: &mut Criterion) {
    (10..=16).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);
        let c1 = Codec::Flat(FlatCodec::Owned(OwnFlat::from_iter((0..size as i32).into_iter())));
        let c2 = Codec::Flat(FlatCodec::Owned(OwnFlat::from_iter((0..size as i32).into_iter())));
        c.bench_function(&format!("flat_codec_add_{}", size), |b| {
            b.iter(black_box(|| {
                let add = AddI32;
                let _ = add.binary_eval(&c1, &c2).unwrap();
            }))
        });
    });
}

criterion_group!(bench_add_group, bench_add);
criterion_main!(bench_add_group);
