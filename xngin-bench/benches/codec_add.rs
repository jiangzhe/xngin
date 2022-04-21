use criterion::{criterion_group, criterion_main, Criterion};
use xngin_compute::binary::{BinaryEval, AddI32, AddI64};
use xngin_storage::attr::Attr;
use xngin_datatype::PreciseType;

fn bench_codec(c: &mut Criterion) {
    for log2_size in [12, 14] {
        let size = 2usize.pow(log2_size);
        let c1 = Attr::from((0..size as i32).into_iter());
        let c2 = Attr::from((0..size as i32).into_iter());
        c.bench_function(&format!("codec_add_array_i32_{}", size), |b| {
            b.iter(|| {
                let add = AddI32;
                let _ = add.binary_eval(PreciseType::i32(), &c1, &c2).unwrap();
            })
        });

        let c1 = Attr::from((0..size as i32).map(|i| i as i64));
        let c2 = Attr::from((0..size as i32).map(|i| i as i64));
        c.bench_function(&format!("codec_add_array_i64_{}", size), |b| {
            b.iter(|| {
                let add = AddI64;
                let _ = add.binary_eval(PreciseType::i64(), &c1, &c2).unwrap();
            })
        });
    }
}

criterion_group!(bench_codec_group, bench_codec);
criterion_main!(bench_codec_group);
