use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::iter::FromIterator;
use xngin_common::bitmap::{VecBitmap, WriteBitmap};

fn bench_shift(c: &mut Criterion) {
    (10..=16).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);
        let mut bm1 = VecBitmap::from_iter((0..size).into_iter().map(|x| x & 3 == 0));
        c.bench_function(&format!("bitmap_shift_{}_aligned_{}", 8, size), |b| {
            b.iter(black_box(|| {
                bm1.shift(8).unwrap();
                bm1.set_len(size).unwrap();
            }))
        });

        c.bench_function(&format!("bitmap_shift_{}_unaligned_{}", 1, size), |b| {
            b.iter(black_box(|| {
                bm1.shift(1).unwrap();
                bm1.set_len(size).unwrap();
            }))
        });

        c.bench_function(&format!("bitmap_shift_{}_unaligned_{}", 255, size), |b| {
            b.iter(black_box(|| {
                bm1.shift(255).unwrap();
                bm1.set_len(size).unwrap();
            }))
        });
    });
}

criterion_group!(bench_shift_group, bench_shift);
criterion_main!(bench_shift_group);
