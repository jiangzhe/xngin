use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::iter::FromIterator;
use xngin_common::bitmap::{AppendBitmap, VecBitmap, WriteBitmap};

fn bench_extend(c: &mut Criterion) {
    (12..=16).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);
        let bm2 = VecBitmap::from_iter((0..size).into_iter().map(|x| x & 3 == 0));
        c.bench_function(&format!("bitmap_extend_aligned_{}", size), |b| {
            let mut bm1 = VecBitmap::new();
            b.iter(black_box(|| {
                bm1.extend(&bm2).unwrap();
                bm1.clear();
            }))
        });

        c.bench_function(&format!("bitmap_extend_unaligned_{}", size), |b| {
            let mut bm1 = VecBitmap::new();
            b.iter(black_box(|| {
                bm1.add(true).unwrap();
                bm1.extend(&bm2).unwrap();
                bm1.clear();
            }))
        });

        c.bench_function(&format!("bitmap_extend_const_aligned_{}", size), |b| {
            let mut bm1 = VecBitmap::new();
            b.iter(black_box(|| {
                bm1.extend_const(true, size).unwrap();
                bm1.clear();
            }))
        });

        c.bench_function(&format!("bitmap_extend_const_unaligned_{}", size), |b| {
            let mut bm1 = VecBitmap::new();
            b.iter(black_box(|| {
                bm1.add(true).unwrap();
                bm1.extend_const(true, size).unwrap();
                bm1.clear();
            }))
        });

        c.bench_function(&format!("bitmap_extend_range_aligned_{}", size), |b| {
            let mut bm1 = VecBitmap::new();
            b.iter(black_box(|| {
                bm1.extend_range(&bm2, 256..size).unwrap();
                bm1.clear();
            }))
        });

        c.bench_function(&format!("bitmap_extend_range_unaligned_{}", size), |b| {
            let mut bm1 = VecBitmap::new();
            b.iter(black_box(|| {
                bm1.extend_range(&bm2, 255..size).unwrap();
                bm1.clear();
            }))
        });
    });
}

criterion_group!(bench_extend_group, bench_extend);
criterion_main!(bench_extend_group);
