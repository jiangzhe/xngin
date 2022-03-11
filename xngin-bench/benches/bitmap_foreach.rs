use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::{thread_rng, Rng};
use xngin_common::bitmap::{AppendBitmap, ReadBitmap, ReadBitmapExt, VecBitmap};

fn bench_foreach(c: &mut Criterion) {
    const N: usize = 10240;
    const PART_N: usize = 256;
    let mut bm0 = VecBitmap::with_capacity(N);
    let mut bm1 = VecBitmap::with_capacity(N);
    let mut bm2 = VecBitmap::with_capacity(N);
    let mut bm3 = VecBitmap::with_capacity(N); // continuous bitmap
    let mut thd_rng = thread_rng();
    let mut part = (thd_rng.gen::<bool>(), 0);
    for _ in 0..N {
        bm0.add(thd_rng.gen()).unwrap();
        bm1.add(true).unwrap();
        bm2.add(false).unwrap();
        if part.1 == 0 {
            part.0 = !part.0;
            part.1 = thd_rng.gen_range(0..PART_N);
        }
        bm3.add(part.0).unwrap();
        if part.1 > 0 {
            part.1 -= 1;
        }
    }
    c.bench_function("bitmap_foreach_rand", |b| {
        b.iter(|| black_box(consume_foreach(&bm0)))
    });
    c.bench_function("bitmap_foreach_trues", |b| {
        b.iter(|| black_box(consume_foreach(&bm1)))
    });
    c.bench_function("bitmap_foreach_falses", |b| {
        b.iter(|| black_box(consume_foreach(&bm2)))
    });
    c.bench_function("bitmap_foreach_continuous", |b| {
        b.iter(|| black_box(consume_foreach(&bm3)))
    });
}

fn consume_foreach(bm: &VecBitmap) {
    let mut nt = 0;
    let mut nf = 0;
    bm.for_each_range(|flag, m| {
        if flag {
            nt += m;
        } else {
            nf += m;
        }
    });
    assert_eq!(bm.len(), nt + nf);
}

criterion_group!(bench_foreach_group, bench_foreach);
criterion_main!(bench_foreach_group);
