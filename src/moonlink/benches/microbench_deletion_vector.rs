use moonlink::BatchDeletionVector;

use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_delete_row(c: &mut Criterion) {
    c.bench_function("delete_row", |b| {
        let mut vec = BatchDeletionVector::new(10_000);
        b.iter(|| {
            for i in 0..10_000 {
                vec.delete_row(black_box(i % 10_000));
            }
        })
    });
}

fn bench_is_deleted(c: &mut Criterion) {
    let mut vec = BatchDeletionVector::new(10_000);
    for i in 0..5_000 {
        vec.delete_row(i);
    }

    c.bench_function("is_deleted", |b| {
        b.iter(|| {
            for i in 0..10_000 {
                black_box(vec.is_deleted(i));
            }
        })
    });
}

fn bench_collect_active_rows(c: &mut Criterion) {
    let mut vec = BatchDeletionVector::new(10_000);
    for i in 0..5_000 {
        vec.delete_row(i);
    }

    c.bench_function("collect_active_rows", |b| {
        b.iter(|| {
            black_box(vec.collect_active_rows(10_000));
        })
    });
}

criterion_group!(
    benches,
    bench_delete_row,
    bench_is_deleted,
    bench_collect_active_rows,
);
criterion_main!(benches);
