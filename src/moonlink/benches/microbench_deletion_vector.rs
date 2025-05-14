use criterion::{black_box, criterion_group, criterion_main, Criterion};
use moonlink::{BatchDeletionVector, RoaringBitmapDV};
use rand::seq::IteratorRandom;
use rand::thread_rng;

fn generate_random_indices(count: usize, max: usize) -> Vec<usize> {
    (0..max).choose_multiple(&mut thread_rng(), count)
}

// ---------- bitmap ----------
fn bench_delete_row(c: &mut Criterion) {
    let indices = generate_random_indices(1000, 2048 * 16);

    c.bench_function("bitmap_delete_row", |b| {
        b.iter(|| {
            let mut vec = BatchDeletionVector::new(2048 * 16);
            for &i in &indices {
                vec.delete_row(black_box(i));
            }
        })
    });
}

fn bench_is_deleted(c: &mut Criterion) {
    let indices = generate_random_indices(1000, 2048 * 16);
    let mut vec = BatchDeletionVector::new(2048 * 16);
    for &i in &indices {
        vec.delete_row(i);
    }

    c.bench_function("bitmap_is_deleted", |b| {
        b.iter(|| {
            for i in 0..2048 * 16 {
                black_box(vec.is_deleted(i));
            }
        })
    });
}

fn bench_collect_active_rows(c: &mut Criterion) {
    let indices = generate_random_indices(1000, 2048 * 16);
    let mut vec = BatchDeletionVector::new(2048 * 16);
    for &i in &indices {
        vec.delete_row(i);
    }

    c.bench_function("bitmap_collect_active_rows", |b| {
        b.iter(|| {
            black_box(vec.collect_active_rows(2048 * 16));
        })
    });
}

fn bench_collect_deleted_rows(c: &mut Criterion) {
    let indices = generate_random_indices(1000, 2048 * 16);
    let mut vec = BatchDeletionVector::new(2048 * 16);
    for &i in &indices {
        vec.delete_row(i);
    }

    c.bench_function("bitmap_collect_deletedrows", |b| {
        b.iter(|| {
            black_box(vec.collect_deleted_rows());
        })
    });
}

// ---------- roaring bitmap ----------
fn bench_roaring_delete_row(c: &mut Criterion) {
    let indices = generate_random_indices(1000, 2048 * 16);

    c.bench_function("roaring_delete_row", |b| {
        b.iter(|| {
            let mut vec = RoaringBitmapDV::new();
            for &i in &indices {
                vec.delete_row(black_box(i));
            }
        })
    });
}

fn bench_roaring_is_deleted(c: &mut Criterion) {
    let indices = generate_random_indices(1000, 2048 * 16);
    let mut vec = RoaringBitmapDV::new();
    for &i in &indices {
        vec.delete_row(i);
    }

    c.bench_function("roaring_is_deleted", |b| {
        b.iter(|| {
            for i in 0..2048 * 16 {
                black_box(vec.is_deleted(i));
            }
        })
    });
}

fn bench_roaring_collect_active_rows(c: &mut Criterion) {
    let indices = generate_random_indices(1000, 2048 * 16);
    let mut vec = RoaringBitmapDV::new();
    for &i in &indices {
        vec.delete_row(i);
    }

    c.bench_function("roaring_collect_active_rows", |b| {
        b.iter(|| {
            black_box(vec.collect_active_rows(2048 * 16));
        })
    });
}

// fn bench_roaring_collect_deleted_rows(c: &mut Criterion) {
//     let indices = generate_random_indices(1000, 2048 * 16);
//     let mut vec = RoaringBitmapDV::new();
//     for &i in &indices {
//         vec.delete_row(i);
//     }

//     c.bench_function("roaring_collect_deleted_rows", |b| {
//         b.iter(|| {
//             black_box(vec.collect_deleted_rows());
//         })
//     });
// }

criterion_group!(
    benches,
    bench_delete_row,
    bench_is_deleted,
    bench_collect_active_rows,
    bench_collect_deleted_rows,
    bench_roaring_delete_row,
    bench_roaring_is_deleted,
    bench_roaring_collect_active_rows,
);
criterion_main!(benches);
