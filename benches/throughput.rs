use std::hint::black_box;
use std::sync::Barrier;
use std::thread;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use bounded_mpmc_queue::{BoundedQueue, MutexRingQueue};

const OPS_PER_THREAD: usize = 50_000;

fn run_symmetric_pairs(pairs: usize, capacity: usize) {
    let q = MutexRingQueue::<u64>::new(capacity);
    let barrier = Barrier::new(pairs * 2);
    thread::scope(|s| {
        for i in 0..pairs {
            let base = (i * OPS_PER_THREAD) as u64;
            let qb = &q;
            let b1 = &barrier;
            s.spawn(move || {
                b1.wait();
                for k in 0..OPS_PER_THREAD {
                    qb.push(black_box(base + k as u64));
                }
            });
            let qc = &q;
            let b2 = &barrier;
            s.spawn(move || {
                b2.wait();
                for _ in 0..OPS_PER_THREAD {
                    black_box(qc.pop());
                }
            });
        }
    });
}

fn symmetric_pairs(c: &mut Criterion) {
    let mut group = c.benchmark_group("symmetric_pairs");
    for cap in [64usize, 256, 1024] {
        for pairs in [1usize, 2, 4, 8, 16] {
            let total_ops = (OPS_PER_THREAD * pairs * 2) as u64;
            group.throughput(Throughput::Elements(total_ops));
            let id = format!("pairs_{}_cap_{}", pairs, cap);
            group.bench_function(id, |b| {
                b.iter(|| run_symmetric_pairs(pairs, cap));
            });
        }
    }
    group.finish();
}

fn run_asymmetric(producers: usize, consumers: usize, capacity: usize) {
    let total_pushes = OPS_PER_THREAD * producers;
    assert_eq!(total_pushes % consumers, 0);
    let pops_per_consumer = total_pushes / consumers;

    let q = MutexRingQueue::<u64>::new(capacity);
    let barrier = Barrier::new(producers + consumers);
    thread::scope(|s| {
        for i in 0..producers {
            let base = (i * OPS_PER_THREAD) as u64;
            let qb = &q;
            let b = &barrier;
            s.spawn(move || {
                b.wait();
                for k in 0..OPS_PER_THREAD {
                    qb.push(black_box(base + k as u64));
                }
            });
        }

        for _ in 0..consumers {
            let qc = &q;
            let b = &barrier;
            s.spawn(move || {
                b.wait();
                for _ in 0..pops_per_consumer {
                    black_box(qc.pop());
                }
            });
        }
    });
}

fn asymmetric(c: &mut Criterion) {
    let mut group = c.benchmark_group("asymmetric_8p_1c");
    for cap in [64usize, 256, 1024] {
        let producers = 8usize;
        let consumers = 1usize;
        let total_ops = (OPS_PER_THREAD * producers * 2) as u64;
        group.throughput(Throughput::Elements(total_ops));
        let id = format!("cap_{}", cap);
        group.bench_function(id, |b| {
            b.iter(|| run_asymmetric(producers, consumers, cap));
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    // Keep this benchmark suite reasonably fast while still producing stable throughput numbers.
    // These can be overridden via CLI args (Criterion's `--sample-size`, `--measurement-time`, etc).
    config = Criterion::default()
        .sample_size(20)
        .warm_up_time(std::time::Duration::from_secs(1))
        .measurement_time(std::time::Duration::from_secs(2));
    targets = symmetric_pairs, asymmetric
}
criterion_main!(benches);
