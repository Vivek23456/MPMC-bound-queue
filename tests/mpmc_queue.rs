use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;

use bounded_mpmc_queue::{BoundedQueue, MutexRingQueue};

#[test]
fn single_thread_try_semantics() {
    let q = MutexRingQueue::<u32>::new(2);

    assert_eq!(q.try_pop(), None);
    assert_eq!(q.try_push(1), Ok(()));
    assert_eq!(q.try_push(2), Ok(()));
    assert_eq!(q.try_push(3), Err(3)); // full

    assert_eq!(q.try_pop(), Some(1));
    assert_eq!(q.try_pop(), Some(2));
    assert_eq!(q.try_pop(), None); // empty
}

#[test]
fn fifo_spot_check_single_producer_single_consumer() {
    let q = Arc::new(MutexRingQueue::<usize>::new(4));
    let n = 10_000usize;

    let qp = q.clone();
    let prod = thread::spawn(move || {
        for i in 0..n {
            qp.push(i);
        }
    });

    let qc = q.clone();
    let cons = thread::spawn(move || {
        for i in 0..n {
            let v = qc.pop();
            assert_eq!(v, i);
        }
    });

    prod.join().unwrap();
    cons.join().unwrap();
}

#[test]
fn contention_balance_many_producers_many_consumers() {
    const PRODUCERS: usize = 8;
    const CONSUMERS: usize = 8;
    const PER_PRODUCER: usize = 25_000;
    let total = PRODUCERS * PER_PRODUCER;

    let q = Arc::new(MutexRingQueue::<usize>::new(64));
    let start = Arc::new(Barrier::new(PRODUCERS + CONSUMERS));
    let remaining = Arc::new(AtomicUsize::new(total));

    let mut handles = Vec::new();

    for p in 0..PRODUCERS {
        let qp = q.clone();
        let b = start.clone();
        handles.push(thread::spawn(move || {
            b.wait();
            let base = p * PER_PRODUCER;
            for i in 0..PER_PRODUCER {
                qp.push(base + i);
            }
        }));
    }

    let seen = Arc::new(std::sync::Mutex::new(HashSet::with_capacity(total)));
    for _ in 0..CONSUMERS {
        let qc = q.clone();
        let b = start.clone();
        let rem = remaining.clone();
        let seen = seen.clone();
        handles.push(thread::spawn(move || {
            b.wait();
            loop {
                // Acquire one "permit" to pop, so we pop exactly `total` items overall.
                let prev = rem.fetch_update(Ordering::AcqRel, Ordering::Acquire, |x| {
                    if x == 0 { None } else { Some(x - 1) }
                });
                if prev.is_err() {
                    break;
                }

                let v = qc.pop();
                let mut set = seen.lock().unwrap();
                assert!(set.insert(v), "duplicate value popped: {v}");
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(remaining.load(Ordering::Acquire), 0);
    let set = seen.lock().unwrap();
    assert_eq!(set.len(), total);
}

// ---------------------------------------------------------------------------
// Capacity-1 edge case
// ---------------------------------------------------------------------------

#[test]
fn capacity_one_single_thread() {
    let q = MutexRingQueue::<i32>::new(1);
    assert_eq!(q.try_push(42), Ok(()));
    assert_eq!(q.try_push(99), Err(99));
    assert_eq!(q.try_pop(), Some(42));
    assert_eq!(q.try_pop(), None);
}

#[test]
fn capacity_one_two_threads() {
    let q = Arc::new(MutexRingQueue::<usize>::new(1));
    let n = 5_000usize;

    let qp = q.clone();
    let prod = thread::spawn(move || {
        for i in 0..n {
            qp.push(i);
        }
    });

    let qc = q.clone();
    let cons = thread::spawn(move || {
        let mut received = Vec::with_capacity(n);
        for _ in 0..n {
            received.push(qc.pop());
        }
        received
    });

    prod.join().unwrap();
    let received = cons.join().unwrap();
    assert_eq!(received.len(), n);
    for (i, &v) in received.iter().enumerate() {
        assert_eq!(v, i, "FIFO violated at index {i}");
    }
}

// ---------------------------------------------------------------------------
// Fill then drain (single thread, ring wrapping)
// ---------------------------------------------------------------------------

#[test]
fn fill_drain_single_thread() {
    let cap = 16;
    let q = MutexRingQueue::<usize>::new(cap);

    for i in 0..cap {
        assert_eq!(q.try_push(i), Ok(()));
    }
    assert_eq!(q.try_push(999), Err(999));

    for i in 0..cap {
        assert_eq!(q.try_pop(), Some(i));
    }
    assert_eq!(q.try_pop(), None);
}

// ---------------------------------------------------------------------------
// Multiple fill/drain cycles (proves head/tail wrap correctly)
// ---------------------------------------------------------------------------

#[test]
fn multiple_fill_drain_cycles() {
    let cap = 8;
    let q = MutexRingQueue::<usize>::new(cap);

    for cycle in 0..10 {
        let base = cycle * cap;
        for i in 0..cap {
            q.push(base + i);
        }
        for i in 0..cap {
            assert_eq!(q.pop(), base + i);
        }
    }
    assert_eq!(q.try_pop(), None);
}

// ---------------------------------------------------------------------------
// Blocking: push blocks when full, unblocks on pop
// ---------------------------------------------------------------------------

#[test]
fn push_blocks_when_full() {
    let q = Arc::new(MutexRingQueue::<u32>::new(2));
    q.push(1);
    q.push(2);

    let blocked = Arc::new(AtomicBool::new(true));
    let blocked2 = blocked.clone();
    let qp = q.clone();
    let producer = thread::spawn(move || {
        qp.push(3); // should block until consumer pops
        blocked2.store(false, Ordering::Release);
    });

    thread::sleep(Duration::from_millis(50));
    assert!(blocked.load(Ordering::Acquire), "push should still be blocked");

    assert_eq!(q.pop(), 1); // free a slot

    producer.join().unwrap();
    assert!(!blocked.load(Ordering::Acquire), "push should have completed");
}

// ---------------------------------------------------------------------------
// Blocking: pop blocks when empty, unblocks on push
// ---------------------------------------------------------------------------

#[test]
fn pop_blocks_when_empty() {
    let q = Arc::new(MutexRingQueue::<u32>::new(4));

    let blocked = Arc::new(AtomicBool::new(true));
    let blocked2 = blocked.clone();
    let qc = q.clone();
    let consumer = thread::spawn(move || {
        let v = qc.pop(); // should block until producer pushes
        blocked2.store(false, Ordering::Release);
        v
    });

    thread::sleep(Duration::from_millis(50));
    assert!(blocked.load(Ordering::Acquire), "pop should still be blocked");

    q.push(42);

    let v = consumer.join().unwrap();
    assert_eq!(v, 42);
    assert!(!blocked.load(Ordering::Acquire), "pop should have completed");
}

// ---------------------------------------------------------------------------
// Drop correctness: every pushed item is dropped exactly once
// ---------------------------------------------------------------------------

fn make_tracked(counter: &Arc<AtomicUsize>, val: u32) -> TrackedItem {
    TrackedItem {
        _val: val,
        counter: counter.clone(),
    }
}

struct TrackedItem {
    #[allow(dead_code)]
    _val: u32,
    counter: Arc<AtomicUsize>,
}

impl Drop for TrackedItem {
    fn drop(&mut self) {
        self.counter.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn drop_correctness_items_dropped_on_queue_drop() {
    let counter = Arc::new(AtomicUsize::new(0));
    {
        let q = MutexRingQueue::<TrackedItem>::new(8);
        for i in 0..5u32 {
            q.push(make_tracked(&counter, i));
        }
        // pop 2, leaving 3 inside when queue drops
        let _ = q.pop();
        let _ = q.pop();
    }
    // 2 popped (dropped after pop) + 3 dropped by queue's Drop = 5
    assert_eq!(counter.load(Ordering::SeqCst), 5);
}

#[test]
fn drop_correctness_full_pop() {
    let counter = Arc::new(AtomicUsize::new(0));
    {
        let q = MutexRingQueue::<TrackedItem>::new(4);
        for i in 0..4u32 {
            q.push(make_tracked(&counter, i));
        }
        for _ in 0..4 {
            let _ = q.pop();
        }
    }
    assert_eq!(counter.load(Ordering::SeqCst), 4);
}

// ---------------------------------------------------------------------------
// try_push returns the original item back on failure
// ---------------------------------------------------------------------------

#[test]
fn try_push_returns_original_item() {
    let q = MutexRingQueue::<String>::new(1);
    q.push("first".to_string());
    let result = q.try_push("second".to_string());
    assert_eq!(result, Err("second".to_string()));
}

// ---------------------------------------------------------------------------
// Mixed blocking + non-blocking across threads
// ---------------------------------------------------------------------------

#[test]
fn mixed_blocking_and_nonblocking() {
    let q = Arc::new(MutexRingQueue::<usize>::new(32));
    let n = 10_000usize;
    let barrier = Arc::new(Barrier::new(4));

    let qp1 = q.clone();
    let b1 = barrier.clone();
    let blocking_producer = thread::spawn(move || {
        b1.wait();
        for i in 0..n {
            qp1.push(i);
        }
    });

    let qp2 = q.clone();
    let b2 = barrier.clone();
    let try_producer = thread::spawn(move || {
        b2.wait();
        let mut pushed = 0usize;
        let mut val = n; // start from n so values don't overlap
        while pushed < n {
            match qp2.try_push(val) {
                Ok(()) => {
                    pushed += 1;
                    val += 1;
                }
                Err(_) => thread::yield_now(),
            }
        }
    });

    let total = n * 2;
    let collected = Arc::new(Mutex::new(Vec::with_capacity(total)));

    let qc1 = q.clone();
    let b3 = barrier.clone();
    let coll1 = collected.clone();
    let blocking_consumer = thread::spawn(move || {
        b3.wait();
        for _ in 0..n {
            let v = qc1.pop();
            coll1.lock().unwrap().push(v);
        }
    });

    let qc2 = q.clone();
    let b4 = barrier.clone();
    let coll2 = collected.clone();
    let try_consumer = thread::spawn(move || {
        b4.wait();
        let mut popped = 0usize;
        while popped < n {
            match qc2.try_pop() {
                Some(v) => {
                    coll2.lock().unwrap().push(v);
                    popped += 1;
                }
                None => thread::yield_now(),
            }
        }
    });

    blocking_producer.join().unwrap();
    try_producer.join().unwrap();
    blocking_consumer.join().unwrap();
    try_consumer.join().unwrap();

    let mut items = collected.lock().unwrap().clone();
    items.sort();
    items.dedup();
    assert_eq!(items.len(), total, "expected {total} unique items, got {}", items.len());
}

// ---------------------------------------------------------------------------
// Asymmetric: many producers, few consumers
// ---------------------------------------------------------------------------

#[test]
fn stress_many_producers_few_consumers() {
    const PRODUCERS: usize = 16;
    const CONSUMERS: usize = 2;
    const PER_PRODUCER: usize = 10_000;
    let total = PRODUCERS * PER_PRODUCER;

    let q = Arc::new(MutexRingQueue::<usize>::new(128));
    let barrier = Arc::new(Barrier::new(PRODUCERS + CONSUMERS));
    let remaining = Arc::new(AtomicUsize::new(total));
    let sum_pushed = Arc::new(AtomicUsize::new(0));
    let sum_popped = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    for p in 0..PRODUCERS {
        let qp = q.clone();
        let b = barrier.clone();
        let sp = sum_pushed.clone();
        handles.push(thread::spawn(move || {
            b.wait();
            let base = p * PER_PRODUCER;
            for i in 0..PER_PRODUCER {
                let val = base + i;
                qp.push(val);
                sp.fetch_add(val, Ordering::Relaxed);
            }
        }));
    }

    for _ in 0..CONSUMERS {
        let qc = q.clone();
        let b = barrier.clone();
        let rem = remaining.clone();
        let sc = sum_popped.clone();
        handles.push(thread::spawn(move || {
            b.wait();
            loop {
                let prev = rem.fetch_update(Ordering::AcqRel, Ordering::Acquire, |x| {
                    if x == 0 { None } else { Some(x - 1) }
                });
                if prev.is_err() {
                    break;
                }
                let v = qc.pop();
                sc.fetch_add(v, Ordering::Relaxed);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(sum_pushed.load(Ordering::SeqCst), sum_popped.load(Ordering::SeqCst));
}

// ---------------------------------------------------------------------------
// Asymmetric: few producers, many consumers
// ---------------------------------------------------------------------------

#[test]
fn stress_few_producers_many_consumers() {
    const PRODUCERS: usize = 2;
    const CONSUMERS: usize = 16;
    const PER_PRODUCER: usize = 10_000;
    let total = PRODUCERS * PER_PRODUCER;

    let q = Arc::new(MutexRingQueue::<usize>::new(128));
    let barrier = Arc::new(Barrier::new(PRODUCERS + CONSUMERS));
    let remaining = Arc::new(AtomicUsize::new(total));
    let count_popped = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    for p in 0..PRODUCERS {
        let qp = q.clone();
        let b = barrier.clone();
        handles.push(thread::spawn(move || {
            b.wait();
            let base = p * PER_PRODUCER;
            for i in 0..PER_PRODUCER {
                qp.push(base + i);
            }
        }));
    }

    for _ in 0..CONSUMERS {
        let qc = q.clone();
        let b = barrier.clone();
        let rem = remaining.clone();
        let cp = count_popped.clone();
        handles.push(thread::spawn(move || {
            b.wait();
            loop {
                let prev = rem.fetch_update(Ordering::AcqRel, Ordering::Acquire, |x| {
                    if x == 0 { None } else { Some(x - 1) }
                });
                if prev.is_err() {
                    break;
                }
                let _ = qc.pop();
                cp.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(count_popped.load(Ordering::SeqCst), total);
}

// ---------------------------------------------------------------------------
// Stress with varying capacities
// ---------------------------------------------------------------------------

#[test]
fn stress_varying_capacities() {
    for &cap in &[1, 2, 7, 64, 256, 1024] {
        let q = Arc::new(MutexRingQueue::<usize>::new(cap));
        let n = 5_000usize;

        let qp = q.clone();
        let prod = thread::spawn(move || {
            for i in 0..n {
                qp.push(i);
            }
        });

        let qc = q.clone();
        let cons = thread::spawn(move || {
            let mut sum = 0usize;
            for _ in 0..n {
                sum += qc.pop();
            }
            sum
        });

        prod.join().unwrap();
        let sum = cons.join().unwrap();
        let expected: usize = (0..n).sum();
        assert_eq!(sum, expected, "sum mismatch at capacity {cap}");
    }
}

// ---------------------------------------------------------------------------
// Large volume SPSC (1M items)
// ---------------------------------------------------------------------------

#[test]
fn large_volume_spsc() {
    let q = Arc::new(MutexRingQueue::<u64>::new(256));
    let n = 1_000_000u64;

    let qp = q.clone();
    let prod = thread::spawn(move || {
        for i in 0..n {
            qp.push(i);
        }
    });

    let qc = q.clone();
    let cons = thread::spawn(move || {
        let mut sum = 0u64;
        for _ in 0..n {
            sum = sum.wrapping_add(qc.pop());
        }
        sum
    });

    prod.join().unwrap();
    let sum = cons.join().unwrap();
    let expected: u64 = (0..n).sum();
    assert_eq!(sum, expected);
}

// ---------------------------------------------------------------------------
// Zero-sized type (ZST)
// ---------------------------------------------------------------------------

#[test]
fn zero_sized_type() {
    let q = MutexRingQueue::<()>::new(8);
    for _ in 0..8 {
        q.push(());
    }
    assert_eq!(q.try_push(()), Err(()));
    for _ in 0..8 {
        assert_eq!(q.try_pop(), Some(()));
    }
    assert_eq!(q.try_pop(), None);
}

// ---------------------------------------------------------------------------
// Concurrent try_push / try_pop (no deadlock, no panic)
// ---------------------------------------------------------------------------

#[test]
fn concurrent_try_operations_no_deadlock() {
    let q = Arc::new(MutexRingQueue::<usize>::new(16));
    let barrier = Arc::new(Barrier::new(8));
    let mut handles = Vec::new();

    for t in 0..4 {
        let qp = q.clone();
        let b = barrier.clone();
        handles.push(thread::spawn(move || {
            b.wait();
            let mut pushed = 0usize;
            for i in 0..50_000 {
                if qp.try_push(t * 50_000 + i).is_ok() {
                    pushed += 1;
                }
            }
            pushed
        }));
    }

    for _ in 0..4 {
        let qc = q.clone();
        let b = barrier.clone();
        handles.push(thread::spawn(move || {
            b.wait();
            let mut popped = 0usize;
            for _ in 0..50_000 {
                if qc.try_pop().is_some() {
                    popped += 1;
                }
            }
            popped
        }));
    }

    for h in handles {
        let _ = h.join().unwrap();
    }
}

