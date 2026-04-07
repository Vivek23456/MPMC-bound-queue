use std::mem::MaybeUninit;
use std::sync::{Condvar, Mutex};

use crate::BoundedQueue;

/// [`Mutex`] + two [`Condvar`]s + fixed ring buffer of [`MaybeUninit`] slots.
pub struct MutexRingQueue<T> {
    state: Mutex<Inner<T>>,
    not_full: Condvar,
    not_empty: Condvar,
}

struct Inner<T> {
    buffer: Vec<MaybeUninit<T>>,
    head: usize,
    len: usize,
}

impl<T> Inner<T> {
    fn capacity(&self) -> usize {
        self.buffer.len()
    }

    fn is_full(&self) -> bool {
        self.len == self.capacity()
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl<T> MutexRingQueue<T> {
    fn push_inner(&self, inner: &mut Inner<T>, item: T) {
        let cap = inner.capacity();
        let idx = (inner.head + inner.len) % cap;
        // `MaybeUninit::write` is safe. The safety invariant we rely on overall is that this
        // slot is only written when logically empty, and only accessed under the mutex.
        inner.buffer[idx].write(item);
        inner.len += 1;
        self.not_empty.notify_one();
    }

    fn pop_inner(&self, inner: &mut Inner<T>) -> T {
        let cap = inner.capacity();
        let idx = inner.head;
        // SAFETY: `len > 0` is guaranteed by the caller after waiting until not empty.
        // Slot `idx` is occupied; we move it out exactly once and leave the slot logically empty.
        let value = unsafe { inner.buffer[idx].assume_init_read() };
        inner.head = (inner.head + 1) % cap;
        inner.len -= 1;
        self.not_full.notify_one();
        value
    }
}

impl<T> Drop for MutexRingQueue<T> {
    fn drop(&mut self) {
        let Ok(mut inner) = self.state.lock() else {
            return;
        };
        let cap = inner.capacity();
        while inner.len > 0 {
            let idx = inner.head;
            // SAFETY: Same invariant as `pop`: under the mutex, `len > 0` implies slot `idx`
            // is initialized and must be dropped exactly once.
            unsafe {
                inner.buffer[idx].assume_init_drop();
            }
            inner.head = (inner.head + 1) % cap;
            inner.len -= 1;
        }
    }
}

impl<T: Send> BoundedQueue<T> for MutexRingQueue<T> {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "MutexRingQueue::new: capacity must be positive");
        let buffer: Vec<MaybeUninit<T>> = (0..capacity).map(|_| MaybeUninit::uninit()).collect();
        MutexRingQueue {
            state: Mutex::new(Inner {
                buffer,
                head: 0,
                len: 0,
            }),
            not_full: Condvar::new(),
            not_empty: Condvar::new(),
        }
    }

    fn push(&self, item: T) {
        let mut guard = self.state.lock().unwrap();
        let item = item;
        loop {
            if !guard.is_full() {
                self.push_inner(&mut guard, item);
                return;
            }
            guard = self.not_full.wait(guard).unwrap();
        }
    }

    fn pop(&self) -> T {
        let mut guard = self.state.lock().unwrap();
        loop {
            if !guard.is_empty() {
                return self.pop_inner(&mut guard);
            }
            guard = self.not_empty.wait(guard).unwrap();
        }
    }

    fn try_push(&self, item: T) -> Result<(), T> {
        let mut guard = self.state.lock().unwrap();
        if guard.is_full() {
            return Err(item);
        }
        self.push_inner(&mut guard, item);
        Ok(())
    }

    fn try_pop(&self) -> Option<T> {
        let mut guard = self.state.lock().unwrap();
        if guard.is_empty() {
            return None;
        }
        Some(self.pop_inner(&mut guard))
    }
}
