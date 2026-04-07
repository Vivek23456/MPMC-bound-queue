//! Bounded multi-producer multi-consumer queue (`std` only).

mod mutex_ring;

pub use mutex_ring::MutexRingQueue;

/// Fixed-capacity queue: multiple threads may `push` and `pop` concurrently.
/// When full, `push` blocks; when empty, `pop` blocks.
pub trait BoundedQueue<T: Send>: Send + Sync {
    fn new(capacity: usize) -> Self
    where
        Self: Sized;

    /// Push an item into the queue. Blocks if the queue is full.
    fn push(&self, item: T);

    /// Pop an item from the queue. Blocks if the queue is empty.
    fn pop(&self) -> T;

    /// Try to push without blocking. Returns `Err(item)` if full.
    fn try_push(&self, item: T) -> Result<(), T>;

    /// Try to pop without blocking. Returns `None` if empty.
    fn try_pop(&self) -> Option<T>;
}
