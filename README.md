 # Bounded MPMC Queue

A bounded, multi-producer multi-consumer (MPMC) queue implemented in Rust using only `std`.

## Design

The queue uses a **Mutex + two Condvars + fixed ring buffer** approach:

- A single `Mutex<Inner<T>>` serializes all access to the buffer.
- `not_full` condvar: producers wait here when the buffer is at capacity.
- `not_empty` condvar: consumers wait here when the buffer is empty.
- The buffer is a fixed-size `Vec<MaybeUninit<T>>` with `head` and `len` indices for O(1) ring-buffer push/pop.

```mermaid
flowchart LR
  subgraph producers [Producers]
    P1[push]
    P2[push]
  end
  subgraph queue [MutexRingQueue]
    M[Mutex]
    NF["not_full (Condvar)"]
    NE["not_empty (Condvar)"]
    RB[RingBuffer]
  end
  subgraph consumers [Consumers]
    C1[pop]
    C2[pop]
  end
  P1 --> M
  P2 --> M
  M --> RB
  RB --> M
  M --> NF
  M --> NE
  C1 --> M
  C2 --> M
