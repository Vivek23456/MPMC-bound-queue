# Benchmark Results

Environment:
- OS: Linux 6.17.0-20-generic
- Rust crate: `bounded_mpmc_queue`
- Command:
  - `cargo bench --bench throughput -- --sample-size 10 --measurement-time 1 --warm-up-time 1`
- Notes:
  - Criterion reports throughput as elements/second (`elem/s`), where each benchmark element is one queue operation.
  - Values below use Criterion's middle throughput estimate (the median-like center of the reported range).

## Symmetric workload (N producer/consumer pairs)

### Capacity = 64

| Pairs | Throughput |
|---|---:|
| 1 | 1.2043 Melem/s |
| 2 | 647.93 Kelem/s |
| 4 | 618.15 Kelem/s |
| 8 | 544.64 Kelem/s |
| 16 | 535.86 Kelem/s |

### Capacity = 256

| Pairs | Throughput |
|---|---:|
| 1 | 1.3990 Melem/s |
| 2 | 878.73 Kelem/s |
| 4 | 739.91 Kelem/s |
| 8 | 682.20 Kelem/s |
| 16 | 738.79 Kelem/s |

### Capacity = 1024

| Pairs | Throughput |
|---|---:|
| 1 | 1.5098 Melem/s |
| 2 | 1.1467 Melem/s |
| 4 | 1.0124 Melem/s |
| 8 | 895.23 Kelem/s |
| 16 | 834.69 Kelem/s |

## Asymmetric workload (8 producers / 1 consumer)

| Capacity | Throughput |
|---|---:|
| 64 | 501.94 Kelem/s |
| 256 | 527.64 Kelem/s |
| 1024 | 513.03 Kelem/s |

## Quick observations

- Larger capacities generally improve throughput by reducing producer/consumer blocking frequency.
- At high contention (more thread pairs), throughput declines due to lock contention and scheduling overhead.
- The asymmetric `8P/1C` case is bottlenecked by the single consumer and remains around ~0.5 Melem/s.

