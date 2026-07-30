# Benchmarks

This module compares appendstore with other embedded Go key/value stores:
[bbolt](https://github.com/etcd-io/bbolt),
[BadgerDB](https://github.com/dgraph-io/badger),
[goleveldb](https://github.com/syndtr/goleveldb), and
[Pebble](https://github.com/cockroachdb/pebble).

The module is separate from the main package. The main package keeps zero
dependencies.

## Run

```sh
go test -bench . -benchmem ./bench
```

## Results

Apple M1 Pro, macOS, Go 1.26.5, 2026-07-31.

### Time per operation

| Benchmark | appendstore | bbolt | badger | goleveldb | pebble |
|---|---|---|---|---|---|
| Put 128 B, no fsync | 2.5 µs | 30.5 µs | 8.6 µs | 3.7 µs | **1.1 µs** |
| Put 4 KiB, no fsync | **6.5 µs** | 44.6 µs | 10.9 µs | 27.3 µs | 42.6 µs |
| Put 128 B, fsync per write | **4.1 ms** | 9.1 ms | 0.04 ms* | 4.2 ms | 4.1 ms |
| Get, random over 100k keys | **0.91 µs** | 1.29 µs | 1.94 µs | 2.94 µs | 6.85 µs |
| Get, hot 500-key working set | **0.27 µs** | 0.73 µs | 1.05 µs | 0.48 µs | 0.62 µs |

### Allocated memory per operation

Bytes per operation, with allocation count in parentheses.

| Benchmark | appendstore | bbolt | badger | goleveldb | pebble |
|---|---|---|---|---|---|
| Put 128 B, no fsync | 139 (3) | 59,764 (60) | 1,990 (41) | 169 (6) | **36 (2)** |
| Put 4 KiB, no fsync | **143 (3)** | 93,964 (68) | 20,511 (52) | 419 (6) | 366 (2) |
| Put 128 B, fsync per write | **158 (2)** | 31,414 (50) | 2,074 (41) | 225 (5) | 176 (1) |
| Get, random over 100k keys | 1,138 (5) | 1,112 (12) | 2,269 (15) | 1,411 (17) | **616 (4)** |
| Get, hot 500-key working set | **532 (2)** | 1,012 (10) | 2,002 (14) | 644 (6) | 549 (3) |

\* Badger syncs its mmapped write-ahead log with `msync(MS_SYNC)`, which does
not flush the drive cache on macOS. The other stores use `F_FULLFSYNC` through
Go's `File.Sync`. The Badger number is not comparable on this platform.

## Method

- Each store runs with its default options.
- "No fsync" disables synchronous writes in each store.
- The random-read benchmark preloads 100,000 keys with 512-byte values. The
  working set defeats appendstore's 1000-entry LRU cache, so the benchmark
  measures the uncached read path. appendstore serves uncached reads from a
  read-only memory map of the log, with a file-read fallback.
- The hot-read benchmark uses 500 keys, which fit in appendstore's cache.
- Allocation numbers come from `-benchmem` and include the benchmark
  harness's key formatting (about two allocations per operation), which is
  identical for every store.
