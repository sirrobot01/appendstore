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

Apple M1 Pro, macOS, Go 1.26.5, 2026-07-30.

| Benchmark | appendstore | bbolt | badger | goleveldb | pebble |
|---|---|---|---|---|---|
| Put 128 B, no fsync | 2.5 µs | 32.0 µs | 6.7 µs | 3.3 µs | **1.1 µs** |
| Put 4 KiB, no fsync | **7.8 µs** | 59.1 µs | 11.4 µs | 27.9 µs | 41.5 µs |
| Put 128 B, fsync per write | **4.0 ms** | 8.4 ms | 0.04 ms* | 4.1 ms | 4.1 ms |
| Get, random over 100k keys | 1.6 µs | **1.3 µs** | 2.1 µs | 3.1 µs | 6.8 µs |
| Get, hot 500-key working set | **0.27 µs** | 0.73 µs | 1.15 µs | 0.49 µs | 0.64 µs |

\* Badger syncs its mmapped write-ahead log with `msync(MS_SYNC)`, which does
not flush the drive cache on macOS. The other stores use `F_FULLFSYNC` through
Go's `File.Sync`. The Badger number is not comparable on this platform.

## Method

- Each store runs with its default options.
- "No fsync" disables synchronous writes in each store.
- The random-read benchmark preloads 100,000 keys with 512-byte values. The
  working set defeats appendstore's 1000-entry LRU cache, so the benchmark
  measures the disk read path.
- The hot-read benchmark uses 500 keys, which fit in appendstore's cache.
