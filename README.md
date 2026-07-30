# appendstore

`appendstore` is a small, embeddable append-only key/value store for Go. It
provides crash recovery, checksummed records, optional in-memory secondary
indexes, an LRU value cache, and atomic compaction using only the Go standard
library.

The module is intended for applications that need a durable local store owned
by one process. It is not a distributed database or a transactional SQL
engine.

## Install

```sh
go get github.com/sirrobot01/appendstore
```

## Usage

```go
package main

import (
	"fmt"
	"log"

	"github.com/sirrobot01/appendstore"
)

func main() {
	store, err := appendstore.Open("records.db", appendstore.Options{
		IndexedFields: []string{"kind"},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	err = store.Put("greeting", []byte("hello"), &appendstore.PutOptions{
		Attributes: map[string]string{"kind": "message"},
	})
	if err != nil {
		log.Fatal(err)
	}

	value, err := store.Get("greeting")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(value))
	fmt.Println(store.KeysBy("kind", "message"))
}
```

## Process model

Only one process may open a database path at a time. `Open` acquires an
exclusive operating-system lock and returns `ErrStoreLocked` when another
process owns the store. The adjacent `.lock` file remains after `Close` by
design; the operating-system lock, not the file's presence, indicates
ownership. Locks are released automatically when a process exits.

Calls on one `Store` are safe for concurrent use by multiple goroutines.

## Durability

Each mutation is appended immediately. `Options.SyncInterval` controls when
data is synchronized to durable storage:

- `0` synchronizes every mutation.
- A positive duration synchronizes periodically.
- A negative duration disables automatic synchronization, including on
  `Close`; call `Sync` explicitly when needed.

On startup, appendstore rebuilds its indexes, discards an incomplete trailing
record, and rejects checksum corruption. Compaction writes and synchronizes a
replacement before atomically installing it. Compaction blocks all reads and
writes until the replacement is installed; the pause grows with the size of
the live data.

Only version 4 logs are supported. `Open` returns `ErrUnsupportedVersion` for
version 1–3 logs; open them once with appendstore v0.1.x to migrate them.

## Limits

- All keys and index metadata stay in memory. The store is not suitable for
  keyspaces that exceed available RAM.
- Each value is encoded in one in-memory buffer on write and read. Very large
  values increase memory use accordingly (hard limit: 2 GiB per value).
- Secondary indexes support exact-match equality only; there are no range or
  prefix queries.

## Status

The on-disk format is versioned, but the public API should be considered
pre-1.0. Back up important data before upgrading between early releases.
