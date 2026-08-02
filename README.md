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

New records are always written in the version 4 format. `Open` reads legacy
version 1–3 logs and migrates them to version 4 in one atomic compaction
before it returns. `Open` returns `ErrUnsupportedVersion` for logs newer than
version 4.

## Format migration

Migration is one-way. A build compiled against the older format rejects a
migrated log with `ErrUnsupportedVersion`, so an application that upgrades and
then rolls back cannot read its own data.

To keep a way back, `Open` copies the log to `<path>.v<version>.bak` before it
migrates. A backup that cannot be written fails the open and leaves the log
untouched. An existing backup is never replaced, so a repeated attempt cannot
overwrite the original with partly migrated data. Backups are never reclaimed;
delete them when the older release is no longer a target.

Set `Options.OnMigrate` to observe the migration. It runs after the backup is
taken and before the log is rewritten, and an error from it aborts the open:

```go
store, err := appendstore.Open(path, appendstore.Options{
    OnMigrate: func(info appendstore.MigrationInfo) error {
        log.Printf("upgrading %s from v%d to v%d; restore %s to roll back",
            info.Path, info.FromVersion, info.ToVersion, info.Backup)
        return nil
    },
})
```

Set `Options.NoMigrationBackup` to skip the copy. Use it only where the
application has its own way back, or where the disk cannot hold a second copy
of the log.

To roll back, stop the application and restore each backup over its store:

```bash
mv store.db.v3.bak store.db
```

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
