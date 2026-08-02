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

## Log format

New records are always written in the version 5 format. Version 5 is designed
to be the last format change that breaks compatibility. `docs/stable-format.md`
specifies it in full.

Two mechanisms replace the version number for later changes:

- **Feature bits.** The header holds an incompatible mask and a read-only mask.
  `Open` returns `ErrUnsupportedFeature` for an unknown incompatible bit, and
  names the bit. For an unknown read-only bit, `Open` succeeds, `ReadOnly()`
  reports `true`, and every write returns an error that wraps `ErrReadOnly`.
- **Record extensions.** Each record ends with an extension area of tagged
  entries. A reader steps over a tag it does not know, and compaction copies the
  entry unchanged. A later release can therefore add a per-record field that an
  older reader ignores, without a feature bit and without a new version. The
  reader and compaction sides are in place. There is no public API to write an
  extension yet, so the area is empty in every record this release writes.

`Open` still reads legacy version 1–4 logs and migrates them:

- A version 4 log needs only a new header, because its records are already
  valid version 5 records.
- A version 1–3 log is rewritten in one atomic compaction.

`Open` returns `ErrUnsupportedVersion` for a log newer than version 5.

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

A backup only returns the data that existed when the store was migrated. To
roll back a store that has been written since, use the downgrade tool. It writes
a version 3 or version 4 log from the live contents of a version 5 log:

```bash
go run ./cmd/appendstore-downgrade -to 4 store.db store-v4.db
```

The tool refuses to write anything when a record holds data the target format
cannot express. A version 4 target cannot express a record extension. A version
3 target also cannot express an attribute outside its fixed set of named
fields.

The tool refuses a destination that already exists. Pass `-f` to replace one;
the output is renamed into place only after every record is written, so a failed
run leaves the old file untouched.

The tool reads the source log. It writes to the source only to discard an
incomplete trailing record from an interrupted write, which is the same repair
that `Open` performs.

A version 4 log holds the same attribute map as version 5, so that target is
lossless. A version 3 log holds a fixed field per attribute instead, so a value
that was absent comes back as an empty field when the log is read again.

The downgrade tool exists for the transition to version 5. It is removed
together with the version 1–4 migration path.

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
