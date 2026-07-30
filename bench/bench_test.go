// Package bench compares appendstore against other embedded Go key/value
// stores. It lives in its own module so the main package stays free of
// dependencies.
//
// Run with:
//
//	go test -bench . -benchmem ./bench
package bench

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/sirrobot01/appendstore"
	"github.com/syndtr/goleveldb/leveldb"
	leveldbopt "github.com/syndtr/goleveldb/leveldb/opt"
	bolt "go.etcd.io/bbolt"
)

// kv is the minimal surface each backend must expose for the benchmarks.
type kv interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Close() error
}

type backend struct {
	name string
	open func(tb testing.TB, dir string, syncEveryWrite bool) kv
}

var backends = []backend{
	{"appendstore", openAppendstore},
	{"bbolt", openBbolt},
	{"badger", openBadger},
	{"goleveldb", openGoleveldb},
	{"pebble", openPebble},
}

// --- appendstore ---

type appendstoreKV struct{ store *appendstore.Store }

func openAppendstore(tb testing.TB, dir string, syncEveryWrite bool) kv {
	interval := time.Duration(-1)
	if syncEveryWrite {
		interval = 0
	}
	store, err := appendstore.Open(dir+"/store.db", appendstore.Options{SyncInterval: interval})
	if err != nil {
		tb.Fatalf("open appendstore: %v", err)
	}
	return &appendstoreKV{store: store}
}

func (s *appendstoreKV) Put(key string, value []byte) error { return s.store.Put(key, value, nil) }
func (s *appendstoreKV) Get(key string) ([]byte, error)     { return s.store.Get(key) }
func (s *appendstoreKV) Close() error                       { return s.store.Close() }

// --- bbolt ---

var boltBucket = []byte("bench")

type boltKV struct{ db *bolt.DB }

func openBbolt(tb testing.TB, dir string, syncEveryWrite bool) kv {
	db, err := bolt.Open(dir+"/bolt.db", 0600, nil)
	if err != nil {
		tb.Fatalf("open bbolt: %v", err)
	}
	db.NoSync = !syncEveryWrite
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(boltBucket)
		return err
	})
	if err != nil {
		tb.Fatalf("create bbolt bucket: %v", err)
	}
	return &boltKV{db: db}
}

func (s *boltKV) Put(key string, value []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(boltBucket).Put([]byte(key), value)
	})
}

func (s *boltKV) Get(key string) ([]byte, error) {
	var value []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		found := tx.Bucket(boltBucket).Get([]byte(key))
		if found == nil {
			return fmt.Errorf("key %s not found", key)
		}
		value = append([]byte(nil), found...)
		return nil
	})
	return value, err
}

func (s *boltKV) Close() error { return s.db.Close() }

// --- badger ---

type badgerKV struct{ db *badger.DB }

func openBadger(tb testing.TB, dir string, syncEveryWrite bool) kv {
	options := badger.DefaultOptions(dir + "/badger").
		WithSyncWrites(syncEveryWrite).
		WithLogger(nil)
	db, err := badger.Open(options)
	if err != nil {
		tb.Fatalf("open badger: %v", err)
	}
	return &badgerKV{db: db}
}

func (s *badgerKV) Put(key string, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), value)
	})
}

func (s *badgerKV) Get(key string) ([]byte, error) {
	var value []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	return value, err
}

func (s *badgerKV) Close() error { return s.db.Close() }

// --- goleveldb ---

type goleveldbKV struct {
	db   *leveldb.DB
	sync bool
}

func openGoleveldb(tb testing.TB, dir string, syncEveryWrite bool) kv {
	db, err := leveldb.OpenFile(dir+"/goleveldb", nil)
	if err != nil {
		tb.Fatalf("open goleveldb: %v", err)
	}
	return &goleveldbKV{db: db, sync: syncEveryWrite}
}

func (s *goleveldbKV) Put(key string, value []byte) error {
	return s.db.Put([]byte(key), value, &leveldbopt.WriteOptions{Sync: s.sync})
}

func (s *goleveldbKV) Get(key string) ([]byte, error) {
	return s.db.Get([]byte(key), nil)
}

func (s *goleveldbKV) Close() error { return s.db.Close() }

// --- pebble ---

type pebbleKV struct {
	db   *pebble.DB
	sync *pebble.WriteOptions
}

func openPebble(tb testing.TB, dir string, syncEveryWrite bool) kv {
	db, err := pebble.Open(dir+"/pebble", &pebble.Options{Logger: discardLogger{}})
	if err != nil {
		tb.Fatalf("open pebble: %v", err)
	}
	writeOptions := pebble.NoSync
	if syncEveryWrite {
		writeOptions = pebble.Sync
	}
	return &pebbleKV{db: db, sync: writeOptions}
}

type discardLogger struct{}

func (discardLogger) Infof(string, ...interface{})  {}
func (discardLogger) Errorf(string, ...interface{}) {}
func (discardLogger) Fatalf(string, ...interface{}) {}

func (s *pebbleKV) Put(key string, value []byte) error {
	return s.db.Set([]byte(key), value, s.sync)
}

func (s *pebbleKV) Get(key string) ([]byte, error) {
	found, closer, err := s.db.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	value := append([]byte(nil), found...)
	return value, closer.Close()
}

func (s *pebbleKV) Close() error { return s.db.Close() }

// --- benchmarks ---

func benchKey(i int) string { return fmt.Sprintf("key-%08d", i) }

func benchValue(size int) []byte {
	value := make([]byte, size)
	rng := rand.New(rand.NewSource(42))
	rng.Read(value)
	return value
}

// BenchmarkPut measures sequential writes without fsync.
func BenchmarkPut(b *testing.B) {
	for _, size := range []int{128, 4096} {
		value := benchValue(size)
		for _, bk := range backends {
			b.Run(fmt.Sprintf("%s/%dB", bk.name, size), func(b *testing.B) {
				store := bk.open(b, b.TempDir(), false)
				defer store.Close()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := store.Put(benchKey(i), value); err != nil {
						b.Fatalf("put: %v", err)
					}
				}
			})
		}
	}
}

// BenchmarkPutSync measures writes with an fsync per write.
func BenchmarkPutSync(b *testing.B) {
	value := benchValue(128)
	for _, bk := range backends {
		b.Run(bk.name+"/128B", func(b *testing.B) {
			store := bk.open(b, b.TempDir(), true)
			defer store.Close()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := store.Put(benchKey(i), value); err != nil {
					b.Fatalf("put: %v", err)
				}
			}
		})
	}
}

// BenchmarkGetRandom measures random point reads over 100k preloaded keys.
// The working set defeats appendstore's 1000-entry LRU cache, so this measures
// its disk read path against the other stores.
func BenchmarkGetRandom(b *testing.B) {
	const numKeys = 100_000
	value := benchValue(512)
	for _, bk := range backends {
		b.Run(bk.name+"/100k-keys", func(b *testing.B) {
			store := bk.open(b, b.TempDir(), false)
			defer store.Close()
			for i := 0; i < numKeys; i++ {
				if err := store.Put(benchKey(i), value); err != nil {
					b.Fatalf("preload: %v", err)
				}
			}
			rng := rand.New(rand.NewSource(1))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := benchKey(rng.Intn(numKeys))
				if _, err := store.Get(key); err != nil {
					b.Fatalf("get %s: %v", key, err)
				}
			}
		})
	}
}

// BenchmarkGetHot measures random point reads over a 500-key working set that
// fits in appendstore's LRU cache.
func BenchmarkGetHot(b *testing.B) {
	const numKeys = 500
	value := benchValue(512)
	for _, bk := range backends {
		b.Run(bk.name+"/500-keys", func(b *testing.B) {
			store := bk.open(b, b.TempDir(), false)
			defer store.Close()
			for i := 0; i < numKeys; i++ {
				if err := store.Put(benchKey(i), value); err != nil {
					b.Fatalf("preload: %v", err)
				}
			}
			rng := rand.New(rand.NewSource(1))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := benchKey(rng.Intn(numKeys))
				if _, err := store.Get(key); err != nil {
					b.Fatalf("get %s: %v", key, err)
				}
			}
		})
	}
}
