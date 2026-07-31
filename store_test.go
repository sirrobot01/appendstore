package appendstore

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

func openTestStore(t *testing.T, path string, configure ...func(*Options)) *Store {
	t.Helper()
	options := Options{
		CacheSize:           8,
		SyncInterval:        -1,
		CompactionThreshold: 0.2,
		IndexedFields:       []string{"category", "provider", "status"},
	}
	for _, fn := range configure {
		fn(&options)
	}
	store, err := Open(path, options)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	return store
}

func TestStoreCRUDReopenAndDefensiveCopies(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	store := openTestStore(t, path)
	meta := &PutOptions{Attributes: map[string]string{
		"category":   "movies",
		"provider":   "provider-a",
		"status":     "ready",
		"name":       "example",
		"total_size": "123",
		"protocol":   "test",
		"bad":        "true",
		"added_on":   "456",
	}}
	original := []byte("first value")
	if err := store.Put("key", original, meta); err != nil {
		t.Fatalf("put: %v", err)
	}
	original[0] = 'X'
	meta.Attributes["category"] = "changed input"

	got, err := store.Get("key")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != "first value" {
		t.Fatalf("get = %q, want %q", got, "first value")
	}
	got[0] = 'X'
	gotAgain, err := store.Get("key")
	if err != nil {
		t.Fatalf("cached get: %v", err)
	}
	if string(gotAgain) != "first value" {
		t.Fatalf("cached value was mutated: %q", gotAgain)
	}

	gotMeta, err := store.GetMetadata("key")
	if err != nil {
		t.Fatalf("get metadata: %v", err)
	}
	gotMeta.Attributes["category"] = "changed"
	gotMetaAgain, err := store.GetMetadata("key")
	if err != nil {
		t.Fatalf("get metadata again: %v", err)
	}
	if gotMetaAgain.Attribute("category") != "movies" {
		t.Fatalf("metadata was mutated: category = %q", gotMetaAgain.Attribute("category"))
	}

	if !store.Exists("key") || store.Len() != 1 {
		t.Fatalf("unexpected existence/count: exists=%v len=%d", store.Exists("key"), store.Len())
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close before reopen: %v", err)
	}

	reopened := openTestStore(t, path)
	got, err = reopened.Get("key")
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if string(got) != "first value" {
		t.Fatalf("value after reopen = %q", got)
	}
	gotMeta, err = reopened.GetMetadata("key")
	if err != nil {
		t.Fatalf("metadata after reopen: %v", err)
	}
	if gotMeta.Attribute("category") != "movies" || gotMeta.Attribute("protocol") != "test" || gotMeta.Attribute("added_on") != "456" {
		t.Fatalf("metadata after reopen = %+v", gotMeta)
	}

	if err := reopened.Delete("key"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := reopened.Get("key"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("get deleted error = %v, want ErrKeyNotFound", err)
	}
	if err := reopened.Delete("key"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("delete missing error = %v, want ErrKeyNotFound", err)
	}
}

func TestGenericSecondaryIndexesTrackUpdates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	store := openTestStore(t, path)
	if err := store.Put("one", []byte("1"), &PutOptions{Attributes: map[string]string{
		"category": "movies",
		"status":   "ready",
		"ignored":  "not-indexed",
	}}); err != nil {
		t.Fatalf("put one: %v", err)
	}
	if err := store.Put("two", []byte("2"), &PutOptions{Attributes: map[string]string{
		"category": "shows",
		"status":   "ready",
	}}); err != nil {
		t.Fatalf("put two: %v", err)
	}
	if got := store.CountBy("status", "ready"); got != 2 {
		t.Fatalf("ready count = %d, want 2", got)
	}
	if got := store.KeysBy("category", "movies"); len(got) != 1 || got[0] != "one" {
		t.Fatalf("movie keys = %v, want [one]", got)
	}
	if got := store.AttributeValues("category"); fmt.Sprint(got) != "[movies shows]" {
		t.Fatalf("category values = %v", got)
	}
	if got := store.KeysBy("ignored", "not-indexed"); len(got) != 0 {
		t.Fatalf("unindexed query returned %v", got)
	}
	if err := store.Put("one", []byte("updated"), &PutOptions{Attributes: map[string]string{
		"category": "shows",
		"status":   "pending",
	}}); err != nil {
		t.Fatalf("update one: %v", err)
	}
	if got := store.CountBy("category", "movies"); got != 0 {
		t.Fatalf("stale movie index count = %d", got)
	}
	if got := store.CountBy("status", "ready"); got != 1 {
		t.Fatalf("ready count after update = %d, want 1", got)
	}
}

func TestV4ChecksumDetectsValueCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	store := openTestStore(t, path)
	if err := store.Put("key", []byte("checksum-value"), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	entry := store.index.Get("key")
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open corrupt target: %v", err)
	}
	if _, err := file.WriteAt([]byte{'X'}, entry.Offset+2); err != nil {
		t.Fatalf("corrupt value: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close corrupt target: %v", err)
	}
	_, err = Open(path, Options{})
	if !errors.Is(err, ErrCorruptedData) {
		t.Fatalf("open corrupted value error = %v, want ErrCorruptedData", err)
	}
}

func TestOpenRejectsUnsupportedLogVersions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	header := make([]byte, logHeaderSize)
	copy(header[:4], logMagic)
	binary.LittleEndian.PutUint32(header[4:8], logVersion+1)
	if err := os.WriteFile(path, header, 0600); err != nil {
		t.Fatalf("write log: %v", err)
	}
	_, err := Open(path, Options{})
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("open v%d error = %v, want ErrUnsupportedVersion", logVersion+1, err)
	}
}

func TestLegacyLogsMigrateToV4(t *testing.T) {
	for _, version := range []uint32{1, 2, 3} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store.db")
			writeLegacyLog(t, path, version, true)
			store := openTestStore(t, path)
			if store.log.version != logVersion {
				t.Fatalf("open version = %d, want %d", store.log.version, logVersion)
			}
			value, err := store.Get("legacy-key")
			if err != nil {
				t.Fatalf("get migrated value: %v", err)
			}
			if string(value) != "legacy-value" {
				t.Fatalf("migrated value = %q", value)
			}
			meta, err := store.GetMetadata("legacy-key")
			if err != nil {
				t.Fatalf("get migrated metadata: %v", err)
			}
			if meta.Attribute("category") != "movies" || meta.Attribute("provider") != "provider-a" || meta.Attribute("total_size") != "1234" {
				t.Fatalf("migrated metadata = %#v", meta.Attributes)
			}
			if version >= 3 {
				if meta.Attribute("protocol") != "torrent" || meta.Attribute("bad") != "true" || meta.Attribute("added_on") != "456" {
					t.Fatalf("v3 metadata = %#v", meta.Attributes)
				}
			} else if _, exists := meta.Attributes["protocol"]; exists {
				t.Fatalf("v%d unexpectedly gained protocol metadata", version)
			}
			if got := store.KeysBy("category", "movies"); len(got) != 1 || got[0] != "legacy-key" {
				t.Fatalf("migrated category index = %v", got)
			}
			header, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read migrated header: %v", err)
			}
			if got := binary.LittleEndian.Uint32(header[4:8]); got != logVersion {
				t.Fatalf("disk version = %d, want %d", got, logVersion)
			}
		})
	}
}

func TestEmptyLegacyLogMigratesBeforeFirstWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	writeLegacyLog(t, path, 3, false)
	store := openTestStore(t, path)
	if err := store.Put("new", []byte("v4"), nil); err != nil {
		t.Fatalf("put after empty migration: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened := openTestStore(t, path)
	value, err := reopened.Get("new")
	if err != nil || string(value) != "v4" {
		t.Fatalf("get after reopen = %q, %v", value, err)
	}
}

func TestRecoversLegacyInterruptedCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	writeLegacyLog(t, path+legacyCompactionSuffix, 3, true)
	store := openTestStore(t, path)
	value, err := store.Get("legacy-key")
	if err != nil || string(value) != "legacy-value" {
		t.Fatalf("recovered legacy compact value = %q, %v", value, err)
	}
	if store.log.version != logVersion {
		t.Fatalf("recovered version = %d, want %d", store.log.version, logVersion)
	}
	if _, err := os.Stat(path + legacyCompactionSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy compact artifact remains: %v", err)
	}
}

func writeLegacyLog(t *testing.T, path string, version uint32, withRecord bool) {
	t.Helper()
	var buf bytes.Buffer
	header := make([]byte, logHeaderSize)
	copy(header[:4], logMagic)
	binary.LittleEndian.PutUint32(header[4:8], version)
	buf.Write(header)
	if withRecord {
		writeLegacyU32(&buf, len("legacy-key"))
		buf.WriteString("legacy-key")
		writeLegacyU32(&buf, len("legacy-value"))
		buf.WriteString("legacy-value")
		flags := byte(0)
		if version >= 3 {
			flags = 2
		}
		buf.WriteByte(flags)
		writeLegacyField(&buf, "movies")
		writeLegacyField(&buf, "provider-a")
		writeLegacyField(&buf, "ready")
		writeLegacyField(&buf, "Example")
		var number [8]byte
		binary.LittleEndian.PutUint64(number[:], 1234)
		buf.Write(number[:])
		if version >= 3 {
			writeLegacyField(&buf, "torrent")
			binary.LittleEndian.PutUint64(number[:], 456)
			buf.Write(number[:])
		}
	}
	if err := os.WriteFile(path, buf.Bytes(), 0600); err != nil {
		t.Fatalf("write v%d log: %v", version, err)
	}
}

func writeLegacyU32(buf *bytes.Buffer, value int) {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], uint32(value))
	buf.Write(encoded[:])
}

func writeLegacyField(buf *bytes.Buffer, value string) {
	var length [2]byte
	binary.LittleEndian.PutUint16(length[:], uint16(len(value)))
	buf.Write(length[:])
	buf.WriteString(value)
}

func TestReadsAcrossMapGrowthAndCompaction(t *testing.T) {
	oldSlack := defaultMapSlack
	defaultMapSlack = 4 << 10 // force a remap every few appends
	t.Cleanup(func() { defaultMapSlack = oldSlack })

	path := filepath.Join(t.TempDir(), "store.db")
	store := openTestStore(t, path)
	payload := bytes.Repeat([]byte("p"), 1024)
	for i := 0; i < 64; i++ {
		value := append([]byte(fmt.Sprintf("%03d-", i)), payload...)
		if err := store.Put(fmt.Sprintf("key-%03d", i), value, nil); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		got, err := store.Get(fmt.Sprintf("key-%03d", i))
		if err != nil {
			t.Fatalf("get %d after append: %v", i, err)
		}
		if !bytes.HasPrefix(got, []byte(fmt.Sprintf("%03d-", i))) {
			t.Fatalf("value %d = %q", i, got[:4])
		}
	}
	if err := store.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	seen := 0
	err := store.ForEach(func(key string, value []byte) error {
		seen++
		if !bytes.HasPrefix(value, []byte(key[len("key-"):]+"-")) {
			return fmt.Errorf("key %s has value prefix %q", key, value[:4])
		}
		return nil
	})
	if err != nil {
		t.Fatalf("iterate after compaction: %v", err)
	}
	if seen != 64 {
		t.Fatalf("iterated %d values, want 64", seen)
	}
}

func TestStoreRecoversIncompleteTrailingRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	store := openTestStore(t, path)
	if err := store.Put("complete", []byte("value"), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	validInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat valid log: %v", err)
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open append: %v", err)
	}
	if _, err := file.Write([]byte{5, 0, 0}); err != nil {
		t.Fatalf("append incomplete record: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close appended file: %v", err)
	}

	recovered := openTestStore(t, path)
	got, err := recovered.Get("complete")
	if err != nil {
		t.Fatalf("get recovered value: %v", err)
	}
	if string(got) != "value" {
		t.Fatalf("recovered value = %q", got)
	}
	recoveredInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat recovered log: %v", err)
	}
	if recoveredInfo.Size() != validInfo.Size() {
		t.Fatalf("recovered size = %d, want %d", recoveredInfo.Size(), validInfo.Size())
	}
}

func TestStoreRecoversPartiallyWrittenV4Record(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.db")
	store := openTestStore(t, path)
	if err := store.Put("complete", []byte("kept"), nil); err != nil {
		t.Fatalf("put complete: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close complete store: %v", err)
	}
	validInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat complete store: %v", err)
	}

	partialPath := filepath.Join(dir, "partial.db")
	partial := openTestStore(t, partialPath)
	if err := partial.Put("partial", bytes.Repeat([]byte("x"), 1024), nil); err != nil {
		t.Fatalf("put partial source: %v", err)
	}
	if err := partial.Close(); err != nil {
		t.Fatalf("close partial source: %v", err)
	}
	encoded, err := os.ReadFile(partialPath)
	if err != nil {
		t.Fatalf("read partial source: %v", err)
	}
	encoded = encoded[logHeaderSize:]
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open destination: %v", err)
	}
	if _, err := file.Write(encoded[:len(encoded)/2]); err != nil {
		t.Fatalf("append partial record: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close destination: %v", err)
	}

	recovered := openTestStore(t, path)
	value, err := recovered.Get("complete")
	if err != nil || string(value) != "kept" {
		t.Fatalf("get complete after recovery = %q, %v", value, err)
	}
	if recovered.Exists("partial") {
		t.Fatal("partially written record became visible")
	}
	recoveredInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat recovered store: %v", err)
	}
	if recoveredInfo.Size() != validInfo.Size() {
		t.Fatalf("recovered size = %d, want %d", recoveredInfo.Size(), validInfo.Size())
	}
}

func TestStoreRejectsCorruptHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), logHeaderSize), 0600); err != nil {
		t.Fatalf("write corrupt log: %v", err)
	}
	_, err := Open(path, Options{})
	if !errors.Is(err, ErrCorruptedData) {
		t.Fatalf("open corrupt log error = %v, want ErrCorruptedData", err)
	}
}

func TestStoreCompactionAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	store := openTestStore(t, path)
	payload := bytes.Repeat([]byte("x"), 4096)
	for i := 0; i < 20; i++ {
		value := append([]byte(fmt.Sprintf("%02d", i)), payload...)
		if err := store.Put("updated", value, nil); err != nil {
			t.Fatalf("put update %d: %v", i, err)
		}
	}
	if err := store.Put("deleted", payload, nil); err != nil {
		t.Fatalf("put deleted key: %v", err)
	}
	if err := store.Delete("deleted"); err != nil {
		t.Fatalf("delete key: %v", err)
	}
	if !store.NeedsCompaction() {
		t.Fatal("store should need compaction after update churn")
	}
	before := store.DiskSize()
	if err := store.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	after := store.DiskSize()
	if after >= before {
		t.Fatalf("compacted size = %d, want less than %d", after, before)
	}
	if store.NeedsCompaction() {
		t.Fatal("clean compacted store should not need compaction")
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close compacted store: %v", err)
	}

	reopened := openTestStore(t, path)
	got, err := reopened.Get("updated")
	if err != nil {
		t.Fatalf("get after compact/reopen: %v", err)
	}
	if !bytes.HasPrefix(got, []byte("19")) {
		t.Fatalf("latest value was not retained: prefix %q", got[:2])
	}
	if reopened.Exists("deleted") {
		t.Fatal("deleted key returned after compact/reopen")
	}
}

func TestCompactionPreservesFilePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not expose Unix permission bits")
	}
	path := filepath.Join(t.TempDir(), "store.db")
	store := openTestStore(t, path)
	if err := store.Put("key", []byte("old"), nil); err != nil {
		t.Fatalf("put old: %v", err)
	}
	if err := store.Put("key", []byte("new"), nil); err != nil {
		t.Fatalf("put new: %v", err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if err := store.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if got := info.Mode().Perm(); got != 0600 {
		t.Fatalf("permissions = %o, want 600", got)
	}
}

func TestStoreCompletesInterruptedCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	store := openTestStore(t, path)
	if err := store.Put("version", []byte("old"), nil); err != nil {
		t.Fatalf("put old value: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close old store: %v", err)
	}

	readyPath := path + compactionReadySuffix
	ready, err := createAppendLog(readyPath)
	if err != nil {
		t.Fatalf("create ready log: %v", err)
	}
	if _, _, _, _, err := ready.Append("version", []byte("new"), false, nil); err != nil {
		t.Fatalf("append ready value: %v", err)
	}
	if err := ready.Sync(); err != nil {
		t.Fatalf("sync ready log: %v", err)
	}
	if err := ready.Close(); err != nil {
		t.Fatalf("close ready log: %v", err)
	}
	if err := os.Rename(path, path+compactionBackupSuffix); err != nil {
		t.Fatalf("simulate old-log backup: %v", err)
	}

	recovered := openTestStore(t, path)
	got, err := recovered.Get("version")
	if err != nil {
		t.Fatalf("get promoted value: %v", err)
	}
	if string(got) != "new" {
		t.Fatalf("promoted value = %q, want new", got)
	}
	for _, suffix := range []string{compactionTempSuffix, compactionReadySuffix, compactionBackupSuffix} {
		if _, err := os.Stat(path + suffix); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("artifact %s remains after recovery: %v", suffix, err)
		}
	}
}

func TestStoreRollsBackInterruptedCompactionWithoutReadyLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	store := openTestStore(t, path)
	if err := store.Put("version", []byte("old"), nil); err != nil {
		t.Fatalf("put old value: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close old store: %v", err)
	}
	if err := os.Rename(path, path+compactionBackupSuffix); err != nil {
		t.Fatalf("simulate old-log backup: %v", err)
	}
	if err := os.WriteFile(path+compactionTempSuffix, []byte("incomplete"), 0600); err != nil {
		t.Fatalf("write incomplete compacted log: %v", err)
	}

	recovered := openTestStore(t, path)
	got, err := recovered.Get("version")
	if err != nil {
		t.Fatalf("get rolled-back value: %v", err)
	}
	if string(got) != "old" {
		t.Fatalf("rolled-back value = %q, want old", got)
	}
	if _, err := os.Stat(path + compactionTempSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary artifact remains after rollback: %v", err)
	}
}

func TestStoreDoesNotCompactRecordOverhead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	store := openTestStore(t, path)
	if err := store.Put("k", []byte("v"), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	if store.NeedsCompaction() {
		t.Fatal("a clean log should not count record/header overhead as dead data")
	}
}

func TestStoreValidatesEncodedLengths(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	store := openTestStore(t, path)
	if err := store.Put(string(bytes.Repeat([]byte("k"), maxKeySize+1)), nil, nil); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("oversized key error = %v, want ErrInvalidKey", err)
	}
	meta := &PutOptions{Attributes: map[string]string{"category": string(bytes.Repeat([]byte("m"), maxAttributesSize))}}
	if err := store.Put("key", nil, meta); !errors.Is(err, ErrAttributesTooLarge) {
		t.Fatalf("oversized attributes error = %v, want ErrAttributesTooLarge", err)
	}
	if err := store.Put("key", nil, &PutOptions{Attributes: map[string]string{"": "value"}}); !errors.Is(err, ErrInvalidAttribute) {
		t.Fatalf("empty attribute name error = %v, want ErrInvalidAttribute", err)
	}
}

func TestOpenValidatesOptions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	if _, err := Open(path, Options{CompactionThreshold: 1.1}); err == nil {
		t.Fatal("Open accepted a compaction threshold above 1")
	}
	if _, err := Open(path, Options{IndexedFields: []string{""}}); !errors.Is(err, ErrInvalidAttribute) {
		t.Fatalf("empty indexed field error = %v, want ErrInvalidAttribute", err)
	}
}

func TestOpenExclusivelyLocksStorePath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	first := openTestStore(t, path)

	second, err := Open(path, Options{})
	if second != nil {
		_ = second.Close()
		t.Fatal("second Open unexpectedly returned a store")
	}
	if !errors.Is(err, ErrStoreLocked) {
		t.Fatalf("second Open error = %v, want ErrStoreLocked", err)
	}

	if err := first.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}
	reopened, err := Open(path, Options{})
	if err != nil {
		t.Fatalf("Open after lock release: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened store: %v", err)
	}
	if _, err := os.Stat(path + lockFileSuffix); err != nil {
		t.Fatalf("persistent lock file: %v", err)
	}
}

func TestStoreLockAcrossProcesses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	command := exec.Command(os.Args[0], "-test.run=^TestStoreLockHelper$")
	command.Env = append(os.Environ(), "APPENDSTORE_LOCK_HELPER_PATH="+path)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatalf("create helper stdin: %v", err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatalf("create helper stdout: %v", err)
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatalf("start lock helper: %v", err)
	}
	helperDone := false
	t.Cleanup(func() {
		_ = stdin.Close()
		if !helperDone {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})

	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || line != "locked\n" {
		t.Fatalf("wait for helper lock: line=%q err=%v", line, err)
	}
	store, err := Open(path, Options{})
	if store != nil {
		_ = store.Close()
		t.Fatal("Open unexpectedly succeeded while helper held the lock")
	}
	if !errors.Is(err, ErrStoreLocked) {
		t.Fatalf("Open error = %v, want ErrStoreLocked", err)
	}

	if err := stdin.Close(); err != nil {
		t.Fatalf("signal lock helper: %v", err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("lock helper exited: %v", err)
	}
	helperDone = true
	reopened, err := Open(path, Options{})
	if err != nil {
		t.Fatalf("Open after helper exit: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened store: %v", err)
	}
}

func TestStoreLockHelper(t *testing.T) {
	path := os.Getenv("APPENDSTORE_LOCK_HELPER_PATH")
	if path == "" {
		t.Skip("subprocess helper")
	}
	store, err := Open(path, Options{})
	if err != nil {
		t.Fatalf("helper Open: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("helper Close: %v", err)
		}
	}()
	if _, err := fmt.Fprintln(os.Stdout, "locked"); err != nil {
		t.Fatalf("signal parent: %v", err)
	}
	_, _ = bufio.NewReader(os.Stdin).ReadByte()
}

func TestStoreConcurrentAccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	store := openTestStore(t, path, func(options *Options) {
		options.CacheSize = 32
	})

	const workers = 8
	const iterations = 250
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				key := fmt.Sprintf("key-%d", i%16)
				value := []byte(fmt.Sprintf("worker-%d-value-%d", worker, i))
				if err := store.Put(key, value, nil); err != nil {
					t.Errorf("put: %v", err)
					return
				}
				got, err := store.Get(key)
				if err != nil && !errors.Is(err, ErrKeyNotFound) {
					t.Errorf("get: %v", err)
					return
				}
				if err == nil && len(got) == 0 {
					t.Error("get returned an empty committed value")
					return
				}
				if i%25 == 0 {
					if err := store.Sync(); err != nil {
						t.Errorf("sync: %v", err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
}

func TestForEachMetadataCallbackCanMutateStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	store := openTestStore(t, path)
	if err := store.Put("first", []byte("value"), &PutOptions{Attributes: map[string]string{"category": "one"}}); err != nil {
		t.Fatalf("put first: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- store.ForEachMetadata(func(_ string, meta *Metadata) error {
			meta.Attributes["category"] = "mutated copy"
			return store.Put("second", []byte("value"), nil)
		})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("iterate metadata: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ForEachMetadata callback deadlocked while mutating the store")
	}
	meta, err := store.GetMetadata("first")
	if err != nil {
		t.Fatalf("get first metadata: %v", err)
	}
	if meta.Attribute("category") != "one" {
		t.Fatalf("callback mutated stored metadata: %q", meta.Attribute("category"))
	}
}

func TestSyncAndOperationsAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	store := openTestStore(t, path, func(options *Options) {
		options.SyncInterval = time.Hour
	})
	if err := store.Put("key", []byte("value"), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := store.Sync(); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("sync after close error = %v", err)
	}
	if err := store.Put("other", nil, nil); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("put after close error = %v", err)
	}
}
