package appendstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// setHeaderFeatures rewrites the feature masks of a closed log.
func setHeaderFeatures(t *testing.T, path string, incompatible, readOnly uint32) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open log header: %v", err)
	}
	defer file.Close()
	masks := make([]byte, 8)
	binary.LittleEndian.PutUint32(masks[:4], incompatible)
	binary.LittleEndian.PutUint32(masks[4:], readOnly)
	if _, err := file.WriteAt(masks, 8); err != nil {
		t.Fatalf("write feature masks: %v", err)
	}
}

// appendExtensionEntry encodes one extension entry.
func appendExtensionEntry(buf []byte, tag uint16, payload string) []byte {
	buf = binary.LittleEndian.AppendUint16(buf, tag)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(payload)))
	return append(buf, payload...)
}

// An incompatible bit means this build would read the log wrongly, so the only
// safe answer is to refuse and name the bit.
func TestOpenRefusesUnknownIncompatibleFeature(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	store := openTestStore(t, path)
	if err := store.Put("key", []byte("value"), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	setHeaderFeatures(t, path, 1<<3, 0)

	_, err := Open(path, Options{})
	if !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("open error = %v, want ErrUnsupportedFeature", err)
	}
	if !strings.Contains(err.Error(), "incompatible") || !strings.Contains(err.Error(), "bit 3") {
		t.Fatalf("error does not name the feature: %v", err)
	}
}

// A read-only bit means this build reads correctly but would corrupt the log if
// it wrote. Reads must work; every write must refuse and say why.
func TestUnknownReadOnlyFeatureServesReadsAndRefusesWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	store := openTestStore(t, path)
	if err := store.Put("key", []byte("value"), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Put("doomed", []byte("value"), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	setHeaderFeatures(t, path, 0, 1<<5)

	reopened, err := Open(path, Options{AutoCompact: true})
	if err != nil {
		t.Fatalf("open read-only log: %v", err)
	}
	defer reopened.Close()

	if !reopened.ReadOnly() {
		t.Fatal("ReadOnly() = false for a log with an unknown read-only feature")
	}
	value, err := reopened.Get("key")
	if err != nil || string(value) != "value" {
		t.Fatalf("get = %q, %v; want the stored value", value, err)
	}

	writes := []struct {
		name string
		err  error
	}{
		{"put", reopened.Put("key", []byte("new"), nil)},
		{"delete", reopened.Delete("doomed")},
		{"compact", reopened.Compact()},
	}
	for _, write := range writes {
		if !errors.Is(write.err, ErrReadOnly) {
			t.Errorf("%s error = %v, want ErrReadOnly", write.name, write.err)
			continue
		}
		if !strings.Contains(write.err.Error(), "read-only mask sets bit 5") {
			t.Errorf("%s error does not name the feature: %v", write.name, write.err)
		}
	}
	if !reopened.Exists("doomed") {
		t.Fatal("a refused delete removed the key anyway")
	}
}

// Discarding a torn trailing record is a repair, and a repair is a write. A log
// this build does not fully understand must keep every byte.
func TestReadOnlyLogKeepsIncompleteTrailingRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	store := openTestStore(t, path)
	if err := store.Put("key", []byte("value"), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open for append: %v", err)
	}
	if _, err := file.Write([]byte{40, 0, 0, 0, 1, 2}); err != nil {
		t.Fatalf("append torn record: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	setHeaderFeatures(t, path, 0, 1<<1)
	tornSize, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, Options{})
	if err != nil {
		t.Fatalf("open read-only log: %v", err)
	}
	defer reopened.Close()
	if value, err := reopened.Get("key"); err != nil || string(value) != "value" {
		t.Fatalf("get = %q, %v; want the record before the tear", value, err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != tornSize.Size() {
		t.Fatalf("read-only log was truncated from %d to %d bytes", tornSize.Size(), after.Size())
	}
}

// The point of the extension area: a writer adds an entry, and a build that
// does not know the tag still reads every field it does know.
func TestReadsRecordWithUnknownExtension(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	log, err := createAppendLog(path)
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	extensions := appendExtensionEntry(nil, 0xBEEF, "from a later build")
	extensions = appendExtensionEntry(extensions, 0x0001, "")
	if _, _, _, _, err := log.Append("key", []byte("value"), false, map[string]string{"category": "movies"}, extensions); err != nil {
		t.Fatalf("append record with extension: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close log: %v", err)
	}

	store := openTestStore(t, path)
	value, err := store.Get("key")
	if err != nil || string(value) != "value" {
		t.Fatalf("get = %q, %v; want the stored value", value, err)
	}
	meta, err := store.GetMetadata("key")
	if err != nil || meta.Attribute("category") != "movies" {
		t.Fatalf("metadata = %#v, %v", meta, err)
	}
	if got := store.KeysBy("category", "movies"); len(got) != 1 {
		t.Fatalf("secondary index = %v", got)
	}
}

// Dropping an unknown extension during compaction loses data silently: the
// build that wrote the entry still needs it after a downgrade and upgrade.
func TestCompactionPreservesUnknownExtension(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	log, err := createAppendLog(path)
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	extensions := appendExtensionEntry(nil, 0xBEEF, "must survive")
	if _, _, _, _, err := log.Append("kept", []byte("value"), false, nil, extensions); err != nil {
		t.Fatalf("append record with extension: %v", err)
	}
	// A second record makes compaction reclaim something, so the rewrite is not
	// trivially a copy of one record.
	if _, _, _, _, err := log.Append("dropped", []byte("dead"), false, nil, nil); err != nil {
		t.Fatalf("append second record: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close log: %v", err)
	}

	store := openTestStore(t, path)
	if err := store.Delete("dropped"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := store.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	value, err := store.Get("kept")
	if err != nil || string(value) != "value" {
		t.Fatalf("get after compaction = %q, %v", value, err)
	}

	entry := store.index.Get("kept")
	_, got, err := store.log.ReadRecordAndExtensionsAt(entry.RecordOffset, entry.StoredSize)
	if err != nil {
		t.Fatalf("read compacted record: %v", err)
	}
	if !bytes.Equal(got, extensions) {
		t.Fatalf("extensions after compaction = %x, want %x", got, extensions)
	}
}

// A record whose extension area is not framed correctly is corruption, not an
// entry to step over.
func TestRejectsMalformedExtensionArea(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	log, err := createAppendLog(path)
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	// An entry header that claims more payload than the area holds.
	truncated := appendExtensionEntry(nil, 1, "payload")[:8]
	if _, _, _, _, err := log.Append("key", []byte("value"), false, nil, truncated); err == nil {
		t.Fatal("append accepted a truncated extension entry")
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close log: %v", err)
	}
}

// Version 4 records are already valid version 5 records, so the migration is a
// header rewrite. Anything more would rewrite a log that needs no rewriting.
func TestV4LogMigratesByHeaderOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.db")
	source := openTestStore(t, filepath.Join(dir, "source.db"))
	if err := source.Put("key", []byte("value"), &PutOptions{Attributes: map[string]string{"category": "movies"}}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := source.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}
	if err := Downgrade(filepath.Join(dir, "source.db"), path, DowngradeOptions{Version: 4}); err != nil {
		t.Fatalf("write v4 log: %v", err)
	}
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read v4 log: %v", err)
	}
	if got := binary.LittleEndian.Uint32(original[4:8]); got != 4 {
		t.Fatalf("prepared log version = %d, want 4", got)
	}

	store := openTestStore(t, path)
	if store.log.version != logVersion {
		t.Fatalf("migrated version = %d, want %d", store.log.version, logVersion)
	}
	value, err := store.Get("key")
	if err != nil || string(value) != "value" {
		t.Fatalf("get after migration = %q, %v", value, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	migrated, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migrated log: %v", err)
	}
	if !bytes.Equal(migrated[logHeaderSize:], original[logHeaderSize:]) {
		t.Fatal("migration rewrote the record bytes of a v4 log")
	}
	if got := binary.LittleEndian.Uint32(migrated[4:8]); got != logVersion {
		t.Fatalf("migrated header version = %d, want %d", got, logVersion)
	}
	// Migrating away from v4 is still one-way, so the copy must be there.
	backup, err := os.ReadFile(path + ".v4" + migrationBackupSuffix)
	if err != nil {
		t.Fatalf("no pre-migration copy: %v", err)
	}
	if !bytes.Equal(backup, original) {
		t.Fatal("backup does not match the pre-migration log")
	}
}
