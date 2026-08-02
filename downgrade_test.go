package appendstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeDowngradeSource fills a store, closes it, and returns its path. Downgrade
// locks the source, so the store must not stay open.
func writeDowngradeSource(t *testing.T, dir string, attributes map[string]string) string {
	t.Helper()
	path := filepath.Join(dir, "source.db")
	store, err := Open(path, Options{SyncInterval: -1})
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	if err := store.Put("kept", []byte("kept value"), &PutOptions{Attributes: attributes}); err != nil {
		t.Fatalf("put kept: %v", err)
	}
	if err := store.Put("removed", []byte("dead value"), nil); err != nil {
		t.Fatalf("put removed: %v", err)
	}
	if err := store.Delete("removed"); err != nil {
		t.Fatalf("delete removed: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}
	return path
}

func TestDowngradeToV4RoundTrips(t *testing.T) {
	dir := t.TempDir()
	attributes := map[string]string{"category": "movies", "anything": "a v4 attribute map holds any key"}
	source := writeDowngradeSource(t, dir, attributes)
	destination := filepath.Join(dir, "v4.db")

	if err := Downgrade(source, destination, DowngradeOptions{Version: 4}); err != nil {
		t.Fatalf("downgrade: %v", err)
	}
	if got := logVersionOnDisk(t, destination); got != 4 {
		t.Fatalf("downgraded log version = %d, want 4", got)
	}
	if got := logVersionOnDisk(t, source); got != logVersion {
		t.Fatalf("downgrade changed the source to v%d", got)
	}

	// Reopening migrates the log back, which is the round trip a caller makes
	// after running the older build.
	restored := openTestStore(t, destination)
	value, err := restored.Get("kept")
	if err != nil || string(value) != "kept value" {
		t.Fatalf("get after round trip = %q, %v", value, err)
	}
	meta, err := restored.GetMetadata("kept")
	if err != nil {
		t.Fatalf("metadata after round trip: %v", err)
	}
	for name, want := range attributes {
		if got := meta.Attribute(name); got != want {
			t.Errorf("attribute %q = %q, want %q", name, got, want)
		}
	}
	if restored.Exists("removed") {
		t.Fatal("a deleted key came back through the downgrade")
	}
}

func TestDowngradeToV3RoundTrips(t *testing.T) {
	dir := t.TempDir()
	attributes := map[string]string{
		"category":   "movies",
		"provider":   "provider-a",
		"status":     "ready",
		"name":       "Example",
		"total_size": "1234",
		"protocol":   "torrent",
		"bad":        "true",
		"added_on":   "456",
	}
	source := writeDowngradeSource(t, dir, attributes)
	destination := filepath.Join(dir, "v3.db")

	if err := Downgrade(source, destination, DowngradeOptions{Version: 3}); err != nil {
		t.Fatalf("downgrade: %v", err)
	}
	if got := logVersionOnDisk(t, destination); got != 3 {
		t.Fatalf("downgraded log version = %d, want 3", got)
	}

	restored := openTestStore(t, destination)
	value, err := restored.Get("kept")
	if err != nil || string(value) != "kept value" {
		t.Fatalf("get after round trip = %q, %v", value, err)
	}
	meta, err := restored.GetMetadata("kept")
	if err != nil {
		t.Fatalf("metadata after round trip: %v", err)
	}
	for name, want := range attributes {
		if got := meta.Attribute(name); got != want {
			t.Errorf("attribute %q = %q, want %q", name, got, want)
		}
	}
	if restored.Exists("removed") {
		t.Fatal("a deleted key came back through the downgrade")
	}
}

// Version 3 has a field per named attribute, so anything else must stop the run
// rather than be dropped from the output.
func TestDowngradeToV3RefusesUnexpressibleRecords(t *testing.T) {
	cases := []struct {
		name       string
		attributes map[string]string
		want       string
	}{
		{"unknown attribute", map[string]string{"colour": "red"}, `attribute "colour"`},
		{"non-numeric total_size", map[string]string{"total_size": "many"}, "not a version 3 number"},
		{"non-boolean bad", map[string]string{"bad": "maybe"}, "not a version 3 flag"},
		{"oversized field", map[string]string{"name": strings.Repeat("x", 1<<16)}, "more than a version 3 field holds"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			source := writeDowngradeSource(t, dir, test.attributes)
			destination := filepath.Join(dir, "v3.db")

			err := Downgrade(source, destination, DowngradeOptions{Version: 3})
			if err == nil {
				t.Fatal("downgrade accepted a record version 3 cannot express")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want it to mention %q", err, test.want)
			}
			if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("a partial log was left behind: %v", err)
			}
		})
	}
}

// An extension carries data no older format has a place for.
func TestDowngradeRefusesExtensions(t *testing.T) {
	for _, version := range []uint32{3, 4} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			dir := t.TempDir()
			source := filepath.Join(dir, "source.db")
			log, err := createAppendLog(source)
			if err != nil {
				t.Fatalf("create source: %v", err)
			}
			extensions := appendExtensionEntry(nil, 0xBEEF, "from a later build")
			if _, _, _, _, err := log.Append("key", []byte("value"), false, nil, extensions); err != nil {
				t.Fatalf("append: %v", err)
			}
			if err := log.Close(); err != nil {
				t.Fatalf("close source: %v", err)
			}

			destination := filepath.Join(dir, "old.db")
			err = Downgrade(source, destination, DowngradeOptions{Version: version})
			if err == nil {
				t.Fatal("downgrade dropped an extension instead of refusing")
			}
			if !strings.Contains(err.Error(), "extension") {
				t.Fatalf("error = %v, want it to mention the extension", err)
			}
			if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("a partial log was left behind: %v", err)
			}
		})
	}
}

// A feature bit marks a capability that arrived after version 4, so no older
// format can hold it whether or not this build knows the bit.
func TestDowngradeRefusesFeatureBits(t *testing.T) {
	dir := t.TempDir()
	source := writeDowngradeSource(t, dir, nil)
	setHeaderFeatures(t, source, 0, 1<<2)
	destination := filepath.Join(dir, "v4.db")

	err := Downgrade(source, destination, DowngradeOptions{Version: 4})
	if err == nil {
		t.Fatal("downgrade ignored a feature bit")
	}
	if !strings.Contains(err.Error(), "feature bits") {
		t.Fatalf("error = %v, want it to mention the feature bits", err)
	}
}

func TestDowngradeRejectsBadArguments(t *testing.T) {
	dir := t.TempDir()
	source := writeDowngradeSource(t, dir, nil)

	if err := Downgrade(source, filepath.Join(dir, "out.db"), DowngradeOptions{Version: 5}); err == nil {
		t.Error("downgrade accepted version 5 as a target")
	}
	if err := Downgrade(source, source, DowngradeOptions{Version: 4}); err == nil {
		t.Error("downgrade accepted the source as its own destination")
	}

	legacy := filepath.Join(dir, "legacy.db")
	header := make([]byte, logHeaderSize)
	copy(header[:4], logMagic)
	binary.LittleEndian.PutUint32(header[4:8], 3)
	if err := os.WriteFile(legacy, header, 0600); err != nil {
		t.Fatal(err)
	}
	if err := Downgrade(legacy, filepath.Join(dir, "out.db"), DowngradeOptions{Version: 4}); err == nil {
		t.Error("downgrade accepted a log that is already older than the target")
	}
}

// Downgrade recovers a store, so it is run against the wrong path sooner or
// later. An existing destination is somebody's data and must survive.
func TestDowngradeRefusesExistingDestination(t *testing.T) {
	dir := t.TempDir()
	source := writeDowngradeSource(t, dir, nil)
	destination := filepath.Join(dir, "out.db")
	if err := os.WriteFile(destination, []byte("somebody else's data"), 0600); err != nil {
		t.Fatal(err)
	}

	err := Downgrade(source, destination, DowngradeOptions{Version: 4})
	if err == nil {
		t.Fatal("downgrade replaced an existing destination")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("error = %v, want it to name the existing destination", err)
	}
	kept, readErr := os.ReadFile(destination)
	if readErr != nil || string(kept) != "somebody else's data" {
		t.Fatalf("destination = %q, %v", kept, readErr)
	}
	// The refusal must not leave the partial output behind either.
	if _, err := os.Stat(destination + downgradeTempSuffix); !os.IsNotExist(err) {
		t.Fatalf("partial output was left behind: %v", err)
	}
}

func TestDowngradeOverwriteReplacesDestination(t *testing.T) {
	dir := t.TempDir()
	source := writeDowngradeSource(t, dir, nil)
	destination := filepath.Join(dir, "out.db")
	if err := os.WriteFile(destination, []byte("stale"), 0600); err != nil {
		t.Fatal(err)
	}

	if err := Downgrade(source, destination, DowngradeOptions{Version: 4, Overwrite: true}); err != nil {
		t.Fatalf("downgrade with overwrite: %v", err)
	}
	if got := logVersionOnDisk(t, destination); got != 4 {
		t.Fatalf("destination version = %d, want 4", got)
	}
	restored := openTestStore(t, destination)
	if value, err := restored.Get("kept"); err != nil || string(value) != "kept value" {
		t.Fatalf("get after overwrite = %q, %v", value, err)
	}
}

// A run that fails after the destination check must leave the old file intact,
// because the output is renamed into place only once every record is written.
func TestDowngradeOverwriteKeepsDestinationOnFailure(t *testing.T) {
	dir := t.TempDir()
	source := writeDowngradeSource(t, dir, map[string]string{"colour": "red"})
	destination := filepath.Join(dir, "out.db")
	if err := os.WriteFile(destination, []byte("previous good copy"), 0600); err != nil {
		t.Fatal(err)
	}

	// Version 3 has no field for "colour", so the run fails mid-write.
	if err := Downgrade(source, destination, DowngradeOptions{Version: 3, Overwrite: true}); err == nil {
		t.Fatal("downgrade accepted an attribute version 3 cannot express")
	}
	kept, err := os.ReadFile(destination)
	if err != nil || string(kept) != "previous good copy" {
		t.Fatalf("destination = %q, %v", kept, err)
	}
	if _, err := os.Stat(destination + downgradeTempSuffix); !os.IsNotExist(err) {
		t.Fatalf("partial output was left behind: %v", err)
	}
}
