package appendstore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"slices"
	"strconv"
)

// downgradeTempSuffix names the partial output. Downgrade renames it into place
// only after every record is written, so a run that fails leaves no file at the
// destination for a caller to mistake for a complete log.
const downgradeTempSuffix = ".downgrade.tmp"

// v3Fields lists the attributes a version 3 record has a field for. Version 3
// stores named application fields rather than a generic attribute map, so a
// record holding any other attribute cannot be expressed in that format.
var v3Fields = []string{
	legacyCategoryAttribute,
	legacyProviderAttribute,
	legacyStatusAttribute,
	legacyNameAttribute,
	legacyTotalSizeAttribute,
	legacyProtocolAttribute,
	legacyBadAttribute,
	legacyAddedOnAttribute,
}

// v3StringFields lists the version 3 fields that hold text. Each is framed with
// a 16-bit length, so a longer value cannot be written.
var v3StringFields = []string{
	legacyCategoryAttribute,
	legacyProviderAttribute,
	legacyStatusAttribute,
	legacyNameAttribute,
	legacyProtocolAttribute,
}

// DowngradeOptions configures Downgrade.
type DowngradeOptions struct {
	// Version is the target log format: 3 or 4.
	Version uint32

	// Overwrite replaces a destination that already exists. Without it,
	// Downgrade refuses rather than replace a file it did not create. The
	// replacement is still atomic: the output is renamed into place only after
	// every record is written, so a failed run leaves the old file untouched.
	Overwrite bool
}

// Downgrade writes the live contents of the log at src to a new log at dst in
// format version 3 or version 4. It is the way back for a caller that upgraded
// to version 5 and must run an older build again.
//
// Downgrade fails, and leaves nothing at dst, when a record holds data the
// target format cannot express: an extension entry, an attribute version 3 has
// no field for, or a value too long for a version 3 field.
//
// Downgrade reads src. It writes to src only to discard an incomplete trailing
// record from an interrupted write, which is the same repair Open performs.
//
// This function exists for the transition described in docs/stable-format.md.
// It is removed together with the migration path when support for versions 1 to
// 4 ends.
func Downgrade(src, dst string, options DowngradeOptions) error {
	version := options.Version
	if version != 3 && version != 4 {
		return fmt.Errorf("unsupported downgrade target version %d (want 3 or 4)", version)
	}
	if src == dst {
		return fmt.Errorf("downgrade destination must differ from the source")
	}
	// A destination that already exists is somebody's data. Downgrade is run to
	// recover a store, often against the wrong path on the first try, so it
	// refuses by default rather than replace a file it did not create.
	if !options.Overwrite {
		if _, err := os.Stat(dst); err == nil {
			return fmt.Errorf("downgrade destination %s already exists", dst)
		} else if !os.IsNotExist(err) {
			return err
		}
	}

	lock, err := acquireFileLock(src)
	if err != nil {
		return err
	}
	defer lock.release()

	log, err := openAppendLog(src)
	if err != nil {
		return err
	}
	defer log.Close()

	if log.version < 4 {
		return fmt.Errorf("log at %s is already version %d", src, log.version)
	}
	// A feature bit marks a capability the format gained after version 4. No
	// older format can express it, whether or not this build knows the bit.
	if features := log.incompatibleFeatures | log.readOnlyFeatures; features != 0 {
		// Refused before the scan, because Iterate must not repair a log whose
		// features this build does not know.
		return fmt.Errorf("log sets feature bits %#08x that version %d cannot express", features, version)
	}

	live := newIndex(nil)
	err = log.Iterate(func(record *logRecord) error {
		if record.Deleted {
			live.Delete(record.Key)
			return nil
		}
		live.Put(record.Key, &indexEntry{
			Offset:       record.Offset,
			Size:         record.Size,
			RecordOffset: record.RecordOffset,
			StoredSize:   record.StoredSize,
			Attributes:   record.Attributes,
		})
		return nil
	})
	if err != nil {
		return err
	}

	tmp := dst + downgradeTempSuffix
	if err := removeIfExists(tmp); err != nil {
		return err
	}
	if version == 4 {
		err = writeV4Log(log, live, tmp)
	} else {
		err = writeV3Log(log, live, tmp)
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return syncParentDirectory(dst)
}

// writeV4Log writes the live set in version 4 format. A version 5 record with
// an empty extension area is byte-identical to a version 4 record, so the
// current writer produces the body and only the header version differs. Reusing
// the writer is what keeps the two encodings from drifting apart.
func writeV4Log(log *appendLog, live *index, path string) error {
	out, err := createAppendLogMode(path, 0644)
	if err != nil {
		return err
	}
	err = forEachLiveRecord(log, live, func(key string, value, extensions []byte, attributes map[string]string) error {
		if len(extensions) > 0 {
			return fmt.Errorf("key %q carries an extension that version 4 cannot express", key)
		}
		_, _, _, _, appendErr := out.Append(key, value, false, attributes, nil)
		return appendErr
	})
	if err == nil {
		// setVersion syncs, so the records are durable before the header claims
		// a format an older build will act on.
		err = out.setVersion(4)
	}
	if err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

// writeV3Log writes the live set in version 3 format. That format predates the
// generic attribute map and carries no checksum, so it is encoded here rather
// than by the current writer.
func writeV3Log(log *appendLog, live *index, path string) error {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	writer := bufio.NewWriterSize(file, 1<<20)
	header := make([]byte, logHeaderSize)
	copy(header[:4], logMagic)
	binary.LittleEndian.PutUint32(header[4:8], 3)
	_, _ = writer.Write(header)

	// bufio.Writer keeps the first write error and returns it from Flush, so
	// only the encoding errors need checking inside the loop.
	err = forEachLiveRecord(log, live, func(key string, value, extensions []byte, attributes map[string]string) error {
		record, encodeErr := encodeV3Record(key, value, extensions, attributes)
		if encodeErr != nil {
			return encodeErr
		}
		_, _ = writer.Write(record)
		return nil
	})
	if err == nil {
		err = writer.Flush()
	}
	if err == nil {
		err = file.Sync()
	}
	if err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func forEachLiveRecord(log *appendLog, live *index, fn func(key string, value, extensions []byte, attributes map[string]string) error) error {
	for _, key := range live.KeysSortedByOffset() {
		entry := live.Get(key)
		if entry == nil {
			continue
		}
		value, extensions, err := log.ReadRecordAndExtensionsAt(entry.RecordOffset, entry.StoredSize)
		if err != nil {
			return fmt.Errorf("read key %q: %w", key, err)
		}
		if err := fn(key, value, extensions, entry.Attributes); err != nil {
			return err
		}
	}
	return nil
}

// encodeV3Record returns one version 3 record, or reports why the record cannot
// be expressed in that format.
func encodeV3Record(key string, value, extensions []byte, attributes map[string]string) ([]byte, error) {
	if len(extensions) > 0 {
		return nil, fmt.Errorf("key %q carries an extension that version 3 cannot express", key)
	}
	for name := range attributes {
		if !slices.Contains(v3Fields, name) {
			return nil, fmt.Errorf("key %q has attribute %q, which version 3 has no field for", key, name)
		}
	}
	for _, name := range v3StringFields {
		if len(attributes[name]) > 1<<16-1 {
			return nil, fmt.Errorf("key %q attribute %q is %d bytes, more than a version 3 field holds", key, name, len(attributes[name]))
		}
	}
	totalSize, err := v3Number(key, attributes, legacyTotalSizeAttribute)
	if err != nil {
		return nil, err
	}
	addedOn, err := v3Number(key, attributes, legacyAddedOnAttribute)
	if err != nil {
		return nil, err
	}
	var flags byte
	if raw := attributes[legacyBadAttribute]; raw != "" {
		bad, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("key %q attribute %q is not a version 3 flag: %w", key, legacyBadAttribute, err)
		}
		if bad {
			flags |= 2
		}
	}

	appendField := func(buf []byte, text string) []byte {
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(text)))
		return append(buf, text...)
	}
	buf := make([]byte, 0, 64+len(key)+len(value))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(key)))
	buf = append(buf, key...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(value)))
	buf = append(buf, value...)
	buf = append(buf, flags)
	buf = appendField(buf, attributes[legacyCategoryAttribute])
	buf = appendField(buf, attributes[legacyProviderAttribute])
	buf = appendField(buf, attributes[legacyStatusAttribute])
	buf = appendField(buf, attributes[legacyNameAttribute])
	buf = binary.LittleEndian.AppendUint64(buf, uint64(totalSize))
	buf = appendField(buf, attributes[legacyProtocolAttribute])
	buf = binary.LittleEndian.AppendUint64(buf, uint64(addedOn))
	return buf, nil
}

func v3Number(key string, attributes map[string]string, name string) (int64, error) {
	raw := attributes[name]
	if raw == "" {
		return 0, nil
	}
	number, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("key %q attribute %q is not a version 3 number: %w", key, name, err)
	}
	return number, nil
}
