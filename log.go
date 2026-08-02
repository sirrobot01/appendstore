package appendstore

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	logMagic      = "HYBR"
	logVersion    = uint32(5)
	logHeaderSize = 16

	// knownIncompatibleFeatures and knownReadOnlyFeatures are the feature bits
	// this build understands. Version 5 defines none. A later capability claims
	// a bit here instead of changing the version: a build that does not know an
	// incompatible bit refuses the log, and a build that does not know a
	// read-only bit reads the log but never writes it. See docs/stable-format.md.
	knownIncompatibleFeatures = uint32(0)
	knownReadOnlyFeatures     = uint32(0)

	maxKeySize        = 1 << 20
	maxValueSize      = int64(1<<31 - 1)
	maxAttributesSize = 16 << 20
	maxAttributeCount = 4096

	// maxExtensionsSize bounds the extension area of one record. The format
	// allows any size the record length can express; the bound keeps a corrupt
	// length from turning into a multi-gigabyte allocation during a scan.
	maxExtensionsSize = 16 << 20

	recordFixedBodySize = 16                      // flags/reserved + keyLen + attributesLen + valueLen
	recordMinimumLength = recordFixedBodySize + 4 // fixed body + checksum
	recordFieldsOffset  = 4 + recordFixedBodySize // first byte of the key

	extensionHeaderSize = 6 // tag + length

	// maxRetainedScratch caps reused encode/read buffers so one oversized
	// record does not pin memory for the life of the store.
	maxRetainedScratch = 1 << 20

	compactionTempSuffix   = ".compact.tmp"
	compactionReadySuffix  = ".compact.ready"
	compactionBackupSuffix = ".compact.backup"
	legacyCompactionSuffix = ".compact"

	// migrationBackupSuffix names the copy kept of a pre-migration log. Unlike
	// the compaction artifacts above it is never reclaimed: it is the only way
	// back to the format an older build can read.
	migrationBackupSuffix = ".bak"
	// migrationBackupTempSuffix is appended to the backup path, not the store
	// path: the copy is renamed into place so a crash mid-copy cannot leave a
	// truncated file that a later run trusts.
	migrationBackupTempSuffix = ".tmp"

	legacyCategoryAttribute  = "category"
	legacyProviderAttribute  = "provider"
	legacyStatusAttribute    = "status"
	legacyNameAttribute      = "name"
	legacyTotalSizeAttribute = "total_size"
	legacyProtocolAttribute  = "protocol"
	legacyBadAttribute       = "bad"
	legacyAddedOnAttribute   = "added_on"
)

// Header layout (all integers are little-endian):
//
//	byte[4] magic "HYBR"
//	uint32  version
//	uint32  incompatible feature mask
//	uint32  read-only feature mask
//
// Version 5 record layout:
//
//	uint32 record length (everything after this field)
//	byte   flags (bit 0 is a tombstone)
//	byte[3] reserved
//	uint32 key length
//	uint32 encoded attributes length
//	uint32 value length
//	byte[] key
//	byte[] encoded attributes
//	byte[] value
//	byte[] extensions (zero or more entries of uint16 tag, uint32 length, payload)
//	uint32 CRC32C of flags through the extensions
//
// The explicit length makes incomplete-tail recovery unambiguous. The checksum
// detects corruption in keys, attributes, values, and structural fields.
//
// The known fields must fit inside the record length rather than equal it. The
// remainder is the extension area, which a reader steps over when it does not
// know a tag. That tolerance is what lets a new writer add a field without
// breaking an old reader. A version 4 record is a version 5 record with an
// empty extension area, so the same reader serves both.
var checksumTable = crc32.MakeTable(crc32.Castagnoli)

// recordBounds locates the variable-length areas of a validated stored record.
type recordBounds struct {
	valueStart, valueLen           int64
	extensionsStart, extensionsLen int64
}

type logRecord struct {
	Key          string
	Offset       int64
	Size         int32
	RecordOffset int64
	StoredSize   int64
	Deleted      bool
	Attributes   map[string]string
}

type appendLog struct {
	mu       sync.Mutex
	file     *os.File
	path     string
	writePos int64

	// version is the on-disk log format. Logs older than logVersion cannot take
	// an append: Open migrates them before serving writes.
	version uint32

	// incompatibleFeatures and readOnlyFeatures are the masks from the header.
	// They are zero for a version 1 to 4 log, which has no masks.
	incompatibleFeatures uint32
	readOnlyFeatures     uint32

	// appendBuf is the reusable record encode buffer. Append is serialized by
	// mu, so no further synchronization is needed.
	appendBuf []byte

	// mapped is a read-only memory map of the log file, nil when mapping is
	// unavailable. Append remaps only while the caller holds the store write
	// lock, and readers access mapped under the store read lock, so the store
	// lock orders every remap against every mapped read.
	mapped      []byte
	mapDisabled bool
	mapSlack    int64
}

// defaultMapSlack is the address-space headroom mapped beyond the end of the
// file. Mapped pages become readable as appends grow the file underneath the
// mapping, so a fresh map is only needed once per defaultMapSlack bytes of
// growth. It is a variable so tests can shrink it to exercise remapping.
var defaultMapSlack = int64(64 << 20)

func openAppendLog(path string) (*appendLog, error) {
	if err := recoverCompactionArtifacts(path); err != nil {
		return nil, err
	}
	return openAppendLogFile(path)
}

// openAppendLogFile leaves compaction artifacts untouched. Compaction uses it
// to validate a replacement while the previous database is still recoverable.
func openAppendLogFile(path string) (*appendLog, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	log := &appendLog{file: file, path: path, mapSlack: defaultMapSlack, version: logVersion}
	if info.Size() == 0 {
		if err := log.writeHeader(); err != nil {
			_ = file.Close()
			return nil, err
		}
		log.writePos = logHeaderSize
	} else {
		if err := log.readHeader(); err != nil {
			_ = file.Close()
			return nil, err
		}
		log.writePos = info.Size()
	}
	log.remap()
	return log, nil
}

func recoverCompactionArtifacts(path string) error {
	tempPath := path + compactionTempSuffix
	readyPath := path + compactionReadySuffix
	backupPath := path + compactionBackupSuffix
	legacyPath := path + legacyCompactionSuffix

	mainExists, err := fileExists(path)
	if err != nil {
		return err
	}
	readyExists, err := fileExists(readyPath)
	if err != nil {
		return err
	}
	backupExists, err := fileExists(backupPath)
	if err != nil {
		return err
	}
	tempExists, err := fileExists(tempPath)
	if err != nil {
		return err
	}

	legacyExists, err := fileExists(legacyPath)
	if err != nil {
		return err
	}

	switch {
	case mainExists:
		if err := removeIfExists(readyPath); err != nil {
			return err
		}
		if err := removeIfExists(backupPath); err != nil {
			return err
		}
		if err := removeIfExists(legacyPath); err != nil {
			return err
		}
	case readyExists:
		if err := os.Rename(readyPath, path); err != nil {
			return fmt.Errorf("complete interrupted compaction: %w", err)
		}
		if err := removeIfExists(backupPath); err != nil {
			return err
		}
	case backupExists:
		if err := os.Rename(backupPath, path); err != nil {
			return fmt.Errorf("restore interrupted compaction: %w", err)
		}
	case legacyExists:
		// Versions 1-3 used a single .compact file and removed the main log
		// before renaming it. If the process died between those operations, the
		// synced compact file is the only surviving database.
		if err := os.Rename(legacyPath, path); err != nil {
			return fmt.Errorf("restore legacy interrupted compaction: %w", err)
		}
	}
	for _, artifact := range []string{tempPath, readyPath, backupPath, legacyPath} {
		if err := removeIfExists(artifact); err != nil {
			return err
		}
	}
	if readyExists || backupExists || tempExists || legacyExists {
		if err := syncParentDirectory(path); err != nil {
			return fmt.Errorf("sync directory after compaction recovery: %w", err)
		}
	}
	return nil
}

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, fmt.Errorf("stat %s: %w", filepath.Base(path), err)
}

func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove %s: %w", filepath.Base(path), err)
	}
	return nil
}

func createAppendLog(path string) (*appendLog, error) {
	return createAppendLogMode(path, 0644)
}

func createAppendLogMode(path string, mode os.FileMode) (*appendLog, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return nil, err
	}
	log := &appendLog{file: file, path: path, mapSlack: defaultMapSlack, version: logVersion}
	if err := log.writeHeader(); err != nil {
		_ = file.Close()
		return nil, err
	}
	log.writePos = logHeaderSize
	log.remap()
	return log, nil
}

func (l *appendLog) writeHeader() error {
	header := make([]byte, logHeaderSize)
	copy(header[:4], logMagic)
	binary.LittleEndian.PutUint32(header[4:8], l.version)
	binary.LittleEndian.PutUint32(header[8:12], l.incompatibleFeatures)
	binary.LittleEndian.PutUint32(header[12:16], l.readOnlyFeatures)
	_, err := l.file.WriteAt(header, 0)
	return err
}

// setVersion rewrites the header with a different format version. The record
// bytes of a version 4 log are already valid version 5 records, so migrating
// that log costs one header write instead of a full copy. Downgrade uses the
// same call in the other direction.
func (l *appendLog) setVersion(version uint32) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	previous := l.version
	l.version = version
	if err := l.writeHeader(); err != nil {
		l.version = previous
		return err
	}
	return l.file.Sync()
}

// readHeader validates the header and records what the log requires of the
// reader. It reports ErrUnsupportedFeature when the log needs a capability this
// build does not have; an unknown read-only bit is left for the caller, which
// opens the log but must refuse every write.
func (l *appendLog) readHeader() error {
	header := make([]byte, logHeaderSize)
	if _, err := l.file.ReadAt(header, 0); err != nil {
		return fmt.Errorf("%w: read header: %v", ErrCorruptedData, err)
	}
	if string(header[:4]) != logMagic {
		return fmt.Errorf("%w: invalid magic", ErrCorruptedData)
	}
	version := binary.LittleEndian.Uint32(header[4:8])
	if version == 0 {
		return fmt.Errorf("%w: invalid log version 0", ErrCorruptedData)
	}
	if version > logVersion {
		return fmt.Errorf("%w: %d (maximum %d)", ErrUnsupportedVersion, version, logVersion)
	}
	l.version = version
	// Versions 1 to 4 left bytes 8 to 16 zero, so only a version 5 header
	// carries feature masks. Reading them from an older header would give
	// meaning to bytes no writer ever set.
	if version >= 5 {
		l.incompatibleFeatures = binary.LittleEndian.Uint32(header[8:12])
		l.readOnlyFeatures = binary.LittleEndian.Uint32(header[12:16])
	}
	if unknown := l.incompatibleFeatures &^ knownIncompatibleFeatures; unknown != 0 {
		return unknownFeatureError("incompatible", unknown)
	}
	return nil
}

// unknownReadOnly returns the read-only feature bits this build does not know.
// A log that sets one is readable, but nothing here may write to it.
func (l *appendLog) unknownReadOnly() uint32 {
	return l.readOnlyFeatures &^ knownReadOnlyFeatures
}

// unknownFeatureError names every bit of a mask this build does not know, so a
// refusal points at the capability that caused it and not at a version number.
func unknownFeatureError(mask string, unknown uint32) error {
	bits := make([]string, 0, 32)
	for bit := range 32 {
		if unknown&(1<<bit) != 0 {
			bits = append(bits, strconv.Itoa(bit))
		}
	}
	return fmt.Errorf("%w: %s mask sets bit %s", ErrUnsupportedFeature, mask, strings.Join(bits, ", "))
}

// Append writes one record and returns the value and record locations. The
// extensions argument holds already-encoded extension entries, which only
// compaction supplies: it copies the area verbatim so an entry this build does
// not know survives the rewrite.
func (l *appendLog) Append(key string, value []byte, deleted bool, attributes map[string]string, extensions []byte) (offset int64, size int32, recordOffset int64, storedSize int64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.version != logVersion {
		return 0, 0, 0, 0, fmt.Errorf("cannot append a v%d record to a v%d log", logVersion, l.version)
	}
	if unknown := l.unknownReadOnly(); unknown != 0 {
		return 0, 0, 0, 0, unknownFeatureError("read-only", unknown)
	}
	keyBytes := []byte(key)
	if len(keyBytes) > maxKeySize {
		return 0, 0, 0, 0, fmt.Errorf("%w: key is %d bytes (maximum %d)", ErrInvalidKey, len(keyBytes), maxKeySize)
	}
	if int64(len(value)) > maxValueSize {
		return 0, 0, 0, 0, fmt.Errorf("%w: value is %d bytes (maximum %d)", ErrValueTooLarge, len(value), maxValueSize)
	}
	if len(extensions) > maxExtensionsSize {
		return 0, 0, 0, 0, fmt.Errorf("%w: extensions are %d bytes (maximum %d)", ErrValueTooLarge, len(extensions), maxExtensionsSize)
	}
	if err := validateExtensions(extensions); err != nil {
		return 0, 0, 0, 0, err
	}
	attributesBytes, err := encodeAttributes(attributes)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	recordLength := int64(recordMinimumLength) + int64(len(keyBytes)+len(attributesBytes)+len(value)+len(extensions))
	if recordLength > int64(^uint32(0)) {
		return 0, 0, 0, 0, fmt.Errorf("%w: encoded record is %d bytes", ErrValueTooLarge, recordLength)
	}
	totalSize := 4 + int(recordLength)
	buf := l.appendBuf
	if cap(buf) < totalSize {
		buf = make([]byte, totalSize)
		if totalSize <= maxRetainedScratch {
			l.appendBuf = buf
		}
	}
	buf = buf[:totalSize]
	binary.LittleEndian.PutUint32(buf[:4], uint32(recordLength))
	// The buffer is reused, so the flag and reserved bytes must be written
	// explicitly; every other byte is fully overwritten below.
	buf[4] = 0
	if deleted {
		buf[4] = 1
	}
	buf[5], buf[6], buf[7] = 0, 0, 0
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(keyBytes)))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(len(attributesBytes)))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(len(value)))
	pos := 20
	copy(buf[pos:], keyBytes)
	pos += len(keyBytes)
	copy(buf[pos:], attributesBytes)
	pos += len(attributesBytes)
	valueOffset := l.writePos + int64(pos)
	copy(buf[pos:], value)
	pos += len(value)
	copy(buf[pos:], extensions)
	pos += len(extensions)
	binary.LittleEndian.PutUint32(buf[pos:], crc32.Checksum(buf[4:pos], checksumTable))
	recordStart := l.writePos
	if _, err := l.file.WriteAt(buf, recordStart); err != nil {
		return 0, 0, 0, 0, err
	}
	l.writePos += int64(totalSize)
	if l.writePos > int64(len(l.mapped)) {
		l.remap()
	}
	return valueOffset, int32(len(value)), recordStart, int64(totalSize), nil
}

// remap re-establishes the read-only memory map so it covers writePos plus
// mapSlack bytes of headroom. Callers hold the store write lock (or have
// exclusive access during Open), which orders the swap against mapped reads.
// On any failure mapping is disabled and reads fall back to file reads.
func (l *appendLog) remap() {
	if l.mapDisabled {
		return
	}
	if l.mapped != nil {
		_ = munmapFile(l.mapped)
		l.mapped = nil
	}
	length := l.writePos + l.mapSlack
	if length <= 0 || length > int64(int(^uint(0)>>1)) {
		l.mapDisabled = true
		return
	}
	mapped, err := mmapFile(l.file, length)
	if err != nil {
		l.mapDisabled = true
		return
	}
	l.mapped = mapped
}

// mappedRecord returns the stored record bytes from the memory map when the
// map covers them. The caller must hold the store lock; see the mapped field
// comment for the synchronization contract.
func (l *appendLog) mappedRecord(recordOffset, storedSize int64) ([]byte, bool) {
	end := recordOffset + storedSize
	if l.mapped == nil || recordOffset < logHeaderSize || end > l.writePos || end > int64(len(l.mapped)) {
		return nil, false
	}
	return l.mapped[recordOffset:end], true
}

// emptyAttributes is the encoding of zero attributes. It is shared and must
// only be copied from, never written to.
var emptyAttributes = make([]byte, 4)

func encodeAttributes(attributes map[string]string) ([]byte, error) {
	if len(attributes) == 0 {
		return emptyAttributes, nil
	}
	if len(attributes) > maxAttributeCount {
		return nil, fmt.Errorf("%w: %d attributes (maximum %d)", ErrAttributesTooLarge, len(attributes), maxAttributeCount)
	}
	keys := make([]string, 0, len(attributes))
	total := 4
	for key, value := range attributes {
		if key == "" {
			return nil, ErrInvalidAttribute
		}
		if len(key) > 1<<16-1 {
			return nil, fmt.Errorf("%w: attribute name is %d bytes", ErrAttributesTooLarge, len(key))
		}
		if uint64(len(value)) > uint64(^uint32(0)) {
			return nil, fmt.Errorf("%w: attribute %q value is too large", ErrAttributesTooLarge, key)
		}
		total += 2 + 4 + len(key) + len(value)
		if total > maxAttributesSize {
			return nil, fmt.Errorf("%w: encoded attributes exceed %d bytes", ErrAttributesTooLarge, maxAttributesSize)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	buf := make([]byte, total)
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(keys)))
	pos := 4
	for _, key := range keys {
		value := attributes[key]
		binary.LittleEndian.PutUint16(buf[pos:], uint16(len(key)))
		pos += 2
		binary.LittleEndian.PutUint32(buf[pos:], uint32(len(value)))
		pos += 4
		copy(buf[pos:], key)
		pos += len(key)
		copy(buf[pos:], value)
		pos += len(value)
	}
	return buf, nil
}

func decodeAttributes(buf []byte) (map[string]string, error) {
	if len(buf) < 4 {
		return nil, fmt.Errorf("attribute block is too short")
	}
	count := binary.LittleEndian.Uint32(buf[:4])
	if count > maxAttributeCount {
		return nil, fmt.Errorf("attribute count %d exceeds maximum", count)
	}
	attributes := make(map[string]string, int(count))
	pos := 4
	for range count {
		if len(buf)-pos < 6 {
			return nil, io.ErrUnexpectedEOF
		}
		keyLen := int(binary.LittleEndian.Uint16(buf[pos:]))
		pos += 2
		valueLen := int(binary.LittleEndian.Uint32(buf[pos:]))
		pos += 4
		if keyLen == 0 || keyLen > len(buf)-pos {
			return nil, fmt.Errorf("invalid attribute name length %d", keyLen)
		}
		key := string(buf[pos : pos+keyLen])
		pos += keyLen
		if valueLen > len(buf)-pos {
			return nil, io.ErrUnexpectedEOF
		}
		if _, duplicate := attributes[key]; duplicate {
			return nil, fmt.Errorf("duplicate attribute %q", key)
		}
		attributes[key] = string(buf[pos : pos+valueLen])
		pos += valueLen
	}
	if pos != len(buf) {
		return nil, fmt.Errorf("attribute block has %d trailing bytes", len(buf)-pos)
	}
	if len(attributes) == 0 {
		return nil, nil
	}
	return attributes, nil
}

// recordScratchPool holds reusable buffers for file-based record reads.
var recordScratchPool = sync.Pool{New: func() any { return new([]byte) }}

func checkStoredSize(storedSize int64) error {
	if storedSize < 4+recordMinimumLength || storedSize > int64(int(^uint(0)>>1)) {
		return fmt.Errorf("%w: invalid stored record size %d", ErrCorruptedData, storedSize)
	}
	return nil
}

// validateExtensions walks the extension area and checks that every entry is
// framed correctly. It does not interpret a tag: an entry this build does not
// know is stepped over here and copied unchanged by compaction.
func validateExtensions(extensions []byte) error {
	for pos := 0; pos < len(extensions); {
		if len(extensions)-pos < extensionHeaderSize {
			return fmt.Errorf("extension entry at %d is truncated", pos)
		}
		length := int64(binary.LittleEndian.Uint32(extensions[pos+2:]))
		pos += extensionHeaderSize
		if length > int64(len(extensions)-pos) {
			return fmt.Errorf("extension entry at %d claims %d bytes", pos-extensionHeaderSize, length)
		}
		pos += int(length)
	}
	return nil
}

// validateStoredRecord checks the framing, checksum, and field lengths of one
// complete stored record and returns the bounds of its variable-length areas.
// The known fields must fit inside the record length; whatever follows them is
// the extension area.
func validateStoredRecord(record []byte) (recordBounds, error) {
	var bounds recordBounds
	recordLength := int64(binary.LittleEndian.Uint32(record[:4]))
	if recordLength+4 != int64(len(record)) {
		return bounds, fmt.Errorf("%w: record length mismatch", ErrCorruptedData)
	}
	wantChecksum := binary.LittleEndian.Uint32(record[len(record)-4:])
	gotChecksum := crc32.Checksum(record[4:len(record)-4], checksumTable)
	if gotChecksum != wantChecksum {
		return bounds, fmt.Errorf("%w: checksum mismatch", ErrCorruptedData)
	}
	keyLen := int64(binary.LittleEndian.Uint32(record[8:12]))
	attributesLen := int64(binary.LittleEndian.Uint32(record[12:16]))
	valueLen := int64(binary.LittleEndian.Uint32(record[16:20]))
	known := int64(recordMinimumLength) + keyLen + attributesLen + valueLen
	if known > recordLength {
		return bounds, fmt.Errorf("%w: invalid record field lengths", ErrCorruptedData)
	}
	bounds.valueStart = recordFieldsOffset + keyLen + attributesLen
	bounds.valueLen = valueLen
	bounds.extensionsStart = bounds.valueStart + valueLen
	bounds.extensionsLen = recordLength - known
	return bounds, nil
}

// ReadAt reads a raw value by its offset and size. It is only used to read
// values out of legacy (pre-v4) logs during migration; those records carry no
// checksum, so no validation is possible.
func (l *appendLog) ReadAt(offset int64, size int32) ([]byte, error) {
	buf := make([]byte, size)
	if _, err := l.file.ReadAt(buf, offset); err != nil {
		return nil, err
	}
	return buf, nil
}

// ReadRecordAt validates the stored record and returns a copy of its value
// that the caller owns.
func (l *appendLog) ReadRecordAt(recordOffset, storedSize int64) ([]byte, error) {
	if err := checkStoredSize(storedSize); err != nil {
		return nil, err
	}
	if record, ok := l.mappedRecord(recordOffset, storedSize); ok {
		bounds, err := validateStoredRecord(record)
		if err != nil {
			return nil, err
		}
		return bytes.Clone(record[bounds.valueStart : bounds.valueStart+bounds.valueLen]), nil
	}
	scratchPtr := recordScratchPool.Get().(*[]byte)
	value, scratch, err := l.readRecordFromFile(recordOffset, storedSize, *scratchPtr)
	var owned []byte
	if err == nil {
		owned = bytes.Clone(value)
	}
	if cap(scratch) <= maxRetainedScratch {
		*scratchPtr = scratch
		recordScratchPool.Put(scratchPtr)
	}
	return owned, err
}

// ReadRecordAtInto reads a record value using scratch as reusable backing.
// The returned value is only valid until scratch is next reused.
func (l *appendLog) ReadRecordAtInto(recordOffset, storedSize int64, scratch []byte) ([]byte, []byte, error) {
	if err := checkStoredSize(storedSize); err != nil {
		return nil, scratch, err
	}
	if record, ok := l.mappedRecord(recordOffset, storedSize); ok {
		bounds, err := validateStoredRecord(record)
		if err != nil {
			return nil, scratch, err
		}
		// Copy out of the mapping: callers may scribble on the returned value,
		// and the read-only mapping must never be written.
		scratch = append(scratch[:0], record[bounds.valueStart:bounds.valueStart+bounds.valueLen]...)
		return scratch, scratch, nil
	}
	return l.readRecordFromFile(recordOffset, storedSize, scratch)
}

// ReadRecordAndExtensionsAt returns copies of the value and the raw extension
// area of one stored record. Compaction uses it: an extension entry this build
// does not know must survive the copy, or a downgrade and a later upgrade lose
// the data it carries.
func (l *appendLog) ReadRecordAndExtensionsAt(recordOffset, storedSize int64) ([]byte, []byte, error) {
	if err := checkStoredSize(storedSize); err != nil {
		return nil, nil, err
	}
	record, ok := l.mappedRecord(recordOffset, storedSize)
	if !ok {
		record = make([]byte, storedSize)
		if _, err := l.file.ReadAt(record, recordOffset); err != nil {
			return nil, nil, err
		}
	}
	bounds, err := validateStoredRecord(record)
	if err != nil {
		return nil, nil, err
	}
	value := bytes.Clone(record[bounds.valueStart : bounds.valueStart+bounds.valueLen])
	extensions := bytes.Clone(record[bounds.extensionsStart : bounds.extensionsStart+bounds.extensionsLen])
	return value, extensions, nil
}

func (l *appendLog) readRecordFromFile(recordOffset, storedSize int64, scratch []byte) ([]byte, []byte, error) {
	if cap(scratch) < int(storedSize) {
		scratch = make([]byte, storedSize)
	} else {
		scratch = scratch[:storedSize]
	}
	if _, err := l.file.ReadAt(scratch, recordOffset); err != nil {
		return nil, scratch, err
	}
	bounds, err := validateStoredRecord(scratch)
	if err != nil {
		return nil, scratch, err
	}
	return scratch[bounds.valueStart : bounds.valueStart+bounds.valueLen], scratch, nil
}

func (l *appendLog) Iterate(fn func(*logRecord) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.file.Seek(logHeaderSize, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReaderSize(l.file, 1<<20)
	pos := int64(logHeaderSize)
	fileSize := l.writePos
	var fixed [16]byte
	var stringScratch []byte
	var attributesScratch []byte
	var extensionsScratch []byte
	valueCopyScratch := make([]byte, 32<<10)
	for pos < fileSize {
		var record *logRecord
		var nextPos int64
		var err error
		if l.version >= 4 {
			record, nextPos, err = readRecordFrom(r, pos, fixed[:], &stringScratch, &attributesScratch, &extensionsScratch, valueCopyScratch)
		} else {
			record, nextPos, err = readLegacyRecordFrom(r, pos, l.version, fixed[:8], &stringScratch)
		}
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// Discarding the incomplete record repairs the log, and a repair
				// is a write. A log that needs a feature this build does not know
				// keeps its bytes: stop at the torn record and serve what came
				// before it.
				if l.unknownReadOnly() == 0 {
					if truncateErr := l.file.Truncate(pos); truncateErr != nil {
						return errors.Join(err, fmt.Errorf("truncate incomplete record: %w", truncateErr))
					}
				}
				l.writePos = pos
				return nil
			}
			return fmt.Errorf("%w at offset %d: %v", ErrCorruptedData, pos, err)
		}
		if nextPos > fileSize {
			return fmt.Errorf("%w: record at offset %d exceeds file size", ErrCorruptedData, pos)
		}
		if err := fn(record); err != nil {
			return err
		}
		pos = nextPos
	}
	return nil
}

// readRecordFrom reads one version 4 or version 5 record. The two differ only
// in whether the extension area may be non-empty, so one reader serves both.
func readRecordFrom(r *bufio.Reader, startPos int64, fixed []byte, keyScratch, attributesScratch, extensionsScratch *[]byte, copyScratch []byte) (*logRecord, int64, error) {
	pos := startPos
	if _, err := io.ReadFull(r, fixed[:4]); err != nil {
		return nil, 0, err
	}
	pos += 4
	recordLength := binary.LittleEndian.Uint32(fixed[:4])
	if recordLength < recordMinimumLength {
		return nil, 0, fmt.Errorf("invalid record length %d", recordLength)
	}
	if _, err := io.ReadFull(r, fixed[:recordFixedBodySize]); err != nil {
		return nil, 0, err
	}
	pos += recordFixedBodySize
	hash := crc32.New(checksumTable)
	_, _ = hash.Write(fixed[:recordFixedBodySize])
	flags := fixed[0]
	keyLen := binary.LittleEndian.Uint32(fixed[4:8])
	attributesLen := binary.LittleEndian.Uint32(fixed[8:12])
	valueLen := binary.LittleEndian.Uint32(fixed[12:16])
	if keyLen > maxKeySize || attributesLen > maxAttributesSize || int64(valueLen) > maxValueSize {
		return nil, 0, fmt.Errorf("record field exceeds configured limit")
	}
	// The known fields must fit inside the record, not fill it. The remainder is
	// the extension area, which this reader steps over.
	knownLength := uint64(recordMinimumLength) + uint64(keyLen) + uint64(attributesLen) + uint64(valueLen)
	if knownLength > uint64(recordLength) {
		return nil, 0, fmt.Errorf("record length %d is smaller than its fields", recordLength)
	}
	extensionsLen := uint64(recordLength) - knownLength
	if extensionsLen > maxExtensionsSize {
		return nil, 0, fmt.Errorf("extension area is %d bytes (maximum %d)", extensionsLen, maxExtensionsSize)
	}
	keyBytes := resizeScratch(keyScratch, int(keyLen))
	if _, err := io.ReadFull(r, keyBytes); err != nil {
		return nil, 0, err
	}
	_, _ = hash.Write(keyBytes)
	pos += int64(keyLen)
	attributesBytes := resizeScratch(attributesScratch, int(attributesLen))
	if _, err := io.ReadFull(r, attributesBytes); err != nil {
		return nil, 0, err
	}
	_, _ = hash.Write(attributesBytes)
	pos += int64(attributesLen)
	valueOffset := pos
	copied, err := io.CopyBuffer(hash, io.LimitReader(r, int64(valueLen)), copyScratch)
	if err != nil {
		return nil, 0, err
	}
	if copied != int64(valueLen) {
		return nil, 0, io.ErrUnexpectedEOF
	}
	pos += int64(valueLen)
	extensionBytes := resizeScratch(extensionsScratch, int(extensionsLen))
	if _, err := io.ReadFull(r, extensionBytes); err != nil {
		return nil, 0, err
	}
	_, _ = hash.Write(extensionBytes)
	pos += int64(extensionsLen)
	if _, err := io.ReadFull(r, fixed[:4]); err != nil {
		return nil, 0, err
	}
	pos += 4
	wantChecksum := binary.LittleEndian.Uint32(fixed[:4])
	if hash.Sum32() != wantChecksum {
		return nil, 0, fmt.Errorf("checksum mismatch")
	}
	// Framing is checked after the checksum so that a damaged record is
	// reported as corruption rather than as a malformed extension.
	if err := validateExtensions(extensionBytes); err != nil {
		return nil, 0, err
	}
	attributes, err := decodeAttributes(attributesBytes)
	if err != nil {
		return nil, 0, fmt.Errorf("decode attributes: %w", err)
	}
	return &logRecord{
		Key:          string(keyBytes),
		Offset:       valueOffset,
		Size:         int32(valueLen),
		RecordOffset: startPos,
		StoredSize:   int64(recordLength) + 4,
		Deleted:      flags&1 != 0,
		Attributes:   attributes,
	}, pos, nil
}

func readLegacyRecordFrom(r *bufio.Reader, startPos int64, version uint32, fixed []byte, stringScratch *[]byte) (*logRecord, int64, error) {
	pos := startPos
	readU32 := func() (uint32, error) {
		if _, err := io.ReadFull(r, fixed[:4]); err != nil {
			return 0, err
		}
		pos += 4
		return binary.LittleEndian.Uint32(fixed[:4]), nil
	}
	readU16 := func() (uint16, error) {
		if _, err := io.ReadFull(r, fixed[:2]); err != nil {
			return 0, err
		}
		pos += 2
		return binary.LittleEndian.Uint16(fixed[:2]), nil
	}
	readU64 := func() (int64, error) {
		if _, err := io.ReadFull(r, fixed[:8]); err != nil {
			return 0, err
		}
		pos += 8
		return int64(binary.LittleEndian.Uint64(fixed[:8])), nil
	}
	readString := func(length int) (string, error) {
		buf := resizeScratch(stringScratch, length)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		pos += int64(length)
		return string(buf), nil
	}
	keyLen, err := readU32()
	if err != nil {
		return nil, 0, err
	}
	if keyLen > maxKeySize {
		return nil, 0, fmt.Errorf("invalid key length %d", keyLen)
	}
	key, err := readString(int(keyLen))
	if err != nil {
		return nil, 0, err
	}
	valueLen, err := readU32()
	if err != nil {
		return nil, 0, err
	}
	if int64(valueLen) > maxValueSize {
		return nil, 0, fmt.Errorf("invalid value length %d", valueLen)
	}
	valueOffset := pos
	if _, err := r.Discard(int(valueLen)); err != nil {
		return nil, 0, err
	}
	pos += int64(valueLen)
	if _, err := io.ReadFull(r, fixed[:1]); err != nil {
		return nil, 0, err
	}
	pos++
	flags := fixed[0]
	readField := func() (string, error) {
		length, err := readU16()
		if err != nil {
			return "", err
		}
		return readString(int(length))
	}
	category, err := readField()
	if err != nil {
		return nil, 0, err
	}
	provider, err := readField()
	if err != nil {
		return nil, 0, err
	}
	status, err := readField()
	if err != nil {
		return nil, 0, err
	}
	name, err := readField()
	if err != nil {
		return nil, 0, err
	}
	totalSize, err := readU64()
	if err != nil {
		return nil, 0, err
	}
	attributes := map[string]string{
		legacyCategoryAttribute:  category,
		legacyProviderAttribute:  provider,
		legacyStatusAttribute:    status,
		legacyNameAttribute:      name,
		legacyTotalSizeAttribute: strconv.FormatInt(totalSize, 10),
	}
	if version >= 3 {
		protocol, err := readField()
		if err != nil {
			return nil, 0, err
		}
		addedOn, err := readU64()
		if err != nil {
			return nil, 0, err
		}
		attributes[legacyProtocolAttribute] = protocol
		attributes[legacyBadAttribute] = strconv.FormatBool(flags&2 != 0)
		attributes[legacyAddedOnAttribute] = strconv.FormatInt(addedOn, 10)
	}
	return &logRecord{
		Key:          key,
		Offset:       valueOffset,
		Size:         int32(valueLen),
		RecordOffset: startPos,
		StoredSize:   pos - startPos,
		Deleted:      flags&1 != 0,
		Attributes:   attributes,
	}, pos, nil
}

func resizeScratch(scratch *[]byte, length int) []byte {
	if cap(*scratch) < length {
		*scratch = make([]byte, length)
	}
	return (*scratch)[:length]
}

func (l *appendLog) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Sync()
}

func (l *appendLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.mapped != nil {
		_ = munmapFile(l.mapped)
		l.mapped = nil
	}
	return l.file.Close()
}

func (l *appendLog) Size() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.writePos
}
