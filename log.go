package appendstore

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
)

const (
	logMagic      = "HYBR"
	logVersion    = uint32(4)
	logHeaderSize = 16

	maxKeySize        = 1 << 20
	maxValueSize      = int64(1<<31 - 1)
	maxAttributesSize = 16 << 20
	maxAttributeCount = 4096

	v4FixedBodySize = 16                  // flags/reserved + keyLen + attributesLen + valueLen
	v4MinimumLength = v4FixedBodySize + 4 // fixed body + checksum

	compactionTempSuffix   = ".compact.tmp"
	compactionReadySuffix  = ".compact.ready"
	compactionBackupSuffix = ".compact.backup"
	legacyCompactionSuffix = ".compact"

	legacyCategoryAttribute  = "category"
	legacyProviderAttribute  = "provider"
	legacyStatusAttribute    = "status"
	legacyNameAttribute      = "name"
	legacyTotalSizeAttribute = "total_size"
	legacyProtocolAttribute  = "protocol"
	legacyBadAttribute       = "bad"
	legacyAddedOnAttribute   = "added_on"
)

// Version 4 record layout (all integers are little-endian):
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
//	uint32 CRC32C of flags through value
//
// The explicit length makes incomplete-tail recovery unambiguous. The checksum
// detects corruption in keys, attributes, values, and structural fields.
var checksumTable = crc32.MakeTable(crc32.Castagnoli)

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
	version  uint32
}

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
	log := &appendLog{file: file, path: path, version: logVersion}
	if info.Size() == 0 {
		if err := log.writeHeader(); err != nil {
			_ = file.Close()
			return nil, err
		}
		log.writePos = logHeaderSize
	} else {
		version, err := log.validateHeader()
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		log.version = version
		log.writePos = info.Size()
	}
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
	log := &appendLog{file: file, path: path, version: logVersion}
	if err := log.writeHeader(); err != nil {
		_ = file.Close()
		return nil, err
	}
	log.writePos = logHeaderSize
	return log, nil
}

func (l *appendLog) writeHeader() error {
	header := make([]byte, logHeaderSize)
	copy(header[:4], logMagic)
	binary.LittleEndian.PutUint32(header[4:8], logVersion)
	_, err := l.file.WriteAt(header, 0)
	return err
}

func (l *appendLog) validateHeader() (uint32, error) {
	header := make([]byte, logHeaderSize)
	if _, err := l.file.ReadAt(header, 0); err != nil {
		return 0, fmt.Errorf("%w: read header: %v", ErrCorruptedData, err)
	}
	if string(header[:4]) != logMagic {
		return 0, fmt.Errorf("%w: invalid magic", ErrCorruptedData)
	}
	version := binary.LittleEndian.Uint32(header[4:8])
	if version == 0 {
		return 0, fmt.Errorf("%w: invalid log version 0", ErrCorruptedData)
	}
	if version > logVersion {
		return 0, fmt.Errorf("%w: %d (maximum %d)", ErrUnsupportedVersion, version, logVersion)
	}
	return version, nil
}

// Append writes one v4 record and returns the value and record locations.
func (l *appendLog) Append(key string, value []byte, deleted bool, attributes map[string]string) (offset int64, size int32, recordOffset int64, storedSize int64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.version != logVersion {
		return 0, 0, 0, 0, fmt.Errorf("cannot append v4 record to v%d log", l.version)
	}
	keyBytes := []byte(key)
	if len(keyBytes) > maxKeySize {
		return 0, 0, 0, 0, fmt.Errorf("%w: key is %d bytes (maximum %d)", ErrInvalidKey, len(keyBytes), maxKeySize)
	}
	if int64(len(value)) > maxValueSize {
		return 0, 0, 0, 0, fmt.Errorf("%w: value is %d bytes (maximum %d)", ErrValueTooLarge, len(value), maxValueSize)
	}
	attributesBytes, err := encodeAttributes(attributes)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	recordLength := int64(v4MinimumLength) + int64(len(keyBytes)+len(attributesBytes)+len(value))
	if recordLength > int64(^uint32(0)) {
		return 0, 0, 0, 0, fmt.Errorf("%w: encoded record is %d bytes", ErrValueTooLarge, recordLength)
	}
	totalSize := 4 + int(recordLength)
	buf := make([]byte, totalSize)
	binary.LittleEndian.PutUint32(buf[:4], uint32(recordLength))
	if deleted {
		buf[4] = 1
	}
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
	binary.LittleEndian.PutUint32(buf[pos:], crc32.Checksum(buf[4:pos], checksumTable))
	recordStart := l.writePos
	if _, err := l.file.WriteAt(buf, recordStart); err != nil {
		return 0, 0, 0, 0, err
	}
	l.writePos += int64(totalSize)
	return valueOffset, int32(len(value)), recordStart, int64(totalSize), nil
}

func encodeAttributes(attributes map[string]string) ([]byte, error) {
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

func (l *appendLog) ReadAt(offset int64, size int32) ([]byte, error) {
	buf := make([]byte, size)
	if _, err := l.file.ReadAt(buf, offset); err != nil {
		return nil, err
	}
	return buf, nil
}

func (l *appendLog) ReadRecordAt(recordOffset, storedSize int64) ([]byte, error) {
	value, _, err := l.readRecordAtInto(recordOffset, storedSize, nil)
	return value, err
}

func (l *appendLog) ReadRecordAtInto(recordOffset, storedSize int64, scratch []byte) ([]byte, []byte, error) {
	return l.readRecordAtInto(recordOffset, storedSize, scratch)
}

func (l *appendLog) readRecordAtInto(recordOffset, storedSize int64, scratch []byte) ([]byte, []byte, error) {
	if storedSize < 4+v4MinimumLength || storedSize > int64(int(^uint(0)>>1)) {
		return nil, scratch, fmt.Errorf("%w: invalid stored record size %d", ErrCorruptedData, storedSize)
	}
	if cap(scratch) < int(storedSize) {
		scratch = make([]byte, storedSize)
	} else {
		scratch = scratch[:storedSize]
	}
	if _, err := l.file.ReadAt(scratch, recordOffset); err != nil {
		return nil, scratch, err
	}
	recordLength := int64(binary.LittleEndian.Uint32(scratch[:4]))
	if recordLength+4 != storedSize {
		return nil, scratch, fmt.Errorf("%w: record length mismatch", ErrCorruptedData)
	}
	wantChecksum := binary.LittleEndian.Uint32(scratch[len(scratch)-4:])
	gotChecksum := crc32.Checksum(scratch[4:len(scratch)-4], checksumTable)
	if gotChecksum != wantChecksum {
		return nil, scratch, fmt.Errorf("%w: checksum mismatch", ErrCorruptedData)
	}
	keyLen := int64(binary.LittleEndian.Uint32(scratch[8:12]))
	attributesLen := int64(binary.LittleEndian.Uint32(scratch[12:16]))
	valueLen := int64(binary.LittleEndian.Uint32(scratch[16:20]))
	if int64(v4MinimumLength)+keyLen+attributesLen+valueLen != recordLength {
		return nil, scratch, fmt.Errorf("%w: invalid record field lengths", ErrCorruptedData)
	}
	valueStart := int64(20) + keyLen + attributesLen
	return scratch[valueStart : valueStart+valueLen], scratch, nil
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
	valueCopyScratch := make([]byte, 32<<10)
	for pos < fileSize {
		var record *logRecord
		var nextPos int64
		var err error
		if l.version >= 4 {
			record, nextPos, err = readV4RecordFrom(r, pos, fixed[:], &stringScratch, &attributesScratch, valueCopyScratch)
		} else {
			record, nextPos, err = readLegacyRecordFrom(r, pos, l.version, fixed[:8], &stringScratch)
		}
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if truncateErr := l.file.Truncate(pos); truncateErr != nil {
					return errors.Join(err, fmt.Errorf("truncate incomplete record: %w", truncateErr))
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

func readV4RecordFrom(r *bufio.Reader, startPos int64, fixed []byte, keyScratch, attributesScratch *[]byte, copyScratch []byte) (*logRecord, int64, error) {
	pos := startPos
	if _, err := io.ReadFull(r, fixed[:4]); err != nil {
		return nil, 0, err
	}
	pos += 4
	recordLength := binary.LittleEndian.Uint32(fixed[:4])
	if recordLength < v4MinimumLength {
		return nil, 0, fmt.Errorf("invalid record length %d", recordLength)
	}
	if _, err := io.ReadFull(r, fixed[:v4FixedBodySize]); err != nil {
		return nil, 0, err
	}
	pos += v4FixedBodySize
	hash := crc32.New(checksumTable)
	_, _ = hash.Write(fixed[:v4FixedBodySize])
	flags := fixed[0]
	keyLen := binary.LittleEndian.Uint32(fixed[4:8])
	attributesLen := binary.LittleEndian.Uint32(fixed[8:12])
	valueLen := binary.LittleEndian.Uint32(fixed[12:16])
	if keyLen > maxKeySize || attributesLen > maxAttributesSize || int64(valueLen) > maxValueSize {
		return nil, 0, fmt.Errorf("record field exceeds configured limit")
	}
	expectedLength := uint64(v4MinimumLength) + uint64(keyLen) + uint64(attributesLen) + uint64(valueLen)
	if uint64(recordLength) != expectedLength {
		return nil, 0, fmt.Errorf("record length %d does not match fields", recordLength)
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
	if _, err := io.ReadFull(r, fixed[:4]); err != nil {
		return nil, 0, err
	}
	pos += 4
	wantChecksum := binary.LittleEndian.Uint32(fixed[:4])
	if hash.Sum32() != wantChecksum {
		return nil, 0, fmt.Errorf("checksum mismatch")
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
	return l.file.Close()
}

func (l *appendLog) Size() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.writePos
}
