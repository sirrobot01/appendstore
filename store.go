// Package appendstore provides a high-performance append-only log storage engine
// with in-memory indexing, LRU caching, and secondary indexes.
//
// Architecture:
//   - Append-only log file for durability
//   - In-memory primary index for O(1) key lookups
//   - LRU cache for frequently accessed entries
//   - Optional secondary indexes over application-defined attributes
//   - Background compaction to reclaim deleted space
//
// Thread Safety:
//   - All operations are thread-safe via RWMutex
//   - Reads can proceed concurrently
//   - Writes are serialized
//   - Open acquires an exclusive process lock for the database path
//
// Durability:
//   - All writes are appended to the log immediately
//   - Index is rebuilt from log on startup (crash recovery)
//   - Optional periodic sync for fsync guarantees
package appendstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Common errors
var (
	ErrStoreClosed          = errors.New("store is closed")
	ErrStoreLocked          = errors.New("store is already open by another process")
	ErrCorruptedData        = errors.New("corrupted data detected")
	ErrUnsupportedVersion   = errors.New("unsupported log version")
	ErrCompactionInProgress = errors.New("compaction already in progress")
	ErrKeyNotFound          = errors.New("key not found")
	ErrInvalidKey           = errors.New("invalid key")
	ErrInvalidAttribute     = errors.New("invalid attribute name")
	ErrValueTooLarge        = errors.New("value too large")
	ErrAttributesTooLarge   = errors.New("attributes too large")
)

// Options configures a Store.
type Options struct {
	// CacheSize is the maximum number of entries to cache (default: 1000)
	CacheSize int

	// SyncInterval controls automatic fsync behavior: 0 syncs every mutation,
	// a positive duration syncs periodically, and a negative duration disables
	// automatic syncing (including on Close). Sync can always be called explicitly.
	SyncInterval time.Duration

	// CompactionThreshold is the deleted entry ratio that triggers compaction (default: 0.2)
	CompactionThreshold float64

	// AutoCompact enables automatic background compaction
	AutoCompact bool

	// IndexedFields lists attribute names that should have in-memory secondary
	// indexes. Only these fields can be queried with KeysBy efficiently.
	IndexedFields []string

	// OnError receives errors from background sync and compaction. It is never
	// called while the store mutex is held.
	OnError func(error)
}

// Store is an append-only storage engine.
type Store struct {
	mu sync.RWMutex

	// Core components
	log   *appendLog
	index *index
	cache *lruCache
	lock  *fileLock

	// Configuration
	options Options

	// State
	closed     atomic.Bool
	compacting atomic.Bool

	// Background tasks
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Metrics
	stats statsCounters
}

type statsCounters struct {
	Writes      atomic.Int64
	Reads       atomic.Int64
	CacheHits   atomic.Int64
	CacheMisses atomic.Int64
	Deletes     atomic.Int64
	Compactions atomic.Int64
}

// Stats is an instantaneous snapshot of operational counters. Reads counts
// every value returned by Get and ForEach, whether served from the cache or
// from disk; CacheHits and CacheMisses partition the Get calls.
type Stats struct {
	Writes      int64
	Reads       int64
	CacheHits   int64
	CacheMisses int64
	Deletes     int64
	Compactions int64
}

// Open opens or creates the append-only store at path.
func Open(path string, options Options) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("path is required")
	}

	// Apply defaults
	if options.CacheSize <= 0 {
		options.CacheSize = 1000
	}
	if options.CompactionThreshold <= 0 {
		options.CompactionThreshold = 0.2
	}
	if options.CompactionThreshold > 1 {
		return nil, fmt.Errorf("compaction threshold must be between 0 and 1")
	}
	for _, field := range options.IndexedFields {
		if field == "" {
			return nil, fmt.Errorf("%w: indexed field is empty", ErrInvalidAttribute)
		}
	}
	options.IndexedFields = append([]string(nil), options.IndexedFields...)

	ctx, cancel := context.WithCancel(context.Background())
	lock, err := acquireFileLock(path)
	if err != nil {
		cancel()
		return nil, err
	}

	s := &Store{
		options: options,
		ctx:     ctx,
		cancel:  cancel,
		lock:    lock,
	}

	// Initialize components
	s.log, err = openAppendLog(path)
	if err != nil {
		_ = lock.release()
		cancel()
		return nil, fmt.Errorf("failed to open append log: %w", err)
	}

	s.index = newIndex(options.IndexedFields)
	s.cache = newLRUCache(options.CacheSize)

	// Recover index from log
	if err := s.recover(); err != nil {
		_ = s.log.Close()
		_ = lock.release()
		cancel()
		return nil, fmt.Errorf("failed to recover from log: %w", err)
	}

	// Start background tasks
	if options.SyncInterval > 0 {
		s.startSyncTask()
	}
	if options.AutoCompact {
		s.startCompactionTask()
	}
	return s, nil
}

// Close shuts down the store gracefully
func (s *Store) Close() error {
	if s.closed.Swap(true) {
		return nil // Already closed
	}

	// Stop background tasks
	s.cancel()
	s.wg.Wait()

	// Final sync
	s.mu.Lock()
	defer s.mu.Unlock()

	var syncErr error
	if s.options.SyncInterval >= 0 {
		syncErr = s.log.Sync()
	}
	return errors.Join(syncErr, s.log.Close(), s.lock.release())
}

// recover rebuilds the index from the append log
func (s *Store) recover() error {
	err := s.log.Iterate(func(record *logRecord) error {
		if record.Deleted {
			s.index.Delete(record.Key)
			s.cache.Remove(record.Key)
		} else {
			s.index.Put(record.Key, &indexEntry{
				Offset:       record.Offset,
				Size:         record.Size,
				RecordOffset: record.RecordOffset,
				StoredSize:   record.StoredSize,
				Attributes:   record.Attributes,
			})
		}
		return nil
	})

	if err != nil {
		return err
	}
	return nil
}

// startSyncTask periodically syncs the log to disk
func (s *Store) startSyncTask() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.options.SyncInterval)
		defer ticker.Stop()

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				s.mu.Lock()
				err := s.log.Sync()
				s.mu.Unlock()
				if err != nil {
					s.reportError(fmt.Errorf("periodic sync: %w", err))
				}
			}
		}
	}()
}

// startCompactionTask periodically checks if compaction is needed
func (s *Store) startCompactionTask() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				if s.NeedsCompaction() {
					if err := s.Compact(); err != nil {
						s.reportError(fmt.Errorf("automatic compaction: %w", err))
					}
				}
			}
		}
	}()
}

func (s *Store) reportError(err error) {
	if err != nil && s.options.OnError != nil {
		s.options.OnError(err)
	}
}

// PutOptions describes application-defined metadata for a value.
type PutOptions struct {
	Attributes map[string]string
}

// Put stores a key-value pair with optional application-defined attributes.
func (s *Store) Put(key string, value []byte, options *PutOptions) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var attributes map[string]string
	if options != nil {
		attributes = options.Attributes
	}

	// Write to log
	offset, size, recordOffset, storedSize, err := s.log.Append(key, value, false, attributes)
	if err != nil {
		return fmt.Errorf("failed to append to log: %w", err)
	}

	// Update index
	s.index.Put(key, &indexEntry{
		Offset:       offset,
		Size:         size,
		RecordOffset: recordOffset,
		StoredSize:   storedSize,
		Attributes:   attributes,
	})

	// Invalidate cache (will be populated on next read)
	s.cache.Remove(key)

	s.stats.Writes.Add(1)
	if s.options.SyncInterval == 0 {
		if err := s.log.Sync(); err != nil {
			return fmt.Errorf("sync appended value: %w", err)
		}
	}
	return nil
}

// Get retrieves a value by key
func (s *Store) Get(key string) ([]byte, error) {
	if s.closed.Load() {
		return nil, ErrStoreClosed
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check cache first
	if value, ok := s.cache.Get(key); ok {
		s.stats.CacheHits.Add(1)
		s.stats.Reads.Add(1)
		return bytes.Clone(value), nil
	}

	// Look up in index
	entry := s.index.Get(key)
	if entry == nil {
		return nil, fmt.Errorf("key %s: %w", key, ErrKeyNotFound)
	}

	// Read from log
	value, err := s.log.ReadRecordAt(entry.RecordOffset, entry.StoredSize)
	if err != nil {
		return nil, fmt.Errorf("failed to read from log: %w", err)
	}

	// The cache has its own lock, so fill it while retaining the store read
	// lock. This prevents a concurrent Put from invalidating the key and then
	// being overwritten with the stale value read above.
	s.cache.Put(key, bytes.Clone(value))

	s.stats.CacheMisses.Add(1)
	s.stats.Reads.Add(1)
	return value, nil
}

// GetMetadata retrieves metadata without reading the full value.
// This is O(1) from the in-memory index - no disk access
func (s *Store) GetMetadata(key string) (*Metadata, error) {
	if s.closed.Load() {
		return nil, ErrStoreClosed
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	entry := s.index.Get(key)
	if entry == nil {
		return nil, fmt.Errorf("key %s: %w", key, ErrKeyNotFound)
	}

	return &Metadata{Attributes: cloneAttributes(entry.Attributes)}, nil
}

// Delete removes a key
func (s *Store) Delete(key string) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if key exists
	if s.index.Get(key) == nil {
		return fmt.Errorf("key %s: %w", key, ErrKeyNotFound)
	}

	// Write tombstone to log
	if _, _, _, _, err := s.log.Append(key, nil, true, nil); err != nil {
		return fmt.Errorf("failed to write tombstone: %w", err)
	}
	// Remove from index and cache
	s.index.Delete(key)
	s.cache.Remove(key)

	s.stats.Deletes.Add(1)
	if s.options.SyncInterval == 0 {
		if err := s.log.Sync(); err != nil {
			return fmt.Errorf("sync tombstone: %w", err)
		}
	}
	return nil
}

// Exists checks if a key exists (O(1) from index)
func (s *Store) Exists(key string) bool {
	if s.closed.Load() {
		return false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.index.Get(key) != nil
}

// Len returns the number of entries
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.Len()
}

// Keys returns all keys (snapshot)
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.Keys()
}

// ForEach iterates over all entries in disk order (optimized for sequential
// I/O). The value passed to fn is owned by ForEach and only valid for the
// duration of that call — callers that retain it must copy.
//
// Scans deliberately bypass the LRU cache: a full pass touches every entry once
// and never reuses them, so caching would evict the genuinely-hot working set
// (active streams) and replace it with single-use scan data. Each value is read
// under a short RLock so writers interleave between keys and reads observe the
// current log even across a compaction swap. A single reusable buffer keeps the
// scan allocation-free per record.
func (s *Store) ForEach(fn func(key string, value []byte) error) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	s.mu.RLock()
	keys := s.index.KeysSortedByOffset()
	s.mu.RUnlock()

	var scratch []byte
	for _, key := range keys {
		s.mu.RLock()
		entry := s.index.Get(key)
		if entry == nil {
			s.mu.RUnlock()
			continue // deleted between the snapshot and now
		}
		value, nextScratch, err := s.log.ReadRecordAtInto(entry.RecordOffset, entry.StoredSize, scratch)
		s.mu.RUnlock()
		if err != nil {
			return fmt.Errorf("failed to read from log: %w", err)
		}
		scratch = nextScratch
		s.stats.Reads.Add(1)

		if err := fn(key, value); err != nil {
			return err
		}
	}

	return nil
}

// ForEachMetadata iterates over a snapshot of metadata only (no disk reads).
func (s *Store) ForEachMetadata(fn func(key string, meta *Metadata) error) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	s.mu.RLock()
	type pair struct {
		key        string
		attributes map[string]string
	}
	pairs := make([]pair, 0, s.index.Len())
	err := s.index.ForEach(func(key string, entry *indexEntry) error {
		pairs = append(pairs, pair{key: key, attributes: cloneAttributes(entry.Attributes)})
		return nil
	})
	s.mu.RUnlock()
	if err != nil {
		return err
	}
	for i := range pairs {
		if err := fn(pairs[i].key, &Metadata{Attributes: pairs[i].attributes}); err != nil {
			return err
		}
	}
	return nil
}

// KeysBy returns keys with an indexed attribute equal to value.
func (s *Store) KeysBy(field, value string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.KeysBy(field, value)
}

// CountBy returns the number of keys matching an indexed attribute value.
func (s *Store) CountBy(field, value string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.index.KeysBy(field, value))
}

// AttributeValues returns the sorted values present for an indexed field.
func (s *Store) AttributeValues(field string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.AttributeValues(field)
}

// NeedsCompaction returns true if compaction would reclaim significant space
func (s *Store) NeedsCompaction() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	liveSize := s.index.TotalSize()
	dataSize := s.log.Size() - logHeaderSize

	if dataSize <= 0 {
		return false
	}

	deadRatio := 1.0 - (float64(liveSize) / float64(dataSize))
	return deadRatio > s.options.CompactionThreshold
}

// Compact removes deleted entries and rewrites the log
func (s *Store) Compact() error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	if !s.compacting.CompareAndSwap(false, true) {
		return ErrCompactionInProgress
	}
	defer s.compacting.Store(false)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Create new log file
	oldPath := s.log.path
	newLogPath := oldPath + compactionTempSuffix
	readyPath := oldPath + compactionReadySuffix
	backupPath := oldPath + compactionBackupSuffix
	if err := removeIfExists(newLogPath); err != nil {
		return err
	}
	if err := removeIfExists(readyPath); err != nil {
		return err
	}
	if err := removeIfExists(backupPath); err != nil {
		return err
	}
	info, err := os.Stat(oldPath)
	if err != nil {
		return fmt.Errorf("failed to stat log before compaction: %w", err)
	}
	newLog, err := createAppendLogMode(newLogPath, info.Mode().Perm())
	if err != nil {
		return fmt.Errorf("failed to create compaction log: %w", err)
	}

	// Write all live entries to new log
	newIndex := newIndex(s.options.IndexedFields)
	keys := s.index.KeysSortedByOffset()

	for _, key := range keys {
		entry := s.index.Get(key)
		if entry == nil {
			continue
		}

		// Read value from old log
		value, err := s.log.ReadRecordAt(entry.RecordOffset, entry.StoredSize)
		if err != nil {
			_ = newLog.Close()
			_ = os.Remove(newLogPath)
			return fmt.Errorf("failed to read during compaction: %w", err)
		}

		// Write to new log
		offset, size, recordOffset, storedSize, err := newLog.Append(key, value, false, entry.Attributes)
		if err != nil {
			_ = newLog.Close()
			_ = os.Remove(newLogPath)
			return fmt.Errorf("failed to write during compaction: %w", err)
		}

		// Update new index
		newIndex.Put(key, &indexEntry{
			Offset:       offset,
			Size:         size,
			RecordOffset: recordOffset,
			StoredSize:   storedSize,
			Attributes:   entry.Attributes,
		})
	}

	// Sync new log
	if err := newLog.Sync(); err != nil {
		_ = newLog.Close()
		_ = os.Remove(newLogPath)
		return fmt.Errorf("failed to sync compaction log: %w", err)
	}
	if err := newLog.Close(); err != nil {
		_ = os.Remove(newLogPath)
		return fmt.Errorf("failed to close compaction log: %w", err)
	}
	if err := os.Rename(newLogPath, readyPath); err != nil {
		_ = os.Remove(newLogPath)
		return fmt.Errorf("failed to prepare compaction log: %w", err)
	}
	if err := syncParentDirectory(oldPath); err != nil {
		_ = os.Remove(readyPath)
		return fmt.Errorf("failed to sync prepared compaction log: %w", err)
	}

	// Replace through a recoverable three-file transaction. Open() knows how
	// to finish either side of this sequence after a process crash.
	oldLog := s.log
	if err := oldLog.Close(); err != nil {
		reopened, reopenErr := openAppendLogFile(oldPath)
		if reopenErr == nil {
			s.log = reopened
		}
		_ = os.Remove(readyPath)
		return errors.Join(fmt.Errorf("failed to close old log during compaction: %w", err), reopenErr)
	}
	if err := os.Rename(oldPath, backupPath); err != nil {
		reopened, reopenErr := openAppendLogFile(oldPath)
		if reopenErr == nil {
			s.log = reopened
		}
		_ = os.Remove(readyPath)
		return errors.Join(fmt.Errorf("failed to back up old log: %w", err), reopenErr)
	}
	if err := os.Rename(readyPath, oldPath); err != nil {
		restoreErr := os.Rename(backupPath, oldPath)
		reopened, reopenErr := openAppendLogFile(oldPath)
		if reopenErr == nil {
			s.log = reopened
		}
		return errors.Join(fmt.Errorf("failed to install compacted log: %w", err), restoreErr, reopenErr)
	}

	reopened, err := openAppendLogFile(oldPath)
	if err != nil {
		removeErr := os.Remove(oldPath)
		restoreErr := os.Rename(backupPath, oldPath)
		oldLog, reopenErr := openAppendLogFile(oldPath)
		if reopenErr == nil {
			s.log = oldLog
		}
		return errors.Join(fmt.Errorf("failed to reopen compacted log: %w", err), removeErr, restoreErr, reopenErr)
	}
	s.log = reopened
	s.index = newIndex
	s.cache.Clear()
	if err := removeIfExists(backupPath); err != nil {
		return err
	}
	if err := syncParentDirectory(oldPath); err != nil {
		return fmt.Errorf("failed to sync compacted log replacement: %w", err)
	}

	s.stats.Compactions.Add(1)

	return nil
}

// Sync flushes all appended records to stable storage.
func (s *Store) Sync() error {
	if s.closed.Load() {
		return ErrStoreClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.Sync()
}

// Stats returns an instantaneous snapshot of operational counters.
func (s *Store) Stats() Stats {
	return Stats{
		Writes:      s.stats.Writes.Load(),
		Reads:       s.stats.Reads.Load(),
		CacheHits:   s.stats.CacheHits.Load(),
		CacheMisses: s.stats.CacheMisses.Load(),
		Deletes:     s.stats.Deletes.Load(),
		Compactions: s.stats.Compactions.Load(),
	}
}

// DiskSize returns the current log file size
func (s *Store) DiskSize() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.log.Size()
}

// MemoryUsage returns approximate in-memory usage
func (s *Store) MemoryUsage() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.MemoryUsage() + s.cache.MemoryUsage()
}
