package appendstore

import "sort"

// Metadata is the application-defined metadata associated with a value.
// Its Attributes map is always a defensive copy owned by the caller.
type Metadata struct {
	Attributes map[string]string
}

// Attribute returns one metadata attribute.
func (m Metadata) Attribute(name string) string {
	return m.Attributes[name]
}

type indexEntry struct {
	Offset       int64
	Size         int32
	RecordOffset int64
	StoredSize   int64
	Attributes   map[string]string
}

// index is the in-memory primary and secondary index. Store provides
// synchronization for every access.
type index struct {
	entries       map[string]*indexEntry
	indexedFields map[string]struct{}
	byAttribute   map[string]map[string]map[string]struct{} // field -> value -> keys

	sortedKeys  []string
	sortedDirty bool
}

func newIndex(indexedFields []string) *index {
	fields := make(map[string]struct{}, len(indexedFields))
	secondary := make(map[string]map[string]map[string]struct{}, len(indexedFields))
	for _, field := range indexedFields {
		if field == "" {
			continue
		}
		fields[field] = struct{}{}
		secondary[field] = make(map[string]map[string]struct{})
	}
	return &index{
		entries:       make(map[string]*indexEntry),
		indexedFields: fields,
		byAttribute:   secondary,
	}
}

func (idx *index) Put(key string, entry *indexEntry) {
	if existing, ok := idx.entries[key]; ok {
		idx.removeFromSecondary(key, existing)
	}
	entry.Attributes = cloneAttributes(entry.Attributes)
	idx.entries[key] = entry
	idx.addToSecondary(key, entry)
	idx.sortedDirty = true
}

func (idx *index) Get(key string) *indexEntry {
	entry := idx.entries[key]
	if entry == nil {
		return nil
	}
	copy := *entry
	copy.Attributes = cloneAttributes(entry.Attributes)
	return &copy
}

func (idx *index) Delete(key string) {
	if entry, ok := idx.entries[key]; ok {
		idx.removeFromSecondary(key, entry)
		idx.sortedDirty = true
		delete(idx.entries, key)
	}
}

func (idx *index) Len() int {
	return len(idx.entries)
}

func (idx *index) Keys() []string {
	keys := make([]string, 0, len(idx.entries))
	for key := range idx.entries {
		keys = append(keys, key)
	}
	return keys
}

func (idx *index) KeysSortedByOffset() []string {
	if !idx.sortedDirty && len(idx.sortedKeys) == len(idx.entries) {
		return append([]string(nil), idx.sortedKeys...)
	}
	type keyOffset struct {
		key string
		off int64
	}
	pairs := make([]keyOffset, 0, len(idx.entries))
	for key, entry := range idx.entries {
		pairs = append(pairs, keyOffset{key: key, off: entry.RecordOffset})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].off < pairs[j].off
	})
	idx.sortedKeys = make([]string, len(pairs))
	for i := range pairs {
		idx.sortedKeys[i] = pairs[i].key
	}
	idx.sortedDirty = false
	return append([]string(nil), idx.sortedKeys...)
}

func (idx *index) ForEach(fn func(key string, entry *indexEntry) error) error {
	type pair struct {
		key   string
		entry indexEntry
	}
	pairs := make([]pair, 0, len(idx.entries))
	for key, entry := range idx.entries {
		copy := *entry
		copy.Attributes = cloneAttributes(entry.Attributes)
		pairs = append(pairs, pair{key: key, entry: copy})
	}
	for i := range pairs {
		if err := fn(pairs[i].key, &pairs[i].entry); err != nil {
			return err
		}
	}
	return nil
}

func (idx *index) KeysBy(field, value string) []string {
	values := idx.byAttribute[field]
	if values == nil {
		return nil
	}
	keySet := values[value]
	keys := make([]string, 0, len(keySet))
	for key := range keySet {
		keys = append(keys, key)
	}
	return keys
}

func (idx *index) AttributeValues(field string) []string {
	values := idx.byAttribute[field]
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func (idx *index) TotalSize() int64 {
	var total int64
	for _, entry := range idx.entries {
		total += entry.StoredSize
	}
	return total
}

func (idx *index) MemoryUsage() int64 {
	var total int64
	for key, entry := range idx.entries {
		total += int64(len(key)) + 96
		for name, value := range entry.Attributes {
			total += int64(len(name)+len(value)) + 32
		}
	}
	for _, values := range idx.byAttribute {
		for value, keys := range values {
			total += int64(len(value)) + int64(len(keys))*50
		}
	}
	return total
}

func (idx *index) addToSecondary(key string, entry *indexEntry) {
	for field := range idx.indexedFields {
		value, ok := entry.Attributes[field]
		if !ok {
			continue
		}
		values := idx.byAttribute[field]
		if values[value] == nil {
			values[value] = make(map[string]struct{})
		}
		values[value][key] = struct{}{}
	}
}

func (idx *index) removeFromSecondary(key string, entry *indexEntry) {
	for field := range idx.indexedFields {
		value, ok := entry.Attributes[field]
		if !ok {
			continue
		}
		values := idx.byAttribute[field]
		keySet := values[value]
		delete(keySet, key)
		if len(keySet) == 0 {
			delete(values, value)
		}
	}
}

func cloneAttributes(attributes map[string]string) map[string]string {
	if len(attributes) == 0 {
		return nil
	}
	copy := make(map[string]string, len(attributes))
	for key, value := range attributes {
		copy[key] = value
	}
	return copy
}
