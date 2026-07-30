package appendstore

import (
	"bufio"
	"bytes"
	"testing"
)

func FuzzReadRecordFrom(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{1, 0, 0, 0, 'k', 1, 0, 0, 0, 'v'})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		reader := bufio.NewReader(bytes.NewReader(data))
		var fixed [16]byte
		var key []byte
		var attributes []byte
		_, _, _ = readV4RecordFrom(reader, logHeaderSize, fixed[:], &key, &attributes, make([]byte, 32<<10))
	})
}
