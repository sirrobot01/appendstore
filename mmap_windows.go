//go:build windows

package appendstore

import (
	"errors"
	"os"
)

// Memory-mapped reads are not implemented on Windows, where extending a file
// mapping past the end of file grows the file itself. Reads use ReadAt.
func mmapFile(*os.File, int64) ([]byte, error) {
	return nil, errors.ErrUnsupported
}

func munmapFile([]byte) error {
	return nil
}
