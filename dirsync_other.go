//go:build !windows

package appendstore

import (
	"os"
	"path/filepath"
)

func syncParentDirectory(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
