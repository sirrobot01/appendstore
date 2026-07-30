//go:build !windows

package appendstore

import (
	"os"
	"syscall"
)

// mmapFile maps length bytes of file read-only. length may exceed the current
// file size: pages past the end of file become readable as the file grows
// underneath the mapping, which is what lets the log map ahead with slack.
func mmapFile(file *os.File, length int64) ([]byte, error) {
	return syscall.Mmap(int(file.Fd()), 0, int(length), syscall.PROT_READ, syscall.MAP_SHARED)
}

func munmapFile(mapped []byte) error {
	return syscall.Munmap(mapped)
}
