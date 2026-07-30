package appendstore

import (
	"errors"
	"fmt"
	"os"
)

const lockFileSuffix = ".lock"

// fileLock is held for the lifetime of a Store. The lock file is deliberately
// not removed on Close: removing it can race with another process that has
// already locked the old file, allowing a third process to lock a new inode.
type fileLock struct {
	file *os.File
}

func acquireFileLock(path string) (*fileLock, error) {
	lockPath := path + lockFileSuffix
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open store lock: %w", err)
	}
	if err := tryLockFile(file); err != nil {
		_ = file.Close()
		if isLockContended(err) {
			return nil, fmt.Errorf("%w: %s", ErrStoreLocked, path)
		}
		return nil, fmt.Errorf("acquire store lock: %w", err)
	}
	return &fileLock{file: file}, nil
}

func (l *fileLock) release() error {
	if l == nil || l.file == nil {
		return nil
	}
	file := l.file
	l.file = nil
	return errors.Join(unlockFile(file), file.Close())
}
