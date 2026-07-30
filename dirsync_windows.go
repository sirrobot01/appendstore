//go:build windows

package appendstore

// Windows does not support syncing directory handles through os.File.Sync.
// The recoverable main/ready/backup protocol still handles process crashes.
func syncParentDirectory(string) error { return nil }
