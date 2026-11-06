// Package vfs is the only way the engine touches the disk (D03, R11). OS
// works on the real file system; Mem keeps everything in memory and can
// simulate a crash that loses what was not synced.
package vfs

import "io"

// File is an open file. Writes always append at the end of the file.
type File interface {
	io.Writer
	io.ReaderAt
	// Sync makes the file contents durable.
	Sync() error
	// Truncate changes the size of the file; it is durable after Sync.
	Truncate(size int64) error
	// Size returns the current size of the file.
	Size() (int64, error)
	Close() error
}

// FS is the set of file system operations the log and the checkpoints need.
// Creating, renaming and removing a file become durable only after SyncDir
// on its parent directory.
type FS interface {
	// Create creates or truncates name and opens it for appending and reading.
	Create(name string) (File, error)
	// OpenAppend opens an existing file for appending and reading.
	OpenAppend(name string) (File, error)
	// Open opens an existing file for reading only.
	Open(name string) (File, error)
	Rename(oldname, newname string) error
	// Remove removes a file or an empty directory.
	Remove(name string) error
	// ReadDir returns the names of the entries of dir, sorted.
	ReadDir(dir string) ([]string, error)
	MkdirAll(dir string) error
	// SyncDir makes the entries of dir durable.
	SyncDir(dir string) error
}
