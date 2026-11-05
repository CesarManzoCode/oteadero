package vfs

import (
	"os"
)

// OS is the real file system.
type OS struct{}

type osFile struct {
	*os.File
}

func (f osFile) Size() (int64, error) {
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

func (OS) Create(name string) (File, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

func (OS) OpenAppend(name string) (File, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

func (OS) Open(name string) (File, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

func (OS) Rename(oldname, newname string) error { return os.Rename(oldname, newname) }

func (OS) Remove(name string) error { return os.Remove(name) }

func (OS) ReadDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names, nil
}

func (OS) MkdirAll(dir string) error { return os.MkdirAll(dir, 0o755) }

func (OS) SyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = f.Sync()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}
