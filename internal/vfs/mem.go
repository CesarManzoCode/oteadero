package vfs

import (
	"errors"
	"io"
	"io/fs"
	"path"
	"slices"
	"sync"

	"github.com/CesarManzoCode/oteadero/internal/rng"
)

// ErrCrashed is returned by file handles opened before a Mem crash.
var ErrCrashed = errors.New("vfs: file handle invalidated by crash")

// Mem is an in-memory file system with crash semantics. Each file keeps the
// contents it had at its last Sync and each directory the entries it had at
// its last SyncDir; Crash goes back to that durable state. Directories made
// by MkdirAll are durable at once.
type Mem struct {
	mu      sync.Mutex
	gen     uint64
	dirs    map[string]bool
	files   map[string]*inode
	durable map[string]*inode
	last    lastWrite
}

type inode struct {
	data []byte
	// dur is the durable prefix. It may share the backing array of data
	// because bytes below len(dur) are never rewritten in place.
	dur []byte
}

type lastWrite struct {
	ino *inode
	off int
	n   int
}

// NewMem returns an empty Mem with the root directories "." and "/".
func NewMem() *Mem {
	return &Mem{
		dirs:    map[string]bool{".": true, "/": true},
		files:   map[string]*inode{},
		durable: map[string]*inode{},
	}
}

// Crash simulates a power loss: every file and directory entry goes back to
// its durable state and open handles stop working. If the last write was
// not synced and started at the durable end of its file, r decides how many
// of its bytes survive, from none to all, which may leave it torn.
func (m *Mem) Crash(r rng.Source) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gen++
	torn := map[*inode][]byte{}
	if lw := m.last; lw.ino != nil && lw.off == len(lw.ino.dur) {
		k := r.IntN(lw.n + 1)
		torn[lw.ino] = slices.Clone(lw.ino.data[lw.off : lw.off+k])
	}
	m.last = lastWrite{}
	m.files = make(map[string]*inode, len(m.durable))
	seen := map[*inode]bool{}
	for name, ino := range m.durable {
		m.files[name] = ino
		if seen[ino] {
			continue
		}
		seen[ino] = true
		data := append(slices.Clone(ino.dur), torn[ino]...)
		ino.data = data
		ino.dur = data[:len(data):len(data)]
	}
}

func (m *Mem) checkParent(op, name string) error {
	if !m.dirs[path.Dir(name)] {
		return &fs.PathError{Op: op, Path: name, Err: fs.ErrNotExist}
	}
	if m.dirs[name] {
		return &fs.PathError{Op: op, Path: name, Err: fs.ErrExist}
	}
	return nil
}

func (m *Mem) Create(name string) (File, error) {
	name = path.Clean(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkParent("create", name); err != nil {
		return nil, err
	}
	ino := &inode{}
	m.files[name] = ino
	return &memFile{m: m, ino: ino, gen: m.gen, name: name}, nil
}

func (m *Mem) open(op, name string, readOnly bool) (File, error) {
	name = path.Clean(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	ino, ok := m.files[name]
	if !ok {
		return nil, &fs.PathError{Op: op, Path: name, Err: fs.ErrNotExist}
	}
	return &memFile{m: m, ino: ino, gen: m.gen, name: name, readOnly: readOnly}, nil
}

func (m *Mem) OpenAppend(name string) (File, error) { return m.open("open", name, false) }

func (m *Mem) Open(name string) (File, error) { return m.open("open", name, true) }

func (m *Mem) Rename(oldname, newname string) error {
	oldname, newname = path.Clean(oldname), path.Clean(newname)
	m.mu.Lock()
	defer m.mu.Unlock()
	ino, ok := m.files[oldname]
	if !ok {
		return &fs.PathError{Op: "rename", Path: oldname, Err: fs.ErrNotExist}
	}
	if err := m.checkParent("rename", newname); err != nil {
		return err
	}
	delete(m.files, oldname)
	m.files[newname] = ino
	return nil
}

func (m *Mem) Remove(name string) error {
	name = path.Clean(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.files[name]; ok {
		delete(m.files, name)
		return nil
	}
	if m.dirs[name] {
		for p := range m.dirs {
			if p != name && path.Dir(p) == name {
				return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrExist}
			}
		}
		for p := range m.files {
			if path.Dir(p) == name {
				return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrExist}
			}
		}
		delete(m.dirs, name)
		return nil
	}
	return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrNotExist}
}

func (m *Mem) ReadDir(dir string) ([]string, error) {
	dir = path.Clean(dir)
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.dirs[dir] {
		return nil, &fs.PathError{Op: "readdir", Path: dir, Err: fs.ErrNotExist}
	}
	var names []string
	for p := range m.files {
		if path.Dir(p) == dir {
			names = append(names, path.Base(p))
		}
	}
	for p := range m.dirs {
		if p != dir && path.Dir(p) == dir {
			names = append(names, path.Base(p))
		}
	}
	slices.Sort(names)
	return names, nil
}

func (m *Mem) MkdirAll(dir string) error {
	dir = path.Clean(dir)
	m.mu.Lock()
	defer m.mu.Unlock()
	for p := dir; !m.dirs[p]; p = path.Dir(p) {
		if _, ok := m.files[p]; ok {
			return &fs.PathError{Op: "mkdir", Path: p, Err: fs.ErrExist}
		}
	}
	for p := dir; !m.dirs[p]; p = path.Dir(p) {
		m.dirs[p] = true
	}
	return nil
}

func (m *Mem) SyncDir(dir string) error {
	dir = path.Clean(dir)
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.dirs[dir] {
		return &fs.PathError{Op: "syncdir", Path: dir, Err: fs.ErrNotExist}
	}
	for p := range m.durable {
		if path.Dir(p) == dir {
			delete(m.durable, p)
		}
	}
	for p, ino := range m.files {
		if path.Dir(p) == dir {
			m.durable[p] = ino
		}
	}
	return nil
}

type memFile struct {
	m        *Mem
	ino      *inode
	gen      uint64
	name     string
	readOnly bool
	closed   bool
}

func (f *memFile) check(op string, write bool) error {
	switch {
	case f.closed:
		return &fs.PathError{Op: op, Path: f.name, Err: fs.ErrClosed}
	case f.gen != f.m.gen:
		return &fs.PathError{Op: op, Path: f.name, Err: ErrCrashed}
	case write && f.readOnly:
		return &fs.PathError{Op: op, Path: f.name, Err: fs.ErrPermission}
	}
	return nil
}

func (f *memFile) Write(p []byte) (int, error) {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if err := f.check("write", true); err != nil {
		return 0, err
	}
	off := len(f.ino.data)
	f.ino.data = append(f.ino.data, p...)
	f.m.last = lastWrite{ino: f.ino, off: off, n: len(p)}
	return len(p), nil
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if err := f.check("read", false); err != nil {
		return 0, err
	}
	if off < 0 {
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: fs.ErrInvalid}
	}
	if off >= int64(len(f.ino.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.ino.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *memFile) Sync() error {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if err := f.check("sync", false); err != nil {
		return err
	}
	d := f.ino.data
	f.ino.dur = d[:len(d):len(d)]
	if f.m.last.ino == f.ino {
		f.m.last = lastWrite{}
	}
	return nil
}

func (f *memFile) Truncate(size int64) error {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if err := f.check("truncate", true); err != nil {
		return err
	}
	if size < 0 {
		return &fs.PathError{Op: "truncate", Path: f.name, Err: fs.ErrInvalid}
	}
	ino := f.ino
	switch {
	case size < int64(len(ino.dur)):
		// Later appends would overwrite the durable bytes in place.
		ino.data = slices.Clone(ino.data[:size])
	case size <= int64(len(ino.data)):
		ino.data = ino.data[:size]
	default:
		ino.data = append(ino.data, make([]byte, size-int64(len(ino.data)))...)
	}
	if f.m.last.ino == ino {
		f.m.last = lastWrite{}
	}
	return nil
}

func (f *memFile) Size() (int64, error) {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if err := f.check("stat", false); err != nil {
		return 0, err
	}
	return int64(len(f.ino.data)), nil
}

func (f *memFile) Close() error {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if f.closed {
		return &fs.PathError{Op: "close", Path: f.name, Err: fs.ErrClosed}
	}
	f.closed = true
	return nil
}

// Synced returns a read-only view of the durable state of m: the entries made
// durable by SyncDir, each with the contents of its last Sync. It is how
// another process reads the disk of a node that may crash, as the evaluators
// of the simulation read the log of serve. Its handles keep working across
// Crash and see later syncs of the same file.
func (m *Mem) Synced() FS { return syncedView{m} }

type syncedView struct{ m *Mem }

func readOnlyErr(op, name string) error {
	return &fs.PathError{Op: op, Path: name, Err: fs.ErrPermission}
}

func (v syncedView) Create(name string) (File, error) { return nil, readOnlyErr("create", name) }

func (v syncedView) OpenAppend(name string) (File, error) { return nil, readOnlyErr("open", name) }

func (v syncedView) Open(name string) (File, error) {
	name = path.Clean(name)
	v.m.mu.Lock()
	defer v.m.mu.Unlock()
	ino, ok := v.m.durable[name]
	if !ok {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	return &syncedFile{m: v.m, ino: ino, name: name}, nil
}

func (v syncedView) Rename(oldname, newname string) error { return readOnlyErr("rename", oldname) }

func (v syncedView) Remove(name string) error { return readOnlyErr("remove", name) }

func (v syncedView) ReadDir(dir string) ([]string, error) {
	dir = path.Clean(dir)
	v.m.mu.Lock()
	defer v.m.mu.Unlock()
	if !v.m.dirs[dir] {
		return nil, &fs.PathError{Op: "readdir", Path: dir, Err: fs.ErrNotExist}
	}
	var names []string
	for p := range v.m.durable {
		if path.Dir(p) == dir {
			names = append(names, path.Base(p))
		}
	}
	for p := range v.m.dirs {
		if p != dir && path.Dir(p) == dir {
			names = append(names, path.Base(p))
		}
	}
	slices.Sort(names)
	return names, nil
}

func (v syncedView) MkdirAll(dir string) error { return readOnlyErr("mkdir", dir) }

func (v syncedView) SyncDir(dir string) error { return readOnlyErr("syncdir", dir) }

// syncedFile reads the durable prefix of an inode.
type syncedFile struct {
	m      *Mem
	ino    *inode
	name   string
	closed bool
}

func (f *syncedFile) check(op string) error {
	if f.closed {
		return &fs.PathError{Op: op, Path: f.name, Err: fs.ErrClosed}
	}
	return nil
}

func (f *syncedFile) Write(p []byte) (int, error) { return 0, readOnlyErr("write", f.name) }

func (f *syncedFile) ReadAt(p []byte, off int64) (int, error) {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if err := f.check("read"); err != nil {
		return 0, err
	}
	if off < 0 {
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: fs.ErrInvalid}
	}
	dur := f.ino.dur
	if off >= int64(len(dur)) {
		return 0, io.EOF
	}
	n := copy(p, dur[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Sync has nothing to make durable, as on a read-only handle of Mem.
func (f *syncedFile) Sync() error {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	return f.check("sync")
}

func (f *syncedFile) Truncate(size int64) error { return readOnlyErr("truncate", f.name) }

func (f *syncedFile) Size() (int64, error) {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if err := f.check("stat"); err != nil {
		return 0, err
	}
	return int64(len(f.ino.dur)), nil
}

func (f *syncedFile) Close() error {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if err := f.check("close"); err != nil {
		return err
	}
	f.closed = true
	return nil
}
