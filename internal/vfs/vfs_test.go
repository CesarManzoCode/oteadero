package vfs

import (
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"slices"
	"testing"

	"github.com/CesarManzoCode/oteadero/internal/rng"
)

func readAll(t *testing.T, fsys FS, name string) []byte {
	t.Helper()
	f, err := fsys.Open(name)
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, 0); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	return buf
}

func mustWrite(t *testing.T, f File, s string) {
	t.Helper()
	if _, err := f.Write([]byte(s)); err != nil {
		t.Fatal(err)
	}
}

// exercise runs the same operations on any FS.
func exercise(t *testing.T, fsys FS, root string) {
	dir := filepath.Join(root, "a", "b")
	if err := fsys.MkdirAll(dir); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(dir, "x")
	f, err := fsys.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, f, "hello ")
	mustWrite(t, f, "world")
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if size, _ := f.Size(); size != 11 {
		t.Fatalf("size = %d", size)
	}
	buf := make([]byte, 5)
	if n, err := f.ReadAt(buf, 6); n != 5 || err != nil || string(buf) != "world" {
		t.Fatalf("ReadAt = %d %v %q", n, err, buf)
	}
	if n, err := f.ReadAt(buf, 9); n != 2 || err != io.EOF {
		t.Fatalf("short ReadAt = %d %v", n, err)
	}
	if err := f.Truncate(5); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, f, "!")
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if got := string(readAll(t, fsys, name)); got != "hello!" {
		t.Fatalf("after truncate and append: %q", got)
	}

	f, err = fsys.OpenAppend(name)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, f, "?")
	f.Close()
	if got := string(readAll(t, fsys, name)); got != "hello!?" {
		t.Fatalf("after OpenAppend: %q", got)
	}

	ro, err := fsys.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ro.Write([]byte("no")); err == nil {
		t.Fatal("write on read-only file succeeded")
	}
	ro.Close()

	other := filepath.Join(dir, "w")
	if err := fsys.Rename(name, other); err != nil {
		t.Fatal(err)
	}
	if err := fsys.SyncDir(dir); err != nil {
		t.Fatal(err)
	}
	names, err := fsys.ReadDir(dir)
	if err != nil || !slices.Equal(names, []string{"w"}) {
		t.Fatalf("ReadDir = %v %v", names, err)
	}
	if _, err := fsys.Open(name); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("open renamed-away name: %v", err)
	}
	if _, err := fsys.Create(filepath.Join(root, "missing", "y")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("create in missing dir: %v", err)
	}
	if err := fsys.Remove(other); err != nil {
		t.Fatal(err)
	}
	names, _ = fsys.ReadDir(dir)
	if len(names) != 0 {
		t.Fatalf("ReadDir after remove = %v", names)
	}
	if err := fsys.Remove(dir); err != nil {
		t.Fatal(err)
	}
	names, _ = fsys.ReadDir(filepath.Join(root, "a"))
	if len(names) != 0 {
		t.Fatalf("ReadDir after removing dir = %v", names)
	}
}

func TestOS(t *testing.T) { exercise(t, OS{}, t.TempDir()) }

func TestMem(t *testing.T) { exercise(t, NewMem(), "data") }

func TestMemReadDirSorted(t *testing.T) {
	m := NewMem()
	m.MkdirAll("d/sub")
	for _, n := range []string{"c", "a", "b"} {
		if _, err := m.Create("d/" + n); err != nil {
			t.Fatal(err)
		}
	}
	names, err := m.ReadDir("d")
	if err != nil || !slices.Equal(names, []string{"a", "b", "c", "sub"}) {
		t.Fatalf("ReadDir = %v %v", names, err)
	}
	if err := m.Remove("d"); err == nil {
		t.Fatal("removed non-empty dir")
	}
}

// durableFile creates name with contents synced and its entry synced.
func durableFile(t *testing.T, m *Mem, name, contents string) File {
	t.Helper()
	f, err := m.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, f, contents)
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := m.SyncDir(filepath.Dir(name)); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestMemCrashLosesUnsyncedTail(t *testing.T) {
	sawNone, sawPartial, sawAll := false, false, false
	for seed := range uint64(64) {
		m := NewMem()
		m.MkdirAll("d")
		f := durableFile(t, m, "d/f", "abc")
		mustWrite(t, f, "defgh")
		m.Crash(rng.New(seed))
		got := string(readAll(t, m, "d/f"))
		full := "abcdefgh"
		if len(got) < 3 || got != full[:len(got)] {
			t.Fatalf("seed %d: after crash %q is not a durable prefix plus part of the last write", seed, got)
		}
		switch len(got) {
		case 3:
			sawNone = true
		case len(full):
			sawAll = true
		default:
			sawPartial = true
		}
		if _, err := f.Write([]byte("x")); !errors.Is(err, ErrCrashed) {
			t.Fatalf("old handle after crash: %v", err)
		}
	}
	if !sawNone || !sawPartial || !sawAll {
		t.Fatalf("tear outcomes none=%v partial=%v all=%v", sawNone, sawPartial, sawAll)
	}
}

func TestMemCrashOnlyTearsLastWrite(t *testing.T) {
	for seed := range uint64(32) {
		m := NewMem()
		m.MkdirAll("d")
		f := durableFile(t, m, "d/f", "abc")
		g := durableFile(t, m, "d/g", "123")
		mustWrite(t, f, "lost")
		mustWrite(t, g, "456")
		m.Crash(rng.New(seed))
		if got := string(readAll(t, m, "d/f")); got != "abc" {
			t.Fatalf("seed %d: f = %q, earlier unsynced write survived", seed, got)
		}
		if got := string(readAll(t, m, "d/g")); got != "123456"[:len(got)] || len(got) < 3 {
			t.Fatalf("seed %d: g = %q", seed, got)
		}
	}
}

func TestMemCrashSyncedWriteIsKept(t *testing.T) {
	m := NewMem()
	m.MkdirAll("d")
	f := durableFile(t, m, "d/f", "abc")
	mustWrite(t, f, "def")
	f.Sync()
	m.Crash(rng.New(1))
	if got := string(readAll(t, m, "d/f")); got != "abcdef" {
		t.Fatalf("synced data lost: %q", got)
	}
}

func TestMemCrashDirectoryEntries(t *testing.T) {
	m := NewMem()
	m.MkdirAll("d")
	durableFile(t, m, "d/old", "1")
	f, _ := m.Create("d/new")
	mustWrite(t, f, "2")
	f.Sync()
	if err := m.Rename("d/old", "d/renamed"); err != nil {
		t.Fatal(err)
	}
	m.Crash(rng.New(3))
	names, _ := m.ReadDir("d")
	if !slices.Equal(names, []string{"old"}) {
		t.Fatalf("entries after crash without SyncDir = %v", names)
	}

	f, _ = m.Create("d/new")
	mustWrite(t, f, "2")
	f.Sync()
	m.Rename("d/old", "d/renamed")
	m.SyncDir("d")
	m.Crash(rng.New(3))
	names, _ = m.ReadDir("d")
	if !slices.Equal(names, []string{"new", "renamed"}) {
		t.Fatalf("entries after crash with SyncDir = %v", names)
	}
	if got := string(readAll(t, m, "d/renamed")); got != "1" {
		t.Fatalf("renamed contents = %q", got)
	}
}

func TestMemCrashRevertsUnsyncedTruncate(t *testing.T) {
	m := NewMem()
	m.MkdirAll("d")
	f := durableFile(t, m, "d/f", "abcdef")
	if err := f.Truncate(2); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, f, "XY")
	if got := string(readAll(t, m, "d/f")); got != "abXY" {
		t.Fatalf("live contents = %q", got)
	}
	m.Crash(rng.New(9))
	if got := string(readAll(t, m, "d/f")); got != "abcdef" {
		t.Fatalf("after crash = %q", got)
	}

	f, _ = m.OpenAppend("d/f")
	f.Truncate(2)
	f.Sync()
	m.Crash(rng.New(9))
	if got := string(readAll(t, m, "d/f")); got != "ab" {
		t.Fatalf("after synced truncate and crash = %q", got)
	}
}
