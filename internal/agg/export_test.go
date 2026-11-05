package agg

import (
	"io"
	"math"
	"testing"

	"github.com/CesarManzoCode/oteadero/internal/rng"
	"github.com/CesarManzoCode/oteadero/internal/vfs"
)

func readFile(t *testing.T, fsys vfs.FS, name string) string {
	t.Helper()
	f, err := fsys.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, size)
	if _, err := f.ReadAt(data, 0); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	return string(data)
}

const (
	bucketsPath = "exp/serve-r1/buckets.jsonl"
	latePath    = "exp/serve-r1/late.jsonl"
)

func TestExportLinesAndFlush(t *testing.T) {
	mem := vfs.NewMem()
	e, err := NewExport(mem, "exp", "r1")
	if err != nil {
		t.Fatal(err)
	}
	e.Closed(&Bucket{Tenant: "a", Series: `m{x="1"}`, End: 2000, Count: 2, Sum: 1.5, Rate: 1.5, ClosedAt: 7})
	e.Late("a", 3)
	if got := readFile(t, mem, bucketsPath); got != "" {
		t.Fatalf("written before the flush: %q", got)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	wantBuckets := `{"tenant":"a","series":"m{x=\"1\"}","end":2000,"count":2,"sum":1.5,"rate":1.5,"closed_at":7}` + "\n"
	wantLate := `{"o":3}` + "\n"

	// What was flushed survives a crash; what was not does not.
	e.Late("a", 4)
	mem.Crash(rng.New(1))
	if got := readFile(t, mem, bucketsPath); got != wantBuckets {
		t.Fatalf("buckets = %q, want %q", got, wantBuckets)
	}
	if got := readFile(t, mem, latePath); got != wantLate {
		t.Fatalf("late = %q, want %q", got, wantLate)
	}
}

func TestExportCloseFlushes(t *testing.T) {
	mem := vfs.NewMem()
	e, err := NewExport(mem, "exp", "r1")
	if err != nil {
		t.Fatal(err)
	}
	e.Closed(&Bucket{Tenant: "a", Series: "m{}", Sum: math.NaN(), Rate: math.Inf(-1)})
	e.Closed(&Bucket{Tenant: "a", Series: "m{}", Sum: 1e21, Rate: math.Inf(1)})
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	want := `{"tenant":"a","series":"m{}","end":0,"count":0,"sum":NaN,"rate":-Infinity,"closed_at":0}` + "\n" +
		`{"tenant":"a","series":"m{}","end":0,"count":0,"sum":1e+21,"rate":Infinity,"closed_at":0}` + "\n"
	if got := readFile(t, mem, bucketsPath); got != want {
		t.Fatalf("buckets = %q, want %q", got, want)
	}
}

func TestExportSpillsLargeBuffer(t *testing.T) {
	mem := vfs.NewMem()
	e, err := NewExport(mem, "exp", "r1")
	if err != nil {
		t.Fatal(err)
	}
	for o := uint64(0); len(e.late.buf) < exportBufBytes-32; o++ {
		e.Late("a", o)
	}
	if readFile(t, mem, latePath) != "" {
		t.Fatal("spilled before the buffer was full")
	}
	for range 32 {
		e.Late("a", 1)
	}
	if readFile(t, mem, latePath) == "" {
		t.Fatal("a full buffer was not written")
	}
}
