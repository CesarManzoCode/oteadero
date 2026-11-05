package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"testing"

	"github.com/CesarManzoCode/oteadero/internal/rng"
	"github.com/CesarManzoCode/oteadero/internal/vfs"
)

const dir = "data/log"

// batchPayload builds a BATCH payload with a valid §4.3 header for n events
// followed by opaque filler; the event encoding belongs to internal/event.
func batchPayload(tenant string, n uint64, filler []byte) []byte {
	b := binary.AppendUvarint(nil, uint64(len(tenant)))
	b = append(b, tenant...)
	b = append(b, 1)
	b = binary.LittleEndian.AppendUint64(b, 1_700_000_000_000)
	b = binary.AppendUvarint(b, n)
	return append(b, filler...)
}

// entry is a record as the test appended it.
type entry struct {
	typ     byte
	payload []byte
	events  uint64
}

func appendEntry(t *testing.T, l *Log, e entry) {
	t.Helper()
	var err error
	switch e.typ {
	case TypeBatch:
		_, err = l.AppendBatch(e.payload, e.events)
	case TypeTick:
		var ts int64
		ts, err = DecodeTick(e.payload)
		if err == nil {
			err = l.AppendTick(ts)
		}
	case TypeLease:
		var lease Lease
		lease, err = DecodeLease(e.payload)
		if err == nil {
			err = l.AppendLease(lease)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
}

func randomEntry(r rng.Source, i int) entry {
	switch r.IntN(4) {
	case 0:
		return entry{typ: TypeTick, payload: EncodeTick(int64(i) * 1000)}
	case 1:
		return entry{typ: TypeLease, payload: EncodeLease(Lease{Epoch: uint64(i), Holder: fmt.Sprintf("e%d", i%3+1), ExpiresMs: int64(i) + 2000})}
	}
	n := uint64(r.IntN(5))
	filler := make([]byte, r.IntN(40))
	for j := range filler {
		filler[j] = byte(r.Uint64())
	}
	return entry{typ: TypeBatch, payload: batchPayload(fmt.Sprintf("t%d", i%4), n, filler), events: n}
}

func start(base uint64) Pos { return Pos{Segment: base, Byte: HeaderSize, Next: base} }

// readFrom reads every record from p until io.EOF.
func readFrom(t *testing.T, fsys vfs.FS, p Pos) []Record {
	t.Helper()
	r, err := OpenReader(fsys, dir, p)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var out []Record
	for {
		rec, err := r.Next()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		rec.Payload = slices.Clone(rec.Payload)
		out = append(out, rec)
	}
}

// checkRecords compares read records with the appended entries and checks
// consecutive offsets from zero.
func checkRecords(t *testing.T, got []Record, want []entry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("read %d records, want %d", len(got), len(want))
	}
	var next uint64
	for i, rec := range got {
		w := want[i]
		if rec.Type != w.typ || !bytes.Equal(rec.Payload, w.payload) || rec.Events != w.events {
			t.Fatalf("record %d = type %d events %d %x, want type %d events %d %x", i, rec.Type, rec.Events, rec.Payload, w.typ, w.events, w.payload)
		}
		if rec.Offset != next {
			t.Fatalf("record %d offset %d, want %d", i, rec.Offset, next)
		}
		next += rec.Events
	}
}

func eventsOf(es []entry) uint64 {
	var n uint64
	for _, e := range es {
		n += e.events
	}
	return n
}

func TestRoundTripThreeTypes(t *testing.T) {
	m := vfs.NewMem()
	l, rec, err := Open(m, dir, rng.New(1), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !rec.Created || rec.NextOffset != 0 || rec.TruncatedBytes != 0 {
		t.Fatalf("recovery of new log = %+v", rec)
	}
	lease := Lease{Epoch: 7, Holder: "e2", ExpiresMs: 1_700_000_002_000}
	want := []entry{
		{typ: TypeBatch, payload: batchPayload("acme", 3, []byte("abc")), events: 3},
		{typ: TypeTick, payload: EncodeTick(1_700_000_001_000)},
		{typ: TypeLease, payload: EncodeLease(lease)},
		{typ: TypeBatch, payload: batchPayload("beta", 2, nil), events: 2},
	}
	for _, e := range want {
		appendEntry(t, l, e)
	}
	if l.Pending() == 0 {
		t.Fatal("nothing pending before Sync")
	}
	if got := readFrom(t, m, start(0)); len(got) != 0 {
		t.Fatalf("read %d records before Sync", len(got))
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if l.Pending() != 0 || l.NextOffset() != 5 {
		t.Fatalf("after Sync pending=%d next=%d", l.Pending(), l.NextOffset())
	}
	got := readFrom(t, m, start(0))
	checkRecords(t, got, want)
	if ts, err := DecodeTick(got[1].Payload); err != nil || ts != 1_700_000_001_000 {
		t.Fatalf("tick = %d %v", ts, err)
	}
	if dl, err := DecodeLease(got[2].Payload); err != nil || dl != lease {
		t.Fatalf("lease = %+v %v", dl, err)
	}
	if end := l.End(); end.Next != 5 || got[3].Pos.Byte >= end.Byte {
		t.Fatalf("End = %+v", end)
	}

	// The segment holds the exact §4.3 bytes.
	f, _ := m.Open(filepath.Join(dir, "00000000000000000000.seg"))
	head := make([]byte, HeaderSize+frameSize+1)
	f.ReadAt(head, 0)
	if string(head[:8]) != "OTLOG001" || binary.LittleEndian.Uint64(head[8:16]) != l.ID() {
		t.Fatalf("header = %x", head[:16])
	}
	if n := binary.LittleEndian.Uint32(head[16:20]); n != uint32(1+len(want[0].payload)) {
		t.Fatalf("len = %d", n)
	}
	if head[24] != TypeBatch {
		t.Fatalf("type = %d", head[24])
	}
}

func TestAppendBatchCountMismatch(t *testing.T) {
	l, _, err := Open(vfs.NewMem(), dir, rng.New(1), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.AppendBatch(batchPayload("a", 3, nil), 2); err == nil {
		t.Fatal("mismatched event count accepted")
	}
	if _, err := l.AppendBatch([]byte{0xff}, 0); err == nil {
		t.Fatal("malformed batch accepted")
	}
}

func TestLogIDFromRng(t *testing.T) {
	a, _, _ := Open(vfs.NewMem(), dir, rng.New(5), Options{})
	b, _, _ := Open(vfs.NewMem(), dir, rng.New(5), Options{})
	c, _, _ := Open(vfs.NewMem(), dir, rng.New(6), Options{})
	if a.ID() != b.ID() || a.ID() == c.ID() {
		t.Fatalf("ids %x %x %x", a.ID(), b.ID(), c.ID())
	}
}

func TestRotationKeepsOffsetsConsecutive(t *testing.T) {
	m := vfs.NewMem()
	l, _, err := Open(m, dir, rng.New(2), Options{SegmentBytes: 200})
	if err != nil {
		t.Fatal(err)
	}
	r := rng.New(3)
	var want []entry
	for i := range 300 {
		e := randomEntry(r, i)
		appendEntry(t, l, e)
		want = append(want, e)
		if r.IntN(3) == 0 {
			if err := l.Sync(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	bases, err := listSegments(m, dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(bases) < 10 {
		t.Fatalf("only %d segments with 200-byte rotation", len(bases))
	}
	got := readFrom(t, m, start(0))
	checkRecords(t, got, want)

	// Each segment is named after the first event it holds and every
	// segment carries the same log id.
	firstIn := map[uint64]uint64{}
	for _, rec := range got {
		if _, ok := firstIn[rec.Pos.Segment]; !ok {
			firstIn[rec.Pos.Segment] = rec.Offset
		}
	}
	for _, b := range bases {
		if first, ok := firstIn[b]; ok && first != b {
			t.Fatalf("segment %d starts at offset %d", b, first)
		}
		f, _ := m.Open(filepath.Join(dir, segName(b)))
		id, err := readSegmentHeader(f)
		f.Close()
		if err != nil || id != l.ID() {
			t.Fatalf("segment %d id %x %v, want %x", b, id, err, l.ID())
		}
	}

	l2, rec, err := Open(m, dir, rng.New(99), Options{SegmentBytes: 200})
	if err != nil {
		t.Fatal(err)
	}
	if rec.Created || rec.TruncatedBytes != 0 || rec.NextOffset != eventsOf(want) || l2.ID() != l.ID() {
		t.Fatalf("reopen = %+v id %x", rec, l2.ID())
	}
}

func TestReopenAndAppend(t *testing.T) {
	m := vfs.NewMem()
	l, _, _ := Open(m, dir, rng.New(1), Options{})
	a := entry{typ: TypeBatch, payload: batchPayload("a", 4, nil), events: 4}
	appendEntry(t, l, a)
	l.Close()

	l, rec, err := Open(m, dir, rng.New(1), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if rec.NextOffset != 4 || rec.TruncatedBytes != 0 || rec.Created {
		t.Fatalf("recovery = %+v", rec)
	}
	b := entry{typ: TypeBatch, payload: batchPayload("b", 2, nil), events: 2}
	first, err := l.AppendBatch(b.payload, 2)
	if err != nil || first != 4 {
		t.Fatalf("first = %d %v", first, err)
	}
	l.Sync()
	checkRecords(t, readFrom(t, m, start(0)), []entry{a, b})
}

func TestReadFromOffset(t *testing.T) {
	m := vfs.NewMem()
	l, _, _ := Open(m, dir, rng.New(4), Options{SegmentBytes: 256})
	r := rng.New(8)
	var want []entry
	for i := range 200 {
		e := randomEntry(r, i)
		appendEntry(t, l, e)
		want = append(want, e)
	}
	l.Sync()
	total := eventsOf(want)
	all := readFrom(t, m, start(0))

	for o := range total + 1 {
		rd, err := OpenReaderAt(m, dir, o)
		if err != nil {
			t.Fatalf("offset %d: %v", o, err)
		}
		rec, err := rd.Next()
		if o == total {
			if err != io.EOF {
				t.Fatalf("offset %d at end: %v", o, err)
			}
			rd.Close()
			continue
		}
		if err != nil {
			t.Fatalf("offset %d: %v", o, err)
		}
		if rec.Type != TypeBatch || rec.Offset > o || o >= rec.Offset+rec.Events {
			t.Fatalf("offset %d: got record type %d [%d,%d)", o, rec.Type, rec.Offset, rec.Offset+rec.Events)
		}
		// The rest of the log follows in order.
		i := slices.IndexFunc(all, func(a Record) bool { return a.Pos == rec.Pos })
		for _, w := range all[i+1:] {
			got, err := rd.Next()
			if err != nil || got.Pos != w.Pos || !bytes.Equal(got.Payload, w.Payload) {
				t.Fatalf("offset %d: after start read %+v %v, want %+v", o, got.Pos, err, w.Pos)
			}
		}
		if _, err := rd.Next(); err != io.EOF {
			t.Fatalf("offset %d: no EOF at end: %v", o, err)
		}
		rd.Close()
	}
	if _, err := OpenReaderAt(m, dir, total+1); err == nil {
		t.Fatal("offset past the end accepted")
	}
}

func TestReaderFollowsWriter(t *testing.T) {
	m := vfs.NewMem()
	l, _, _ := Open(m, dir, rng.New(4), Options{SegmentBytes: 128})
	rd, err := OpenReader(m, dir, l.End())
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	r := rng.New(10)
	var next uint64
	for i := range 100 {
		e := randomEntry(r, i)
		appendEntry(t, l, e)
		l.Sync()
		rec, err := rd.Next()
		if err != nil || rec.Type != e.typ || rec.Offset != next || !bytes.Equal(rec.Payload, e.payload) {
			t.Fatalf("step %d: %+v %v", i, rec, err)
		}
		next += e.events
		if _, err := rd.Next(); err != io.EOF {
			t.Fatalf("step %d: expected EOF, got %v", i, err)
		}
	}
	if rd.Pos() != l.End() {
		t.Fatalf("reader at %+v, log ends at %+v", rd.Pos(), l.End())
	}
}

// rewrite replaces the last segment with f(contents) durably.
func rewrite(t *testing.T, m *vfs.Mem, f func([]byte) []byte) string {
	t.Helper()
	bases, err := listSegments(m, dir)
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(dir, segName(bases[len(bases)-1]))
	in, _ := m.Open(name)
	size, _ := in.Size()
	data := make([]byte, size)
	in.ReadAt(data, 0)
	in.Close()
	out, err := m.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	out.Write(f(data))
	out.Sync()
	out.Close()
	m.SyncDir(dir)
	return name
}

// threeRecords writes three synced BATCH records and returns them with the
// size in bytes of each framed record.
func threeRecords(t *testing.T) (*vfs.Mem, []entry, []int) {
	m := vfs.NewMem()
	l, _, _ := Open(m, dir, rng.New(1), Options{})
	es := []entry{
		{typ: TypeBatch, payload: batchPayload("a", 1, []byte("one")), events: 1},
		{typ: TypeBatch, payload: batchPayload("a", 2, []byte("two")), events: 2},
		{typ: TypeBatch, payload: batchPayload("a", 3, []byte("three")), events: 3},
	}
	var sizes []int
	for _, e := range es {
		appendEntry(t, l, e)
		sizes = append(sizes, frameSize+1+len(e.payload))
	}
	l.Close()
	return m, es, sizes
}

func reopen(t *testing.T, m *vfs.Mem) Recovery {
	t.Helper()
	l, rec, err := Open(m, dir, rng.New(1), Options{})
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
	return rec
}

func TestRecoveryTornLastRecord(t *testing.T) {
	m, es, sizes := threeRecords(t)
	rewrite(t, m, func(b []byte) []byte { return b[:len(b)-3] })
	rec := reopen(t, m)
	if rec.TruncatedBytes != int64(sizes[2]-3) || rec.NextOffset != 3 {
		t.Fatalf("recovery = %+v", rec)
	}
	checkRecords(t, readFrom(t, m, start(0)), es[:2])
}

func TestRecoveryTornFrame(t *testing.T) {
	m, es, sizes := threeRecords(t)
	rewrite(t, m, func(b []byte) []byte { return b[:len(b)-sizes[2]+5] })
	rec := reopen(t, m)
	if rec.TruncatedBytes != 5 || rec.NextOffset != 3 {
		t.Fatalf("recovery = %+v", rec)
	}
	checkRecords(t, readFrom(t, m, start(0)), es[:2])
}

func TestRecoveryBadCRC(t *testing.T) {
	for _, tc := range []struct {
		name   string
		record int
	}{{"last", 2}, {"middle", 1}} {
		t.Run(tc.name, func(t *testing.T) {
			m, es, sizes := threeRecords(t)
			at := HeaderSize
			for _, s := range sizes[:tc.record] {
				at += s
			}
			rewrite(t, m, func(b []byte) []byte { b[at+frameSize+3] ^= 0x40; return b })
			// Reading the damaged log reports corruption.
			r, _ := OpenReader(m, dir, start(0))
			var err error
			for err == nil {
				_, err = r.Next()
			}
			r.Close()
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("reader on bad CRC: %v", err)
			}
			rec := reopen(t, m)
			var cut int64
			for _, s := range sizes[tc.record:] {
				cut += int64(s)
			}
			if rec.TruncatedBytes != cut || rec.NextOffset != eventsOf(es[:tc.record]) {
				t.Fatalf("recovery = %+v", rec)
			}
			checkRecords(t, readFrom(t, m, start(0)), es[:tc.record])
		})
	}
}

func TestRecoveryImpossibleLen(t *testing.T) {
	for _, n := range []uint32{0, 1 << 30} {
		m, es, sizes := threeRecords(t)
		at := HeaderSize + sizes[0] + sizes[1]
		rewrite(t, m, func(b []byte) []byte { binary.LittleEndian.PutUint32(b[at:], n); return b })
		rec := reopen(t, m)
		if rec.TruncatedBytes != int64(sizes[2]) || rec.NextOffset != 3 {
			t.Fatalf("len %d: recovery = %+v", n, rec)
		}
		checkRecords(t, readFrom(t, m, start(0)), es[:2])
	}
}

// TestCrashRecoveryKeepsSyncedPrefix crashes Mem at seeded points with a
// small segment size. Recovery must always leave a valid prefix of the
// appended records that contains every synced one, with consecutive offsets,
// and the log must accept new records afterwards.
func TestCrashRecoveryKeepsSyncedPrefix(t *testing.T) {
	const segBytes = 300
	torn := 0
	for seed := range uint64(200) {
		r := rng.New(seed)
		m := vfs.NewMem()
		ffs := &faultFS{FS: m}
		l, _, err := Open(ffs, dir, r, Options{SegmentBytes: segBytes})
		if err != nil {
			t.Fatal(err)
		}
		var appended []entry
		synced := 0
		steps := 1 + r.IntN(60)
		for i := range steps {
			e := randomEntry(r, i)
			appendEntry(t, l, e)
			appended = append(appended, e)
			if r.IntN(4) == 0 {
				if err := l.Sync(); err != nil {
					t.Fatal(err)
				}
				synced = len(appended)
			}
		}
		// Half of the runs crash inside the final Sync, after the write and
		// before the fsync, so the unsynced write may be left torn.
		if r.IntN(2) == 0 {
			ffs.failSync = true
			if err := l.Sync(); err == nil {
				t.Fatalf("seed %d: Sync succeeded with failing fsync", seed)
			}
		}
		// Rotation syncs the segment it closes; those records count as
		// synced too, so only the floor is checked against explicit Syncs.
		m.Crash(r)

		l2, rec, err := Open(m, dir, r, Options{SegmentBytes: segBytes})
		if err != nil {
			t.Fatalf("seed %d: reopen: %v", seed, err)
		}
		got := readFrom(t, m, start(0))
		if len(got) < synced || len(got) > len(appended) {
			t.Fatalf("seed %d: recovered %d records, synced %d, appended %d", seed, len(got), synced, len(appended))
		}
		checkRecords(t, got, appended[:len(got)])
		if rec.NextOffset != eventsOf(appended[:len(got)]) || l2.NextOffset() != rec.NextOffset {
			t.Fatalf("seed %d: next offset %d, want %d", seed, rec.NextOffset, eventsOf(appended[:len(got)]))
		}
		if rec.TruncatedBytes > 0 {
			torn++
		}
		if l2.ID() != l.ID() {
			t.Fatalf("seed %d: log id changed", seed)
		}

		e := entry{typ: TypeBatch, payload: batchPayload("after", 2, nil), events: 2}
		appendEntry(t, l2, e)
		if err := l2.Sync(); err != nil {
			t.Fatal(err)
		}
		checkRecords(t, readFrom(t, m, start(0)), append(appended[:len(got):len(got)], e))
	}
	if torn == 0 {
		t.Fatal("no seed left a torn record to truncate")
	}
}

var errSyncFailed = errors.New("injected fsync failure")

// faultFS makes every fsync of a file fail once failSync is set, which
// models a crash between a write and its fsync.
type faultFS struct {
	vfs.FS
	failSync bool
}

type faultFile struct {
	vfs.File
	fs *faultFS
}

func (f *faultFile) Sync() error {
	if f.fs.failSync {
		return errSyncFailed
	}
	return f.File.Sync()
}

func (f *faultFS) Create(name string) (vfs.File, error) {
	file, err := f.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return &faultFile{File: file, fs: f}, nil
}

func (f *faultFS) OpenAppend(name string) (vfs.File, error) {
	file, err := f.FS.OpenAppend(name)
	if err != nil {
		return nil, err
	}
	return &faultFile{File: file, fs: f}, nil
}

func TestSegmentsAndRemoveSegment(t *testing.T) {
	fsys := vfs.NewMem()
	l, _, err := Open(fsys, dir, rng.New(1), Options{SegmentBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	for range 8 {
		if _, err := l.AppendBatch(batchPayload("a", 1, make([]byte, 64)), 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	bases, err := Segments(fsys, dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(bases) < 2 || bases[0] != 0 {
		t.Fatalf("segments = %v", bases)
	}
	if err := RemoveSegment(fsys, dir, bases[0]); err != nil {
		t.Fatal(err)
	}
	after, err := Segments(fsys, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(after, bases[1:]) {
		t.Fatalf("segments = %v, want %v", after, bases[1:])
	}
}
