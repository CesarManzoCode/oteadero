package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
	"slices"

	"github.com/CesarManzoCode/oteadero/internal/rng"
	"github.com/CesarManzoCode/oteadero/internal/vfs"
)

var (
	// errIncomplete: the record extends past the end of the file.
	errIncomplete = errors.New("wal: incomplete record")
	// errBadRecord: impossible len or wrong CRC.
	errBadRecord = errors.New("wal: bad record")
)

// readRecord reads the record starting at off of a file holding size bytes.
// The payload aliases buf, which is grown as needed and returned.
func readRecord(f vfs.File, off, size int64, buf []byte) (typ byte, payload []byte, end int64, nbuf []byte, err error) {
	if size-off < frameSize {
		return 0, nil, 0, buf, errIncomplete
	}
	var frame [frameSize]byte
	if _, err := f.ReadAt(frame[:], off); err != nil {
		return 0, nil, 0, buf, err
	}
	n := int64(binary.LittleEndian.Uint32(frame[:4]))
	crc := binary.LittleEndian.Uint32(frame[4:])
	if n < 1 {
		return 0, nil, 0, buf, errBadRecord
	}
	if n > size-off-frameSize {
		return 0, nil, 0, buf, errIncomplete
	}
	if int64(cap(buf)) < n {
		buf = make([]byte, n)
	}
	body := buf[:n]
	if _, err := f.ReadAt(body, off+frameSize); err != nil {
		return 0, nil, 0, buf, err
	}
	if crc32.ChecksumIEEE(body) != crc {
		return 0, nil, 0, buf, errBadRecord
	}
	return body[0], body[1:], off + frameSize + n, buf, nil
}

// recordEvents returns how many offsets a record consumes.
func recordEvents(typ byte, payload []byte) (uint64, error) {
	switch typ {
	case TypeBatch:
		return BatchEvents(payload)
	case TypeTick, TypeLease:
		return 0, nil
	}
	return 0, fmt.Errorf("%w: unknown record type %d", ErrCorrupt, typ)
}

func listSegments(fsys vfs.FS, dir string) ([]uint64, error) {
	names, err := fsys.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var bases []uint64
	for _, n := range names {
		if b, ok := parseSegName(n); ok {
			bases = append(bases, b)
		}
	}
	slices.Sort(bases)
	return bases, nil
}

func readSegmentHeader(f vfs.File) (uint64, error) {
	var h [HeaderSize]byte
	if _, err := f.ReadAt(h[:], 0); err != nil {
		if err == io.EOF {
			return 0, fmt.Errorf("%w: short segment header", ErrCorrupt)
		}
		return 0, err
	}
	return parseHeader(h[:])
}

// Options configures a Log.
type Options struct {
	// SegmentBytes is the rotation size; zero means DefaultSegmentBytes.
	SegmentBytes int64
}

// Recovery reports what Open found.
type Recovery struct {
	// Created is true when the log did not exist and was created.
	Created bool
	// TruncatedBytes is how many bytes recovery cut from the end of the log.
	TruncatedBytes int64
	// NextOffset is the offset the next event will receive.
	NextOffset uint64
}

// Log appends records to the log. Appended records are buffered and become
// durable only when Sync returns; the caller decides when to Sync (group
// commit, D05). A Log is not safe for concurrent use.
type Log struct {
	fsys     vfs.FS
	dir      string
	segBytes int64
	id       uint64

	f        vfs.File
	segBase  uint64
	segSize  int64 // bytes already written to f
	buf      []byte
	next     uint64
	dirDirty bool
	synced   Pos
	err      error
}

// Open opens the log in dir, creating it with a random id from r when it
// has no segments. It recovers the last segment: the first record with an
// impossible len or a wrong CRC and everything after it are truncated.
func Open(fsys vfs.FS, dir string, r rng.Source, opts Options) (*Log, Recovery, error) {
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = DefaultSegmentBytes
	}
	l := &Log{fsys: fsys, dir: dir, segBytes: opts.SegmentBytes}
	var rec Recovery
	if err := fsys.MkdirAll(dir); err != nil {
		return nil, rec, err
	}
	bases, err := listSegments(fsys, dir)
	if err != nil {
		return nil, rec, err
	}

	// A last segment shorter than its header was torn while being created;
	// it holds no record, so it is dropped.
	for len(bases) > 0 {
		last := bases[len(bases)-1]
		f, err := fsys.Open(l.path(last))
		if err != nil {
			return nil, rec, err
		}
		size, err := f.Size()
		f.Close()
		if err != nil {
			return nil, rec, err
		}
		if size >= HeaderSize {
			break
		}
		if err := fsys.Remove(l.path(last)); err != nil {
			return nil, rec, err
		}
		if err := fsys.SyncDir(dir); err != nil {
			return nil, rec, err
		}
		rec.TruncatedBytes += size
		bases = bases[:len(bases)-1]
	}

	if len(bases) == 0 {
		l.id = r.Uint64()
		if err := l.createSegment(0); err != nil {
			return nil, rec, err
		}
		if err := l.Sync(); err != nil {
			return nil, rec, err
		}
		rec.Created = true
		return l, rec, nil
	}

	if err := l.recoverLast(bases, &rec); err != nil {
		return nil, rec, err
	}
	rec.NextOffset = l.next
	return l, rec, nil
}

func (l *Log) path(base uint64) string {
	return filepath.Join(l.dir, segName(base))
}

func (l *Log) recoverLast(bases []uint64, rec *Recovery) error {
	if len(bases) > 1 {
		f, err := l.fsys.Open(l.path(bases[0]))
		if err != nil {
			return err
		}
		id, err := readSegmentHeader(f)
		f.Close()
		if err != nil {
			return err
		}
		l.id = id
	}
	last := bases[len(bases)-1]
	f, err := l.fsys.OpenAppend(l.path(last))
	if err != nil {
		return err
	}
	id, err := readSegmentHeader(f)
	if err != nil {
		f.Close()
		return err
	}
	if len(bases) > 1 && id != l.id {
		f.Close()
		return fmt.Errorf("%w: segment %s has log id %016x, want %016x", ErrCorrupt, segName(last), id, l.id)
	}
	l.id = id
	size, err := f.Size()
	if err != nil {
		f.Close()
		return err
	}

	next := last
	off := int64(HeaderSize)
	var buf []byte
	for {
		var typ byte
		var payload []byte
		var end int64
		typ, payload, end, buf, err = readRecord(f, off, size, buf)
		if errors.Is(err, errIncomplete) || errors.Is(err, errBadRecord) {
			break
		}
		if err != nil {
			f.Close()
			return err
		}
		n, err := recordEvents(typ, payload)
		if err != nil {
			f.Close()
			return fmt.Errorf("segment %s at byte %d: %w", segName(last), off, err)
		}
		next += n
		off = end
	}
	if off < size {
		if err := f.Truncate(off); err != nil {
			f.Close()
			return err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
		rec.TruncatedBytes += size - off
	}
	l.f = f
	l.segBase = last
	l.segSize = off
	l.next = next
	l.synced = Pos{Segment: last, Byte: off, Next: next}
	return nil
}

// createSegment creates the segment whose first event offset is base and
// buffers its header. The directory entry is synced with the next Sync.
func (l *Log) createSegment(base uint64) error {
	f, err := l.fsys.Create(l.path(base))
	if err != nil {
		return err
	}
	l.f = f
	l.segBase = base
	l.segSize = 0
	l.buf = appendHeader(l.buf[:0], l.id)
	l.dirDirty = true
	return nil
}

// ID returns the random id of the log.
func (l *Log) ID() uint64 { return l.id }

// NextOffset returns the offset the next appended event will receive.
func (l *Log) NextOffset() uint64 { return l.next }

// Pending returns how many appended bytes are waiting for Sync.
func (l *Log) Pending() int { return len(l.buf) }

// End returns the position right after the last durable record.
func (l *Log) End() Pos { return l.synced }

// AppendBatch buffers a BATCH record whose payload holds n events and
// returns the offset of its first event. n must match the payload header.
func (l *Log) AppendBatch(payload []byte, n uint64) (uint64, error) {
	got, err := BatchEvents(payload)
	if err != nil {
		return 0, err
	}
	if got != n {
		return 0, fmt.Errorf("wal: batch declares %d events, caller passed %d", got, n)
	}
	first, err := l.append(TypeBatch, payload)
	if err != nil {
		return 0, err
	}
	l.next += n
	return first, nil
}

// AppendTick buffers a TICK record with server time tsMs.
func (l *Log) AppendTick(tsMs int64) error {
	_, err := l.append(TypeTick, EncodeTick(tsMs))
	return err
}

// AppendLease buffers a LEASE record.
func (l *Log) AppendLease(lease Lease) error {
	_, err := l.append(TypeLease, EncodeLease(lease))
	return err
}

func (l *Log) append(typ byte, payload []byte) (uint64, error) {
	if l.err != nil {
		return 0, l.err
	}
	if uint64(len(payload)) >= 1<<32-1 {
		return 0, fmt.Errorf("wal: payload of %d bytes too large", len(payload))
	}
	size := int64(frameSize + 1 + len(payload))
	cur := l.segSize + int64(len(l.buf))
	// A segment is named after its first event, so it only rotates once it
	// holds at least one event.
	if cur > HeaderSize && cur+size > l.segBytes && l.next > l.segBase {
		if err := l.rotate(); err != nil {
			l.err = err
			return 0, err
		}
	}
	l.buf = appendRecord(l.buf, typ, payload)
	return l.next, nil
}

// rotate writes and syncs the current segment and starts a new one named
// after the next offset.
func (l *Log) rotate() error {
	if err := l.Sync(); err != nil {
		return err
	}
	if err := l.f.Close(); err != nil {
		return err
	}
	return l.createSegment(l.next)
}

// Sync writes the buffered records and makes them durable. After an error
// the log refuses further work: the state on disk is unknown.
func (l *Log) Sync() error {
	if l.err != nil {
		return l.err
	}
	if len(l.buf) > 0 {
		n, err := l.f.Write(l.buf)
		l.segSize += int64(n)
		if err == nil && n < len(l.buf) {
			err = io.ErrShortWrite
		}
		if err != nil {
			l.err = err
			return err
		}
		l.buf = l.buf[:0]
	}
	if err := l.f.Sync(); err != nil {
		l.err = err
		return err
	}
	if l.dirDirty {
		if err := l.fsys.SyncDir(l.dir); err != nil {
			l.err = err
			return err
		}
		l.dirDirty = false
	}
	l.synced = Pos{Segment: l.segBase, Byte: l.segSize, Next: l.next}
	return nil
}

// Close syncs and closes the log.
func (l *Log) Close() error {
	err := l.Sync()
	if cerr := l.f.Close(); err == nil {
		err = cerr
	}
	if l.err == nil {
		l.err = errors.New("wal: log closed")
	}
	return err
}
