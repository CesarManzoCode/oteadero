package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/CesarManzoCode/oteadero/internal/vfs"
)

// ErrOffsetGone reports an offset older than the oldest segment.
var ErrOffsetGone = errors.New("wal: offset no longer in the log")

// ErrRewound reports a segment now shorter than what the reader consumed: a
// recovery of the writer truncated records the reader had already seen.
var ErrRewound = errors.New("wal: log truncated behind the reader")

// Reader reads records in log order, moving across segments. It sees what
// has been written to the files; Next returns io.EOF at the end of the data
// and may be called again later to pick up new records.
type Reader struct {
	fsys vfs.FS
	dir  string
	id   uint64
	f    vfs.File
	pos  Pos
	buf  []byte
}

// OpenReader returns a reader positioned at p, which must be a record
// boundary such as Log.End or a Pos of a Record.
func OpenReader(fsys vfs.FS, dir string, p Pos) (*Reader, error) {
	r := &Reader{fsys: fsys, dir: dir}
	if err := r.openSegment(p.Segment); err != nil {
		return nil, err
	}
	r.pos = p
	return r, nil
}

// OpenReaderAt returns a reader positioned at the record that holds event
// offset o, or at the end of the log if o is the next offset. The caller
// skips the events of that record below o.
func OpenReaderAt(fsys vfs.FS, dir string, o uint64) (*Reader, error) {
	bases, err := listSegments(fsys, dir)
	if err != nil {
		return nil, err
	}
	i := len(bases) - 1
	for i >= 0 && bases[i] > o {
		i--
	}
	if i < 0 {
		return nil, fmt.Errorf("%w: %d", ErrOffsetGone, o)
	}
	r, err := OpenReader(fsys, dir, Pos{Segment: bases[i], Byte: HeaderSize, Next: bases[i]})
	if err != nil {
		return nil, err
	}
	for {
		before := r.pos
		rec, err := r.Next()
		if err == io.EOF {
			if r.pos.Next < o {
				r.Close()
				return nil, fmt.Errorf("wal: offset %d is past the end of the log (%d)", o, r.pos.Next)
			}
			return r, nil
		}
		if err != nil {
			r.Close()
			return nil, err
		}
		if rec.Type == TypeBatch && rec.Offset+rec.Events > o {
			if err := r.seek(before); err != nil {
				r.Close()
				return nil, err
			}
			return r, nil
		}
	}
}

func (r *Reader) seek(p Pos) error {
	if p.Segment != r.pos.Segment {
		if err := r.openSegment(p.Segment); err != nil {
			return err
		}
	}
	r.pos = p
	return nil
}

func (r *Reader) openSegment(base uint64) error {
	f, err := r.fsys.Open(filepath.Join(r.dir, segName(base)))
	if err != nil {
		return err
	}
	id, err := readSegmentHeader(f)
	if err != nil {
		f.Close()
		return err
	}
	if r.f != nil {
		if id != r.id {
			f.Close()
			return fmt.Errorf("%w: segment %s has log id %016x, want %016x", ErrCorrupt, segName(base), id, r.id)
		}
		r.f.Close()
	}
	r.id = id
	r.f = f
	return nil
}

// ID returns the id of the log being read.
func (r *Reader) ID() uint64 { return r.id }

// Pos returns the position of the next record to read.
func (r *Reader) Pos() Pos { return r.pos }

// Next returns the next record. Its payload is valid until the next call.
func (r *Reader) Next() (Record, error) {
	for {
		size, err := r.f.Size()
		if err != nil {
			return Record{}, err
		}
		if size < r.pos.Byte {
			return Record{}, fmt.Errorf("%w: segment %s has %d bytes, reader at %d", ErrRewound, segName(r.pos.Segment), size, r.pos.Byte)
		}
		typ, payload, end, crc, buf, err := readRecord(r.f, r.pos.Byte, size, r.buf)
		r.buf = buf
		switch {
		case err == nil:
			n, err := recordEvents(typ, payload)
			if err != nil {
				return Record{}, err
			}
			rec := Record{Type: typ, Payload: payload, Offset: r.pos.Next, Events: n, Pos: r.pos, CRC: crc}
			r.pos = Pos{Segment: r.pos.Segment, Byte: end, Next: r.pos.Next + n}
			return rec, nil
		case errors.Is(err, errBadRecord):
			return Record{}, fmt.Errorf("%w: segment %s at byte %d", ErrCorrupt, segName(r.pos.Segment), r.pos.Byte)
		case !errors.Is(err, errIncomplete):
			return Record{}, err
		}

		// End of the data of this segment: move on if a later one exists.
		next, ok, err := r.nextSegment()
		if err != nil || !ok {
			if err == nil {
				err = io.EOF
			}
			return Record{}, err
		}
		// The writer finishes a segment before it creates the next one, so
		// this size is final: what arrived since the first read is still to
		// be read.
		final, err := r.f.Size()
		if err != nil {
			return Record{}, err
		}
		if final != size {
			continue
		}
		if size != r.pos.Byte {
			return Record{}, fmt.Errorf("%w: segment %s has %d trailing bytes", ErrCorrupt, segName(r.pos.Segment), size-r.pos.Byte)
		}
		if next != r.pos.Next {
			return Record{}, fmt.Errorf("%w: segment %s follows next offset %d", ErrCorrupt, segName(next), r.pos.Next)
		}
		if err := r.openSegment(next); err != nil {
			return Record{}, err
		}
		r.pos = Pos{Segment: next, Byte: HeaderSize, Next: next}
	}
}

// nextSegment returns the first segment after the current one whose header
// is complete.
func (r *Reader) nextSegment() (uint64, bool, error) {
	bases, err := listSegments(r.fsys, r.dir)
	if err != nil {
		return 0, false, err
	}
	for _, b := range bases {
		if b <= r.pos.Segment {
			continue
		}
		f, err := r.fsys.Open(filepath.Join(r.dir, segName(b)))
		if err != nil {
			return 0, false, err
		}
		size, err := f.Size()
		f.Close()
		if err != nil {
			return 0, false, err
		}
		return b, size >= HeaderSize, nil
	}
	return 0, false, nil
}

// RecordMatches reports whether the record that starts at byte start of the
// segment still ends at byte end with checksum crc. It is how a reader's saved
// position is checked against a log that a recovery may have rewritten.
func RecordMatches(fsys vfs.FS, dir string, segment uint64, start, end int64, crc uint32) (bool, error) {
	f, err := fsys.Open(filepath.Join(dir, segName(segment)))
	if err != nil {
		return false, err
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		return false, err
	}
	if end > size || end-start < frameSize {
		return false, nil
	}
	var frame [frameSize]byte
	if _, err := f.ReadAt(frame[:], start); err != nil {
		return false, err
	}
	n := int64(binary.LittleEndian.Uint32(frame[:4]))
	return start+frameSize+n == end && binary.LittleEndian.Uint32(frame[4:]) == crc, nil
}

// Close closes the reader.
func (r *Reader) Close() error {
	if r.f == nil {
		return nil
	}
	err := r.f.Close()
	r.f = nil
	return err
}
