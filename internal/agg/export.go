package agg

import (
	"encoding/json"
	"io"
	"math"
	"path/filepath"
	"strconv"

	"github.com/CesarManzoCode/oteadero/internal/vfs"
)

// exportBufBytes is how much an export file buffers between writes.
const exportBufBytes = 1 << 20

// Export is the verification export of §4.8 for serve: it subscribes to the
// closed buckets and late events and appends them to buckets.jsonl and
// late.jsonl in <dir>/serve-<run_id>/. Writes are buffered; Flush makes them
// durable and the checkpoint barrier calls it before writing a checkpoint,
// so that no line a checkpoint will not re-emit is lost in a crash. An error
// is kept and reported by Flush and Close.
type Export struct {
	buckets exportFile
	late    exportFile
	err     error
}

type exportFile struct {
	f   vfs.File
	buf []byte
}

// NewExport creates the export files of the run. runID must be unique per
// start of the process.
func NewExport(fsys vfs.FS, dir, runID string) (*Export, error) {
	run := filepath.Join(dir, "serve-"+runID)
	if err := fsys.MkdirAll(run); err != nil {
		return nil, err
	}
	buckets, err := fsys.Create(filepath.Join(run, "buckets.jsonl"))
	if err != nil {
		return nil, err
	}
	late, err := fsys.Create(filepath.Join(run, "late.jsonl"))
	if err != nil {
		buckets.Close()
		return nil, err
	}
	for _, d := range []string{run, dir} {
		if err := fsys.SyncDir(d); err != nil {
			buckets.Close()
			late.Close()
			return nil, err
		}
	}
	return &Export{buckets: exportFile{f: buckets}, late: exportFile{f: late}}, nil
}

// Closed implements Subscriber.
func (e *Export) Closed(b *Bucket) {
	e.buckets.buf = append(AppendBucketJSON(e.buckets.buf, b), '\n')
	e.spill(&e.buckets)
}

// AppendBucketJSON appends the JSON object of a closed bucket, the line of
// buckets.jsonl of §4.8 without its newline.
func AppendBucketJSON(dst []byte, b *Bucket) []byte {
	buf := append(dst, `{"tenant":`...)
	buf = AppendJSONString(buf, b.Tenant)
	buf = append(buf, `,"series":`...)
	buf = AppendJSONString(buf, b.Series)
	buf = append(buf, `,"end":`...)
	buf = strconv.AppendInt(buf, b.End, 10)
	buf = append(buf, `,"count":`...)
	buf = strconv.AppendUint(buf, b.Count, 10)
	buf = append(buf, `,"sum":`...)
	buf = AppendJSONFloat(buf, b.Sum)
	buf = append(buf, `,"rate":`...)
	buf = AppendJSONFloat(buf, b.Rate)
	buf = append(buf, `,"closed_at":`...)
	buf = strconv.AppendUint(buf, b.ClosedAt, 10)
	if x, ok := b.Ext.(exportedExt); ok {
		buf = x.appendExport(buf)
	}
	return append(buf, '}')
}

// Late implements Subscriber.
func (e *Export) Late(_ string, offset uint64) {
	buf := append(e.late.buf, `{"o":`...)
	buf = strconv.AppendUint(buf, offset, 10)
	e.late.buf = append(buf, '}', '\n')
	e.spill(&e.late)
}

func (e *Export) spill(f *exportFile) {
	if len(f.buf) >= exportBufBytes && e.err == nil {
		e.err = f.write()
	}
}

func (f *exportFile) write() error {
	if len(f.buf) == 0 {
		return nil
	}
	n, err := f.f.Write(f.buf)
	if err == nil && n < len(f.buf) {
		err = io.ErrShortWrite
	}
	f.buf = f.buf[:0]
	return err
}

// Flush writes what is buffered and makes it durable. Call it while no
// emission is in flight, after Aggregator.Barrier.
func (e *Export) Flush() error {
	for _, f := range []*exportFile{&e.buckets, &e.late} {
		if e.err != nil {
			return e.err
		}
		if e.err = f.write(); e.err == nil {
			e.err = f.f.Sync()
		}
	}
	return e.err
}

// Close flushes and closes the files. Call it after Aggregator.Stop.
func (e *Export) Close() error {
	err := e.Flush()
	for _, f := range []*exportFile{&e.buckets, &e.late} {
		if cerr := f.f.Close(); err == nil {
			err = cerr
		}
	}
	return err
}

// AppendJSONString appends s as a JSON string.
func AppendJSONString(dst []byte, s string) []byte {
	// Marshalling a string cannot fail.
	q, _ := json.Marshal(s)
	return append(dst, q...)
}

// AppendJSONFloat writes the shortest form that reads back to the same bits.
// The exposition format allows NaN and ±Inf, which JSON has no number for;
// they use the tokens Python's json module reads.
func AppendJSONFloat(dst []byte, v float64) []byte {
	switch {
	case math.IsNaN(v):
		return append(dst, "NaN"...)
	case math.IsInf(v, 1):
		return append(dst, "Infinity"...)
	case math.IsInf(v, -1):
		return append(dst, "-Infinity"...)
	}
	return strconv.AppendFloat(dst, v, 'g', -1, 64)
}
