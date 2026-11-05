package eval

import (
	"io"
	"path/filepath"
	"strconv"

	"github.com/CesarManzoCode/oteadero/internal/agg"
	"github.com/CesarManzoCode/oteadero/internal/rules"
	"github.com/CesarManzoCode/oteadero/internal/vfs"
)

// exportBufBytes is how much an export file buffers between writes.
const exportBufBytes = 1 << 20

// Export is the verification export of §4.8 for evaluate: evals.jsonl and
// transitions.jsonl in <dir>/<id>-<run_id>/. Writes are buffered; a checkpoint
// flushes before it stops re-emitting those lines. An error is kept and reported
// by Flush and Close.
type Export struct {
	evals       exportFile
	transitions exportFile
	err         error
}

type exportFile struct {
	f   vfs.File
	buf []byte
}

// NewExport creates the export files of the run. runID must be unique per start
// of the replica.
func NewExport(fsys vfs.FS, dir, id, runID string) (*Export, error) {
	run := filepath.Join(dir, id+"-"+runID)
	if err := fsys.MkdirAll(run); err != nil {
		return nil, err
	}
	evals, err := fsys.Create(filepath.Join(run, "evals.jsonl"))
	if err != nil {
		return nil, err
	}
	transitions, err := fsys.Create(filepath.Join(run, "transitions.jsonl"))
	if err != nil {
		evals.Close()
		return nil, err
	}
	for _, d := range []string{run, dir} {
		if err := fsys.SyncDir(d); err != nil {
			evals.Close()
			transitions.Close()
			return nil, err
		}
	}
	return &Export{evals: exportFile{f: evals}, transitions: exportFile{f: transitions}}, nil
}

func appendLineStart(buf []byte, rule, group string, t int64) []byte {
	buf = append(buf, `{"rule":`...)
	buf = agg.AppendJSONString(buf, rule)
	buf = append(buf, `,"group":`...)
	buf = agg.AppendJSONString(buf, group)
	buf = append(buf, `,"t":`...)
	return strconv.AppendInt(buf, t, 10)
}

func appendLineEnd(buf []byte, value float64, present bool, closedAt uint64) []byte {
	buf = append(buf, `,"value":`...)
	if present {
		buf = agg.AppendJSONFloat(buf, value)
	} else {
		buf = append(buf, "null"...)
	}
	buf = append(buf, `,"closed_at":`...)
	buf = strconv.AppendUint(buf, closedAt, 10)
	return append(buf, '}', '\n')
}

// Eval appends a line to evals.jsonl.
func (e *Export) Eval(x *rules.Eval) {
	buf := appendLineStart(e.evals.buf, x.Rule, x.Group, x.T)
	e.evals.buf = appendLineEnd(buf, x.Value, x.Present, x.ClosedAt)
	e.spill(&e.evals)
}

// Transition appends a line to transitions.jsonl.
func (e *Export) Transition(x *rules.Transition) {
	buf := appendLineStart(e.transitions.buf, x.Rule, x.Group, x.T)
	buf = append(buf, `,"state":`...)
	buf = agg.AppendJSONString(buf, x.State)
	e.transitions.buf = appendLineEnd(buf, x.Value, x.Present, x.ClosedAt)
	e.spill(&e.transitions)
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

// Flush writes what is buffered and makes it durable.
func (e *Export) Flush() error {
	for _, f := range []*exportFile{&e.evals, &e.transitions} {
		if e.err != nil {
			return e.err
		}
		if e.err = f.write(); e.err == nil {
			e.err = f.f.Sync()
		}
	}
	return e.err
}

// Close flushes and closes the files.
func (e *Export) Close() error {
	err := e.Flush()
	for _, f := range []*exportFile{&e.evals, &e.transitions} {
		if cerr := f.f.Close(); err == nil {
			err = cerr
		}
	}
	return err
}
