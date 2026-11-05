package ckpt

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/agg"
	"github.com/CesarManzoCode/oteadero/internal/ingest"
	"github.com/CesarManzoCode/oteadero/internal/vfs"
	"github.com/CesarManzoCode/oteadero/internal/wal"
)

// Policy of D05.
const (
	// DefaultEvery and DefaultEvents are when a checkpoint is due.
	DefaultEvery  = 10 * time.Second
	DefaultEvents = 2_000_000
	// DefaultRetainEvents is the tail retention no segment removal touches.
	DefaultRetainEvents = 50_000_000
	// keep is how many checkpoints stay on disk.
	keep = 2

	fileSuffix = ".ckpt"
	tmpSuffix  = ".ckpt.tmp"
)

// Options configures a Manager.
type Options struct {
	// Flush, if set, runs at each barrier before the checkpoint is written;
	// the verification export goes here.
	Flush func() error
	// RetainEvents is the tail retention; zero means DefaultRetainEvents.
	RetainEvents uint64
	// Pinned, if set, returns the lowest segment a live reader is still
	// reading; no segment from it on is removed while ok is true.
	Pinned func() (segment uint64, ok bool)
	// Extra, if set, returns the opaque state stored in State.Extra, read
	// after the shards' barrier; RestoreExtra loads it back in Recover.
	Extra        func() []byte
	RestoreExtra func([]byte) error
}

// Manager writes checkpoints under <data>/ckpt/, removes what they make
// unnecessary and recovers from them. It is the sequencer's Checkpointer.
type Manager struct {
	fsys    vfs.FS
	data    string
	dir     string
	logDir  string
	agg     *agg.Aggregator
	flush   func() error
	retain  uint64
	pinned  func() (uint64, bool)
	extra   func() []byte
	restore func([]byte) error
	dirMade bool
}

var _ ingest.Checkpointer = (*Manager)(nil)

// New returns a Manager for the data directory, whose log is <data>/log.
func New(fsys vfs.FS, data string, a *agg.Aggregator, opts Options) *Manager {
	if opts.RetainEvents == 0 {
		opts.RetainEvents = DefaultRetainEvents
	}
	return &Manager{
		fsys:    fsys,
		data:    data,
		dir:     filepath.Join(data, "ckpt"),
		logDir:  filepath.Join(data, "log"),
		agg:     a,
		flush:   opts.Flush,
		retain:  opts.RetainEvents,
		pinned:  opts.Pinned,
		extra:   opts.Extra,
		restore: opts.RestoreExtra,
	}
}

func fileName(offset uint64) string {
	return fmt.Sprintf("%020d%s", offset, fileSuffix)
}

func parseFileName(name string) (uint64, bool) {
	digits, ok := strings.CutSuffix(name, fileSuffix)
	if !ok || len(digits) != 20 {
		return 0, false
	}
	v, err := strconv.ParseUint(digits, 10, 64)
	return v, err == nil
}

func (m *Manager) path(offset uint64) string {
	return filepath.Join(m.dir, fileName(offset))
}

func (m *Manager) ensureDir() error {
	if m.dirMade {
		return nil
	}
	if err := m.fsys.MkdirAll(m.dir); err != nil {
		return err
	}
	if err := m.fsys.SyncDir(m.data); err != nil {
		return err
	}
	m.dirMade = true
	return nil
}

// Checkpoint completes the barrier the sequencer started: it walks the
// shards, flushes the export and writes the state of that point of the log.
// Lines are made durable before the checkpoint that stops re-emitting them.
func (m *Manager) Checkpoint(snap *ingest.Snapshot) error {
	m.agg.Barrier()
	if m.flush != nil {
		if err := m.flush(); err != nil {
			return err
		}
	}
	st := &State{Snapshot: *snap, Buckets: m.agg.OpenBuckets(), SketchErrorMax: m.agg.SketchErrorMax()}
	if m.extra != nil {
		st.Extra = m.extra()
	}
	if err := m.write(st); err != nil {
		return err
	}
	if err := m.prune(); err != nil {
		return err
	}
	return m.removeSegments(snap.Pos.Next)
}

// write stores st with temporary file + fsync + rename + directory fsync,
// so that the name only ever points to a complete file. Two checkpoints at
// the same offset, with only TICK records between them, share the name and
// the newer one replaces the older.
func (m *Manager) write(st *State) error {
	if err := m.ensureDir(); err != nil {
		return err
	}
	final := m.path(st.Snapshot.Pos.Next)
	tmp := final + ".tmp"
	f, err := m.fsys.Create(tmp)
	if err != nil {
		return err
	}
	data := Encode(st)
	n, err := f.Write(data)
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if err := m.fsys.Rename(tmp, final); err != nil {
		return err
	}
	return m.fsys.SyncDir(m.dir)
}

// Save writes st and prunes the older files. It is the part of a checkpoint
// for a process that reads a log it does not own, which recovers by itself
// with Offsets and Load and never removes a segment.
func (m *Manager) Save(st *State) error {
	if err := m.write(st); err != nil {
		return err
	}
	return m.prune()
}

// Offsets returns the offsets of the checkpoint files, ascending.
func (m *Manager) Offsets() ([]uint64, error) { return m.offsets() }

// Load reads and decodes the checkpoint at offset.
func (m *Manager) Load(offset uint64) (*State, error) { return m.load(offset) }

// offsets returns the offsets of the checkpoint files, ascending.
func (m *Manager) offsets() ([]uint64, error) {
	names, err := m.fsys.ReadDir(m.dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []uint64
	for _, n := range names {
		if o, ok := parseFileName(n); ok {
			out = append(out, o)
		}
	}
	slices.Sort(out)
	return out, nil
}

// prune removes all but the newest checkpoints and any temporary file left
// by a crash.
func (m *Manager) prune() error {
	names, err := m.fsys.ReadDir(m.dir)
	if err != nil {
		return err
	}
	var offsets []uint64
	removed := false
	for _, n := range names {
		if strings.HasSuffix(n, tmpSuffix) {
			if err := m.fsys.Remove(filepath.Join(m.dir, n)); err != nil {
				return err
			}
			removed = true
		} else if o, ok := parseFileName(n); ok {
			offsets = append(offsets, o)
		}
	}
	slices.Sort(offsets)
	for len(offsets) > keep {
		if err := m.fsys.Remove(m.path(offsets[0])); err != nil {
			return err
		}
		offsets = offsets[1:]
		removed = true
	}
	if !removed {
		return nil
	}
	return m.fsys.SyncDir(m.dir)
}

// removeSegments applies D05: a segment goes only if all of it comes before
// the oldest checkpoint's position and outside the tail retention, counted
// back from head, and no live reader is in it or before it. The last segment
// has no successor and always stays.
func (m *Manager) removeSegments(head uint64) error {
	offsets, err := m.offsets()
	if err != nil || len(offsets) == 0 {
		return err
	}
	bases, err := wal.Segments(m.fsys, m.logDir)
	if err != nil {
		return err
	}
	// The position needs the oldest file read; a segment's successor must
	// start by the checkpoint offset for it to matter.
	if len(bases) < 2 || bases[1] > offsets[0] || !m.outsideRetention(bases[1], head) {
		return nil
	}
	oldest, err := m.load(offsets[0])
	if errors.Is(err, ErrInvalid) {
		return nil
	}
	if err != nil {
		return err
	}
	pin, pinned := uint64(0), false
	if m.pinned != nil {
		pin, pinned = m.pinned()
	}
	removed := false
	for i := 0; i+1 < len(bases); i++ {
		next := bases[i+1]
		if next > oldest.Snapshot.Pos.Segment || !m.outsideRetention(next, head) || (pinned && next > pin) {
			break
		}
		if err := wal.RemoveSegment(m.fsys, m.logDir, bases[i]); err != nil {
			return err
		}
		removed = true
	}
	if !removed {
		return nil
	}
	return m.fsys.SyncDir(m.logDir)
}

// outsideRetention reports whether offsets before end are all older than
// the newest m.retain events below head.
func (m *Manager) outsideRetention(end, head uint64) bool {
	return end <= head && head-end >= m.retain
}

// load reads and decodes the checkpoint at offset.
func (m *Manager) load(offset uint64) (*State, error) {
	f, err := m.fsys.Open(m.path(offset))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		return nil, err
	}
	data := make([]byte, size)
	if _, err := f.ReadAt(data, 0); err != nil && err != io.EOF {
		return nil, err
	}
	return Decode(data)
}

// Report says what Recover did.
type Report struct {
	// FromCheckpoint is true when a checkpoint was loaded.
	FromCheckpoint bool
	// Offset is the offset the replay started from.
	Offset uint64
	// Skipped counts newer checkpoints that were not usable.
	Skipped int
}

// Recover loads the newest usable checkpoint into seq and the aggregator
// and replays the log from its position; with none it replays the log from
// the start. A checkpoint that is broken, belongs to another log, lies past
// the end of the log or needs a removed segment is skipped for the previous
// one. The log must have been opened, which truncates its broken tail. Call
// it before the sequencer starts; nobody is acked again.
func (m *Manager) Recover(seq *ingest.Sequencer) (Report, error) {
	var rep Report
	offsets, err := m.offsets()
	if err != nil {
		return rep, err
	}
	for _, off := range slices.Backward(offsets) {
		st, err := m.load(off)
		if errors.Is(err, ErrInvalid) {
			rep.Skipped++
			continue
		}
		if err != nil {
			return rep, err
		}
		r, err := wal.OpenReader(m.fsys, m.logDir, st.Snapshot.Pos)
		if errors.Is(err, fs.ErrNotExist) {
			rep.Skipped++
			continue
		}
		if err != nil {
			return rep, err
		}
		defer r.Close()
		err = seq.Restore(&st.Snapshot)
		if errors.Is(err, ingest.ErrForeignSnapshot) {
			rep.Skipped++
			continue
		}
		if err != nil {
			return rep, err
		}
		if err := m.agg.Restore(st.Buckets, Watermarks(&st.Snapshot)); err != nil {
			return rep, err
		}
		m.agg.RestoreSketchErrorMax(st.SketchErrorMax)
		if m.restore != nil {
			if err := m.restore(st.Extra); err != nil {
				return rep, err
			}
		}
		rep.FromCheckpoint = true
		rep.Offset = st.Snapshot.Pos.Next
		return rep, m.replay(seq, r)
	}

	r, err := wal.OpenReader(m.fsys, m.logDir, wal.Pos{Byte: wal.HeaderSize})
	if errors.Is(err, fs.ErrNotExist) {
		return rep, fmt.Errorf("%w: no usable checkpoint and the first segment is gone", wal.ErrOffsetGone)
	}
	if err != nil {
		return rep, err
	}
	defer r.Close()
	return rep, m.replay(seq, r)
}

func (m *Manager) replay(seq *ingest.Sequencer, r *wal.Reader) error {
	if err := seq.Replay(r); err != nil {
		return err
	}
	m.agg.Barrier()
	return nil
}

// Watermarks returns the tenant watermarks of snap, as Aggregator.Restore takes
// them.
func Watermarks(snap *ingest.Snapshot) []ingest.WatermarkAdvance {
	out := make([]ingest.WatermarkAdvance, len(snap.Marks))
	for i, x := range snap.Marks {
		out[i] = ingest.WatermarkAdvance{Tenant: x.Tenant, Watermark: x.MaxTs - snap.LatenessMs}
	}
	return out
}
