package eval

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/agg"
	"github.com/CesarManzoCode/oteadero/internal/ckpt"
	"github.com/CesarManzoCode/oteadero/internal/clock"
	"github.com/CesarManzoCode/oteadero/internal/config"
	"github.com/CesarManzoCode/oteadero/internal/event"
	"github.com/CesarManzoCode/oteadero/internal/ingest"
	"github.com/CesarManzoCode/oteadero/internal/rng"
	"github.com/CesarManzoCode/oteadero/internal/rules"
	"github.com/CesarManzoCode/oteadero/internal/vfs"
	"github.com/CesarManzoCode/oteadero/internal/wal"
)

// stepRecords is how many records a replay reads between looks at the rest of
// the world.
const stepRecords = 4096

// Deps is what a replica reads the outside world through (D03).
type Deps struct {
	FS    vfs.FS
	Clock clock.Clock
	Rand  rng.Source
	// ExportDir enables the verification export of §4.8 when not empty.
	ExportDir string
}

// tap takes what the evaluator and the watermark steps produce to the export and
// the outbox.
type tap struct {
	tenants  map[string]string
	export   *Export
	notifier *Notifier
}

var (
	_ rules.Output = (*tap)(nil)
	_ agg.Advancer = (*tap)(nil)
)

func (t *tap) Eval(e *rules.Eval) {
	if t.export != nil {
		t.export.Eval(e)
	}
}

func (t *tap) Transition(tr *rules.Transition) {
	if t.export != nil {
		t.export.Transition(tr)
	}
	t.notifier.Add(t.tenants[tr.Rule], tr)
}

func (t *tap) Closed(*agg.Bucket) {}

func (t *tap) Late(string, uint64) {}

func (t *tap) Advanced(tenant string, upTo int64, _ uint64) { t.notifier.Advance(tenant, upTo) }

// tailRecord is the last record consumed. A checkpoint saves it because a
// position alone cannot tell if the log still holds what was read: a recovery of
// serve can cut that record and write another of the same size in its place.
type tailRecord struct {
	start int64
	crc   uint32
}

// Replica follows the log read-only with the aggregation of serve and evaluates
// the rules, so all the replicas compute the same from the same log. It is a
// synchronous state machine: Recover, Step and Checkpoint run in one goroutine,
// and the aggregator stays in its inline mode. One replica is one reading of the
// log: after wal.ErrRewound, make a new one.
type Replica struct {
	fsys     vfs.FS
	clk      clock.Clock
	logDir   string
	cfg      config.Config
	class    *ingest.Classifier
	agg      *agg.Aggregator
	rules    *rules.Evaluator
	notifier *Notifier
	mgr      *ckpt.Manager
	export   *Export
	reader   *wal.Reader
	tail     tailRecord
	ckptAt   time.Duration
	ckptNext uint64
}

// NewReplica wires a replica of evaluator id over the log of cfg.DataDir. Its
// checkpoints go to <data_dir>/eval-<id>/ckpt/. It reads nothing until Recover.
func NewReplica(cfg config.Config, id string, set *rules.Set, d Deps, n *Notifier) (*Replica, error) {
	if id == "" || strings.ContainsRune(id, filepath.Separator) {
		return nil, fmt.Errorf("eval: bad evaluator id %q", id)
	}
	sketches := make(map[string]agg.SketchConfig, len(cfg.Tenants))
	for _, t := range cfg.Tenants {
		sketches[t.ID] = agg.SketchConfig{QuantileMetrics: t.QuantileMetrics, CardinalityFields: t.CardinalityFields}
	}
	a, err := agg.New(agg.Config{BucketMs: cfg.BucketMs, Shards: cfg.Shards, Sketches: sketches})
	if err != nil {
		return nil, err
	}
	t := &tap{tenants: make(map[string]string, len(set.Rules)), notifier: n}
	for i := range set.Rules {
		t.tenants[set.Rules[i].Name] = set.Rules[i].Tenant
	}
	evaluator, err := rules.NewEvaluator(set, sketches, t)
	if err != nil {
		return nil, err
	}
	a.Subscribe(evaluator)
	a.Subscribe(t)

	dir := filepath.Join(cfg.DataDir, "eval-"+id)
	if err := d.FS.MkdirAll(dir); err != nil {
		return nil, err
	}
	if err := d.FS.SyncDir(cfg.DataDir); err != nil {
		return nil, err
	}
	r := &Replica{
		fsys:     d.FS,
		clk:      d.Clock,
		logDir:   filepath.Join(cfg.DataDir, "log"),
		cfg:      cfg,
		class:    ingest.NewClassifier(cfg.BucketMs, cfg.LatenessMs, cfg.IdleGraceMs),
		agg:      a,
		rules:    evaluator,
		notifier: n,
		mgr:      ckpt.New(d.FS, dir, a, ckpt.Options{}),
	}
	if d.ExportDir != "" {
		runID := fmt.Sprintf("%d-%016x", d.Clock.NowMs(), d.Rand.Uint64())
		if t.export, err = NewExport(d.FS, d.ExportDir, id, runID); err != nil {
			return nil, err
		}
		r.export = t.export
	}
	return r, nil
}

// Recover loads the newest usable checkpoint and replays the log up to its end,
// like serve; with none it replays from the start. A checkpoint that is broken,
// of another log or past what the log now holds is skipped for the previous one.
// A log that does not exist yet is not an error: Step waits for it.
func (r *Replica) Recover() (ckpt.Report, error) {
	var rep ckpt.Report
	offsets, err := r.mgr.Offsets()
	if err != nil {
		return rep, err
	}
	for _, off := range slices.Backward(offsets) {
		st, err := r.mgr.Load(off)
		if errors.Is(err, ckpt.ErrInvalid) {
			rep.Skipped++
			continue
		}
		if err != nil {
			return rep, err
		}
		x, err := decodeExtra(st.Extra)
		if err != nil {
			return rep, err
		}
		reader, err := r.openAt(st, x)
		if err != nil {
			return rep, err
		}
		if reader == nil {
			rep.Skipped++
			continue
		}
		r.reader = reader
		if err := r.restore(st, x); err != nil {
			return rep, err
		}
		rep.FromCheckpoint = true
		rep.Offset = st.Snapshot.Pos.Next
		return rep, r.replay()
	}
	r.notifier.ResetOutbox()
	return rep, r.replay()
}

func (r *Replica) replay() error {
	for {
		n, err := r.Step(stepRecords)
		if err != nil {
			return err
		}
		if n < stepRecords {
			break
		}
	}
	r.ckptAt = r.clk.Monotonic()
	r.ckptNext = r.next()
	return nil
}

// openAt returns a reader at the position of st, or nil if the log no longer
// holds what st read.
func (r *Replica) openAt(st *ckpt.State, x extra) (*wal.Reader, error) {
	pos := st.Snapshot.Pos
	reader, err := wal.OpenReader(r.fsys, r.logDir, pos)
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, wal.ErrShortHeader) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	ok := reader.ID() == st.Snapshot.LogID
	if ok && x.tail.start > 0 {
		ok, err = wal.RecordMatches(r.fsys, r.logDir, pos.Segment, x.tail.start, pos.Byte, x.tail.crc)
		if err != nil {
			reader.Close()
			return nil, err
		}
	}
	if !ok {
		reader.Close()
		return nil, nil
	}
	return reader, nil
}

// restore loads st, which has to come from this configuration and these rules:
// other ones would not give the same results.
func (r *Replica) restore(st *ckpt.State, x extra) error {
	snap := &st.Snapshot
	if snap.BucketMs != r.cfg.BucketMs || snap.LatenessMs != r.cfg.LatenessMs || snap.IdleGraceMs != r.cfg.IdleGraceMs {
		return fmt.Errorf("eval: checkpoint has S=%d L=%d G=%d, configuration has S=%d L=%d G=%d",
			snap.BucketMs, snap.LatenessMs, snap.IdleGraceMs, r.cfg.BucketMs, r.cfg.LatenessMs, r.cfg.IdleGraceMs)
	}
	if err := r.rules.LoadState(x.rules); err != nil {
		return err
	}
	if err := r.notifier.LoadState(x.outbox); err != nil {
		return err
	}
	if err := r.agg.Restore(st.Buckets, ckpt.Watermarks(snap)); err != nil {
		return err
	}
	r.agg.RestoreSketchErrorMax(st.SketchErrorMax)
	r.class.Restore(snap.Marks)
	r.tail = x.tail
	return nil
}

// open tries to start reading the log from its beginning, and leaves the reader
// nil while the first segment has not been written yet.
func (r *Replica) open() error {
	bases, err := wal.Segments(r.fsys, r.logDir)
	if errors.Is(err, fs.ErrNotExist) || (err == nil && len(bases) == 0) {
		return nil
	}
	if err != nil {
		return err
	}
	reader, err := wal.OpenReader(r.fsys, r.logDir, wal.Pos{Byte: wal.HeaderSize})
	if errors.Is(err, wal.ErrShortHeader) {
		return nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("%w: no usable checkpoint and the first segment is gone", wal.ErrOffsetGone)
	}
	if err != nil {
		return err
	}
	r.reader = reader
	return nil
}

// Step reads up to limit records and feeds them through the aggregation, the
// rules and the outbox. It returns how many it read: fewer means it reached the
// end of what the log holds now.
func (r *Replica) Step(limit int) (int, error) {
	if r.reader == nil {
		if err := r.open(); err != nil {
			return 0, err
		}
		if r.reader == nil {
			return 0, nil
		}
	}
	for n := range limit {
		rec, err := r.reader.Next()
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return n, err
		}
		switch rec.Type {
		case wal.TypeBatch:
			b, err := event.DecodeBatch(rec.Payload)
			if err != nil {
				return n, fmt.Errorf("eval: record at offset %d: %w", rec.Offset, err)
			}
			r.agg.Batch(r.class.Batch(&b, rec.Offset))
		case wal.TypeTick:
			ts, err := wal.DecodeTick(rec.Payload)
			if err != nil {
				return n, fmt.Errorf("eval: record at offset %d: %w", rec.Offset, err)
			}
			r.agg.Tick(r.class.Tick(ts, rec.Offset))
		}
		r.tail = tailRecord{start: rec.Pos.Byte, crc: rec.CRC}
	}
	return limit, nil
}

// next returns the offset of the next event to read.
func (r *Replica) next() uint64 {
	if r.reader == nil {
		return 0
	}
	return r.reader.Pos().Next
}

// CheckpointIfDue writes a checkpoint with the policy of D05.
func (r *Replica) CheckpointIfDue() error {
	every := time.Duration(r.cfg.CheckpointIntervalMs) * time.Millisecond
	if r.clk.Monotonic()-r.ckptAt < every && r.next()-r.ckptNext < uint64(r.cfg.CheckpointEvents) {
		return nil
	}
	return r.Checkpoint()
}

// Checkpoint saves the aggregation, the rules and the outbox as of the last
// record read. The export lines that a replay from here would not repeat become
// durable first. With no log yet there is nothing to save.
func (r *Replica) Checkpoint() error {
	if r.reader == nil {
		return nil
	}
	if r.export != nil {
		if err := r.export.Flush(); err != nil {
			return err
		}
	}
	pos := r.reader.Pos()
	err := r.mgr.Save(&ckpt.State{
		Snapshot: ingest.Snapshot{
			LogID:       r.reader.ID(),
			Pos:         pos,
			BucketMs:    r.cfg.BucketMs,
			LatenessMs:  r.cfg.LatenessMs,
			IdleGraceMs: r.cfg.IdleGraceMs,
			Marks:       r.class.Marks(),
		},
		Buckets:        r.agg.OpenBuckets(),
		SketchErrorMax: r.agg.SketchErrorMax(),
		Extra:          r.appendExtra(),
	})
	if err != nil {
		return err
	}
	r.ckptAt = r.clk.Monotonic()
	r.ckptNext = pos.Next
	return nil
}

// Close writes the final checkpoint and releases the files.
func (r *Replica) Close() error {
	return errors.Join(r.Checkpoint(), r.Release())
}

// Release closes the files without a checkpoint, for a state that should not be
// saved.
func (r *Replica) Release() error {
	var err error
	if r.export != nil {
		err = r.export.Close()
	}
	if r.reader != nil {
		err = errors.Join(err, r.reader.Close())
	}
	return err
}

// extra is what the Extra of a checkpoint of an evaluator holds.
type extra struct {
	tail   tailRecord
	rules  []byte
	outbox []byte
}

func (r *Replica) appendExtra() []byte {
	b := binary.AppendVarint(nil, r.tail.start)
	b = binary.LittleEndian.AppendUint32(b, r.tail.crc)
	state := r.rules.AppendState(nil)
	b = binary.AppendUvarint(b, uint64(len(state)))
	b = append(b, state...)
	return r.notifier.AppendState(b)
}

func decodeExtra(p []byte) (extra, error) {
	sr := stateReader{p: p}
	x := extra{tail: tailRecord{start: sr.varint(), crc: sr.u32()}, rules: sr.bytes()}
	if sr.bad {
		return extra{}, errBadState
	}
	x.outbox = sr.p
	return x, nil
}
