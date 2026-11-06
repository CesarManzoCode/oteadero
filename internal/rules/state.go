package rules

import (
	"encoding/binary"
	"errors"
	"maps"
	"math"
	"slices"

	"github.com/CesarManzoCode/oteadero/internal/sketch"
)

var errBadState = errors.New("rules: bad evaluator state")

// AppendState appends the exact state of the evaluator for a checkpoint: the
// fingerprint of the rules, the watermark step of each tenant and the groups
// of each rule. Equal states have equal bytes. Call it between watermark
// steps.
func (e *Evaluator) AppendState(dst []byte) []byte {
	dst = append(dst, e.set.fingerprint[:]...)
	for _, id := range slices.Sorted(maps.Keys(e.tenants)) {
		t := e.tenants[id]
		dst = appendBool(dst, t.defined)
		dst = binary.AppendVarint(dst, t.prev)
	}
	for _, r := range e.rules {
		dst = binary.AppendUvarint(dst, uint64(len(r.groups)))
		for _, key := range slices.Sorted(maps.Keys(r.groups)) {
			dst = appendBytes(dst, []byte(key))
			dst = r.groups[key].appendState(dst)
		}
	}
	return dst
}

func (g *group) appendState(dst []byte) []byte {
	dst = append(dst, byte(g.state))
	dst = binary.AppendVarint(dst, g.activeAt)
	dst = appendBool(dst, g.keeping)
	dst = binary.AppendVarint(dst, g.keepSince)
	dst = binary.AppendVarint(dst, g.firedAt)
	dst = binary.AppendUvarint(dst, uint64(len(g.slots)))
	for i := range g.slots {
		s := &g.slots[i]
		dst = binary.AppendVarint(dst, s.end)
		dst = binary.AppendUvarint(dst, s.count)
		dst = binary.AppendUvarint(dst, uint64(len(s.sums)))
		for _, x := range s.sums {
			dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(x))
		}
		var dd, hll []byte
		if s.dd != nil {
			dd = s.dd.AppendState(nil)
		}
		if s.hll != nil {
			hll = s.hll.AppendState(nil)
		}
		dst = appendBytes(dst, dd)
		dst = appendBytes(dst, hll)
	}
	return dst
}

func appendBool(dst []byte, v bool) []byte {
	if v {
		return append(dst, 1)
	}
	return append(dst, 0)
}

func appendBytes(dst, v []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(v)))
	return append(dst, v...)
}

// LoadState replaces the state with what AppendState wrote. A state written
// for other rules is an error, because its slots would mean something else. On
// error the evaluator keeps its state.
func (e *Evaluator) LoadState(state []byte) error {
	if len(state) < len(e.set.fingerprint) || string(state[:len(e.set.fingerprint)]) != string(e.set.fingerprint[:]) {
		return errors.New("rules: evaluator state belongs to other rules")
	}
	r := stateReader{p: state[len(e.set.fingerprint):]}
	tenants := slices.Sorted(maps.Keys(e.tenants))
	marks := make([]tenantState, len(tenants))
	for i := range marks {
		marks[i].defined = r.bool()
		marks[i].prev = r.varint()
	}
	groups := make([]map[string]*group, len(e.rules))
	for i := range groups {
		groups[i] = r.groups()
	}
	if r.bad || len(r.p) != 0 {
		return errBadState
	}
	for i, id := range tenants {
		t := e.tenants[id]
		t.defined = marks[i].defined
		t.prev = marks[i].prev
	}
	for i, rs := range e.rules {
		rs.groups = groups[i]
	}
	return nil
}

type stateReader struct {
	p   []byte
	bad bool
}

func (r *stateReader) uvarint() uint64 {
	v, n := binary.Uvarint(r.p)
	if n <= 0 {
		r.bad = true
		return 0
	}
	r.p = r.p[n:]
	return v
}

func (r *stateReader) varint() int64 {
	v, n := binary.Varint(r.p)
	if n <= 0 {
		r.bad = true
		return 0
	}
	r.p = r.p[n:]
	return v
}

func (r *stateReader) byte() byte {
	if len(r.p) == 0 {
		r.bad = true
		return 0
	}
	v := r.p[0]
	r.p = r.p[1:]
	return v
}

func (r *stateReader) bool() bool {
	v := r.byte()
	if v > 1 {
		r.bad = true
	}
	return v == 1
}

func (r *stateReader) bytes() []byte {
	n := r.uvarint()
	if r.bad || n > uint64(len(r.p)) {
		r.bad = true
		return nil
	}
	v := r.p[:n]
	r.p = r.p[n:]
	return v
}

// count reads a number of entries of at least minBytes each, so that a damaged
// count cannot ask for more memory than the state holds.
func (r *stateReader) count(minBytes uint64) int {
	n := r.uvarint()
	if r.bad || n > uint64(len(r.p))/minBytes {
		r.bad = true
		return 0
	}
	return int(n)
}

func (r *stateReader) groups() map[string]*group {
	groups := map[string]*group{}
	n := r.count(1)
	prev := ""
	for i := range n {
		key := string(r.bytes())
		if r.bad || (i > 0 && key <= prev) {
			r.bad = true
			return nil
		}
		prev = key
		groups[key] = r.group()
		if r.bad {
			return nil
		}
	}
	return groups
}

func (r *stateReader) group() *group {
	g := &group{}
	state := r.byte()
	if state > byte(alertFiring) {
		r.bad = true
	}
	g.state = alertState(state)
	g.activeAt = r.varint()
	g.keeping = r.bool()
	g.keepSince = r.varint()
	g.firedAt = r.varint()
	n := r.count(1)
	for i := range n {
		s := r.slot()
		if r.bad || (i > 0 && s.end <= g.slots[i-1].end) {
			r.bad = true
			return nil
		}
		g.slots = append(g.slots, s)
	}
	return g
}

func (r *stateReader) slot() slot {
	s := slot{end: r.varint(), count: r.uvarint()}
	n := r.count(8)
	for range n {
		s.sums = append(s.sums, math.Float64frombits(binary.LittleEndian.Uint64(r.p)))
		r.p = r.p[8:]
	}
	if dd := r.bytes(); len(dd) > 0 {
		s.dd = sketch.NewDDSketch()
		if err := s.dd.LoadState(dd); err != nil {
			r.bad = true
		}
	}
	if hll := r.bytes(); len(hll) > 0 {
		s.hll = &sketch.HLL{}
		if err := s.hll.LoadState(hll); err != nil {
			r.bad = true
		}
	}
	return s
}
