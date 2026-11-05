package event

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// logLine is one NDJSON object of /v1/logs (§4.1).
type logLine struct {
	Ts     *int64            `json:"ts"`
	Labels map[string]string `json:"labels"`
	Fields map[string]string `json:"fields"`
	Msg    *string           `json:"msg"`
}

// ParseLogs parses a /v1/logs body (§4.1), one JSON object per line, and
// appends its events to dst. `ts` (integer milliseconds, non-negative) and
// `msg` are required; `labels` and `fields` are optional string maps whose
// names follow the label-name syntax. Empty lines are ignored. The batch is
// atomic: on any invalid line it returns dst unchanged and an error
// wrapping ErrInvalid, and past MaxEvents events one wrapping ErrTooLarge.
func ParseLogs(dst []Log, body []byte) ([]Log, error) {
	start := len(dst)
	var scratch []Label
	for n := 1; len(body) > 0; n++ {
		var l []byte
		l, body = nextLine(body)
		l = trimLine(l)
		if len(l) == 0 {
			continue
		}
		if len(dst)-start == MaxEvents {
			return dst[:start], fmt.Errorf("%w: more than %d events", ErrTooLarge, MaxEvents)
		}
		var ev Log
		var msg string
		if ev, scratch, msg = parseLogLine(l, scratch); msg != "" {
			return dst[:start], fmt.Errorf("%w: line %d: %s", ErrInvalid, n, msg)
		}
		dst = append(dst, ev)
	}
	return dst, nil
}

func parseLogLine(l []byte, scratch []Label) (Log, []Label, string) {
	dec := json.NewDecoder(bytes.NewReader(l))
	dec.DisallowUnknownFields()
	var r logLine
	if err := dec.Decode(&r); err != nil {
		return Log{}, scratch, err.Error()
	}
	if _, err := dec.Token(); err != io.EOF {
		return Log{}, scratch, "unexpected data after object"
	}
	switch {
	case r.Ts == nil:
		return Log{}, scratch, "missing ts"
	case *r.Ts < 0:
		return Log{}, scratch, "negative timestamp"
	case r.Msg == nil:
		return Log{}, scratch, "missing msg"
	}
	ev := Log{TsMs: *r.Ts, Msg: *r.Msg}
	var ok bool
	if ev.Labels, scratch, ok = canonicalMap(r.Labels, scratch); !ok {
		return Log{}, scratch, "bad label name"
	}
	if ev.Fields, scratch, ok = canonicalMap(r.Fields, scratch); !ok {
		return Log{}, scratch, "bad field name"
	}
	return ev, scratch, ""
}

// canonicalMap returns the canonical form of m, or false if a name is not
// a valid label name.
func canonicalMap(m map[string]string, scratch []Label) (string, []Label, bool) {
	scratch = scratch[:0]
	for k, v := range m {
		if !isLabelName(k) {
			return "", scratch, false
		}
		scratch = append(scratch, Label{Name: k, Value: v})
	}
	return CanonicalLabels(scratch), scratch, true
}

func isLabelName(s string) bool {
	if s == "" || charClass[s[0]]&classLabelStart == 0 {
		return false
	}
	for i := 1; i < len(s); i++ {
		if charClass[s[i]]&classLabel == 0 {
			return false
		}
	}
	return true
}
