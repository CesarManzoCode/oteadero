package event

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
)

// Character classes of the exposition format.
const (
	classLabelStart  = 1 << iota // [a-zA-Z_]
	classLabel                   // [a-zA-Z0-9_]
	classMetricStart             // [a-zA-Z_:]
	classMetric                  // [a-zA-Z0-9_:]
)

var charClass = func() (t [256]uint8) {
	for c := 0; c < 256; c++ {
		letter := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_'
		digit := c >= '0' && c <= '9'
		if letter {
			t[c] |= classLabelStart | classMetricStart
		}
		if letter || digit {
			t[c] |= classLabel | classMetric
		}
		if c == ':' {
			t[c] |= classMetricStart | classMetric
		}
	}
	return t
}()

func isSpace(c byte) bool { return c == ' ' || c == '\t' }

func skipSpace(l []byte, i int) int {
	for i < len(l) && isSpace(l[i]) {
		i++
	}
	return i
}

func trimLine(l []byte) []byte {
	for len(l) > 0 && isSpace(l[0]) {
		l = l[1:]
	}
	for len(l) > 0 && (isSpace(l[len(l)-1]) || l[len(l)-1] == '\r') {
		l = l[:len(l)-1]
	}
	return l
}

// nextLine splits the first line off body.
func nextLine(body []byte) (line, rest []byte) {
	if i := bytes.IndexByte(body, '\n'); i >= 0 {
		return body[:i], body[i+1:]
	}
	return body, nil
}

// ParseMetrics parses a /v1/metrics body (§4.1) and appends its samples to
// dst. Every sample line is `name{label="value",...} value timestamp_ms`
// with a mandatory timestamp; empty lines and lines starting with `#` are
// ignored. The batch is atomic: on any invalid line it returns dst
// unchanged and an error wrapping ErrInvalid, and past MaxEvents samples
// one wrapping ErrTooLarge. The future-timestamp check belongs to
// admission, which owns the clock.
func ParseMetrics(dst []Metric, body []byte) ([]Metric, error) {
	start := len(dst)
	p := metricParser{labels: make([]rawLabel, 0, 16), key: make([]byte, 0, 256)}
	for n := 1; len(body) > 0; n++ {
		var l []byte
		l, body = nextLine(body)
		l = trimLine(l)
		if len(l) == 0 || l[0] == '#' {
			continue
		}
		if len(dst)-start == MaxEvents {
			return dst[:start], fmt.Errorf("%w: more than %d events", ErrTooLarge, MaxEvents)
		}
		m, msg := p.parse(l)
		if msg != "" {
			return dst[:start], fmt.Errorf("%w: line %d: %s", ErrInvalid, n, msg)
		}
		dst = append(dst, m)
	}
	return dst, nil
}

// metricParser holds scratch buffers reused across the lines of a body.
type metricParser struct {
	labels []rawLabel
	key    []byte
}

// parse parses one trimmed, non-empty sample line. A non-empty message
// reports why the line is invalid.
func (p *metricParser) parse(l []byte) (Metric, string) {
	if charClass[l[0]]&classMetricStart == 0 {
		return Metric{}, "bad metric name"
	}
	i := 1
	for i < len(l) && charClass[l[i]]&classMetric != 0 {
		i++
	}
	name := l[:i]
	p.labels = p.labels[:0]
	j := skipSpace(l, i)
	if j < len(l) && l[j] == '{' {
		var msg string
		if i, msg = p.parseLabels(l, j+1); msg != "" {
			return Metric{}, msg
		}
		j = skipSpace(l, i)
	}
	if j == len(l) {
		return Metric{}, "missing value"
	}
	if j == i {
		return Metric{}, "expected whitespace before value"
	}
	i = j
	for j < len(l) && !isSpace(l[j]) {
		j++
	}
	v, ok := parseValue(l[i:j])
	if !ok {
		return Metric{}, "bad value"
	}
	i = j
	j = skipSpace(l, i)
	if j == i || j == len(l) {
		return Metric{}, "missing timestamp"
	}
	i = j
	for j < len(l) && !isSpace(l[j]) {
		j++
	}
	ts, msg := parseTimestamp(l[i:j])
	if msg != "" {
		return Metric{}, msg
	}
	if j != len(l) {
		return Metric{}, "unexpected data after timestamp"
	}
	p.key = append(p.key[:0], name...)
	if p.key, ok = appendRawLabels(p.key, p.labels); !ok {
		return Metric{}, "duplicate label name"
	}
	return Metric{Key: string(p.key), Value: v, TsMs: ts}, ""
}

// parseLabels parses the label set that starts right after `{` at i and
// returns the index right after the closing `}`.
func (p *metricParser) parseLabels(l []byte, i int) (int, string) {
	for {
		i = skipSpace(l, i)
		if i == len(l) {
			return i, "unterminated label set"
		}
		if l[i] == '}' {
			return i + 1, ""
		}
		if charClass[l[i]]&classLabelStart == 0 {
			return i, "bad label name"
		}
		s := i
		for i < len(l) && charClass[l[i]]&classLabel != 0 {
			i++
		}
		name := l[s:i]
		i = skipSpace(l, i)
		if i == len(l) || l[i] != '=' {
			return i, "expected '=' after label name"
		}
		i = skipSpace(l, i+1)
		if i == len(l) || l[i] != '"' {
			return i, "expected '\"' before label value"
		}
		i++
		s = i
	value:
		for {
			if i == len(l) {
				return i, "unterminated label value"
			}
			switch l[i] {
			case '"':
				break value
			case '\\':
				if i+1 == len(l) {
					return i, "unterminated label value"
				}
				if c := l[i+1]; c != '\\' && c != '"' && c != 'n' {
					return i, "bad escape in label value"
				}
				i += 2
			default:
				i++
			}
		}
		if string(name) != "__name__" {
			p.labels = append(p.labels, rawLabel{name: name, value: l[s:i]})
		}
		i = skipSpace(l, i+1)
		if i == len(l) {
			return i, "unterminated label set"
		}
		switch l[i] {
		case ',':
			i++
		case '}':
			return i + 1, ""
		default:
			return i, "expected ',' or '}' after label value"
		}
	}
}

// parseValue parses a sample value. Plain integers of up to 15 digits,
// exactly representable in a float64, skip strconv; everything else
// (decimals, exponents, NaN, ±Inf) goes through strconv.ParseFloat.
func parseValue(b []byte) (float64, bool) {
	d := b
	neg := false
	if len(d) > 0 && (d[0] == '-' || d[0] == '+') {
		neg = d[0] == '-'
		d = d[1:]
	}
	if len(d) > 0 && len(d) <= 15 {
		var n int64
		i := 0
		for ; i < len(d); i++ {
			c := d[i]
			if c < '0' || c > '9' {
				break
			}
			n = n*10 + int64(c-'0')
		}
		if i == len(d) {
			f := float64(n)
			if neg {
				f = -f
			}
			return f, true
		}
	}
	f, err := strconv.ParseFloat(string(b), 64)
	return f, err == nil
}

// parseTimestamp parses a non-negative decimal timestamp in milliseconds.
func parseTimestamp(b []byte) (int64, string) {
	neg := false
	if b[0] == '-' {
		neg = true
		b = b[1:]
	}
	if len(b) == 0 {
		return 0, "bad timestamp"
	}
	var n int64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, "bad timestamp"
		}
		d := int64(c - '0')
		if n > (math.MaxInt64-d)/10 {
			return 0, "timestamp out of range"
		}
		n = n*10 + d
	}
	if neg && n != 0 {
		return 0, "negative timestamp"
	}
	return n, ""
}
