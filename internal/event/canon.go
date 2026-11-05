package event

import (
	"bytes"
	"slices"
	"strings"
)

// Label is one name/value pair before canonicalization.
type Label struct {
	Name, Value string
}

// AppendLabels appends the canonical form of ls (§4.2) to dst: sorted by
// name in bytes, values escaped, `{}` when empty. It sorts ls in place.
// Names must be unique.
func AppendLabels(dst []byte, ls []Label) []byte {
	slices.SortFunc(ls, func(a, b Label) int { return strings.Compare(a.Name, b.Name) })
	dst = append(dst, '{')
	for i, l := range ls {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, l.Name...)
		dst = append(dst, '=', '"')
		dst = appendEscaped(dst, l.Value)
		dst = append(dst, '"')
	}
	return append(dst, '}')
}

// CanonicalLabels returns the canonical form of ls. It sorts ls in place.
func CanonicalLabels(ls []Label) string {
	return string(AppendLabels(nil, ls))
}

// MetricKey returns the canonical key of a metric series: name followed by
// its canonical labels without `__name__`. It reorders ls.
func MetricKey(name string, ls []Label) string {
	ls = slices.DeleteFunc(ls, func(l Label) bool { return l.Name == "__name__" })
	return string(AppendLabels([]byte(name), ls))
}

func appendEscaped(dst []byte, v string) []byte {
	for i := 0; i < len(v); i++ {
		switch c := v[i]; c {
		case '\\':
			dst = append(dst, '\\', '\\')
		case '"':
			dst = append(dst, '\\', '"')
		case '\n':
			dst = append(dst, '\\', 'n')
		default:
			dst = append(dst, c)
		}
	}
	return dst
}

// rawLabel is a label as it appears in exposition text: the value is still
// escaped, which with the only escapes accepted (\\, \" and \n) is exactly
// its canonical escaped form.
type rawLabel struct {
	name, value []byte
}

// appendRawLabels sorts ls in place and appends their canonical form. It
// reports false on a duplicate name.
func appendRawLabels(dst []byte, ls []rawLabel) ([]byte, bool) {
	slices.SortFunc(ls, func(a, b rawLabel) int { return bytes.Compare(a.name, b.name) })
	dst = append(dst, '{')
	for i, l := range ls {
		if i > 0 {
			if bytes.Equal(ls[i-1].name, l.name) {
				return dst, false
			}
			dst = append(dst, ',')
		}
		dst = append(dst, l.name...)
		dst = append(dst, '=', '"')
		dst = append(dst, l.value...)
		dst = append(dst, '"')
	}
	return append(dst, '}'), true
}

// FNV-1a 64 parameters.
const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// SeriesHash is the FNV-1a 64 hash of the series identity (tenant, key)
// that picks the aggregation shard (D03): tenant bytes, a 0x00 separator,
// then key bytes.
func SeriesHash(tenant, key string) uint64 {
	h := uint64(fnvOffset64)
	for i := 0; i < len(tenant); i++ {
		h ^= uint64(tenant[i])
		h *= fnvPrime64
	}
	h *= fnvPrime64 // separator byte 0x00: xor with zero is a no-op
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= fnvPrime64
	}
	return h
}
