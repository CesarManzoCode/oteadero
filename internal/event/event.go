// Package event holds the event model of §4.1, the canonical series keys of
// §4.2, the ingestion parsers and the BATCH payload encoding of §4.3.
package event

import "errors"

// Kind is the kind of a batch, as stored in the BATCH payload.
type Kind uint8

// Batch kinds of §4.3.
const (
	KindMetrics Kind = 1
	KindLogs    Kind = 2
)

// MaxEvents is the per-batch event limit of §4.1.
const MaxEvents = 100_000

// Parse errors. Their messages are the drop causes of §4.1, so a caller can
// classify with errors.Is and report err's cause directly.
var (
	ErrInvalid  = errors.New("invalid")
	ErrTooLarge = errors.New("too_large")
)

// Metric is one metric sample: canonical series key, value and event time.
type Metric struct {
	Key   string
	Value float64
	TsMs  int64
}

// Log is one log line. Labels and Fields are canonical label sets (§4.2).
type Log struct {
	TsMs   int64
	Labels string
	Fields string
	Msg    string
}

// Batch is the unit of admission and of a BATCH record: one tenant, one
// kind, and the events of that kind.
type Batch struct {
	Tenant   string
	Kind     Kind
	IngestMs int64
	Metrics  []Metric
	Logs     []Log
}

// Len returns the number of events of the batch, the count that consumes
// log offsets.
func (b *Batch) Len() int {
	if b.Kind == KindLogs {
		return len(b.Logs)
	}
	return len(b.Metrics)
}
