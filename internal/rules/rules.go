// Package rules holds the alert rules of §4.5: their JSON format and, on top
// of the closed buckets of internal/agg, the window evaluation, the state
// machine of §4.6 and the incidents of §4.7. Like every core component it is
// a synchronous state machine with no I/O of its own (D03).
package rules

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/CesarManzoCode/oteadero/internal/vfs"
)

// Values of the source field.
const (
	SourceMetrics = "metrics"
	SourceLogs    = "logs"
)

// Values of the agg field.
const (
	AggCount       = "count"
	AggSum         = "sum"
	AggRate        = "rate"
	AggQuantile    = "quantile"
	AggCardinality = "cardinality"
)

// nameLabel is the label that holds the name of a metric in match and by.
const nameLabel = "__name__"

// Rule is one rule of §4.5. A missing by is nil and an empty one is not: the
// first groups by series and the second puts everything in one group.
type Rule struct {
	Name             string            `json:"name"`
	Tenant           string            `json:"tenant"`
	Source           string            `json:"source"`
	Match            map[string]string `json:"match"`
	Agg              string            `json:"agg"`
	Quantile         *float64          `json:"quantile,omitempty"`
	Field            string            `json:"field,omitempty"`
	RangeMs          int64             `json:"range_ms"`
	IntervalMs       int64             `json:"interval_ms"`
	By               []string          `json:"by"`
	Op               string            `json:"op"`
	Threshold        *float64          `json:"threshold"`
	ResolveThreshold *float64          `json:"resolve_threshold,omitempty"`
	ForMs            int64             `json:"for_ms"`
	KeepFiringForMs  int64             `json:"keep_firing_for_ms"`
}

// Set is a validated rule file for a bucket size S. Rules keep the order of
// the file.
type Set struct {
	BucketMs int64
	Rules    []Rule
	// fingerprint ties an evaluator state to the rules that produced it.
	fingerprint [sha256.Size]byte
}

// Parse decodes and validates a rule file. An unknown field or anything after
// the object is an error.
func Parse(data []byte, bucketMs int64) (*Set, error) {
	if bucketMs <= 0 {
		return nil, errors.New("rules: bucket_ms must be positive")
	}
	var file struct {
		Rules []Rule `json:"rules"`
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&file); err != nil {
		return nil, fmt.Errorf("rules: %w", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("rules: unexpected data after the object")
	}
	names := make(map[string]bool, len(file.Rules))
	for i := range file.Rules {
		r := &file.Rules[i]
		if r.IntervalMs == 0 {
			r.IntervalMs = bucketMs
		}
		if err := r.validate(bucketMs); err != nil {
			return nil, fmt.Errorf("rules: rule %d %q: %w", i, r.Name, err)
		}
		if names[r.Name] {
			return nil, fmt.Errorf("rules: rule %d: duplicate name %q", i, r.Name)
		}
		names[r.Name] = true
	}
	canonical, err := json.Marshal(file.Rules)
	if err != nil {
		return nil, fmt.Errorf("rules: %w", err)
	}
	canonical = fmt.Appendf(canonical, "%d", bucketMs)
	return &Set{BucketMs: bucketMs, Rules: file.Rules, fingerprint: sha256.Sum256(canonical)}, nil
}

// Load reads and parses the rule file at path.
func Load(fsys vfs.FS, path string, bucketMs int64) (*Set, error) {
	f, err := fsys.Open(path)
	if err != nil {
		return nil, fmt.Errorf("rules: %w", err)
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		return nil, fmt.Errorf("rules: %w", err)
	}
	data := make([]byte, size)
	if _, err := f.ReadAt(data, 0); err != nil && err != io.EOF {
		return nil, fmt.Errorf("rules: %w", err)
	}
	return Parse(data, bucketMs)
}

func (r *Rule) validate(bucketMs int64) error {
	switch {
	case r.Name == "":
		return errors.New("name is required")
	case r.Tenant == "":
		return errors.New("tenant is required")
	case r.Source != SourceMetrics && r.Source != SourceLogs:
		return fmt.Errorf("source %q is not metrics or logs", r.Source)
	case r.Match == nil:
		return errors.New("match is required")
	case r.RangeMs <= 0 || r.RangeMs%bucketMs != 0:
		return fmt.Errorf("range_ms %d is not a positive multiple of %d", r.RangeMs, bucketMs)
	case r.IntervalMs <= 0 || r.IntervalMs%bucketMs != 0:
		return fmt.Errorf("interval_ms %d is not a positive multiple of %d", r.IntervalMs, bucketMs)
	case r.ForMs < 0:
		return errors.New("for_ms must not be negative")
	case r.KeepFiringForMs < 0:
		return errors.New("keep_firing_for_ms must not be negative")
	case slices.Contains(r.By, ""):
		return errors.New("by holds an empty label name")
	}
	if err := r.validateAgg(); err != nil {
		return err
	}
	return r.validateCondition()
}

func (r *Rule) validateAgg() error {
	switch r.Agg {
	case AggCount, AggSum, AggRate:
	case AggQuantile:
		if r.Source != SourceMetrics {
			return errors.New("agg quantile needs source metrics")
		}
		if r.Quantile == nil || !(*r.Quantile > 0 && *r.Quantile <= 1) {
			return errors.New("agg quantile needs a quantile in (0,1]")
		}
	case AggCardinality:
		if r.Source != SourceLogs {
			return errors.New("agg cardinality needs source logs")
		}
		if r.Field == "" {
			return errors.New("agg cardinality needs field")
		}
	default:
		return fmt.Errorf("unknown agg %q", r.Agg)
	}
	if r.Quantile != nil && r.Agg != AggQuantile {
		return fmt.Errorf("quantile does not apply to agg %s", r.Agg)
	}
	if r.Field != "" && r.Agg != AggCardinality {
		return fmt.Errorf("field does not apply to agg %s", r.Agg)
	}
	return nil
}

func (r *Rule) validateCondition() error {
	if r.Threshold == nil {
		return errors.New("threshold is required")
	}
	resolve := r.ResolveThreshold
	switch r.Op {
	case ">", ">=":
		if resolve != nil && *resolve > *r.Threshold {
			return fmt.Errorf("resolve_threshold %v is above threshold %v for op %s", *resolve, *r.Threshold, r.Op)
		}
	case "<", "<=":
		if resolve != nil && *resolve < *r.Threshold {
			return fmt.Errorf("resolve_threshold %v is below threshold %v for op %s", *resolve, *r.Threshold, r.Op)
		}
	default:
		return fmt.Errorf("unknown op %q", r.Op)
	}
	return nil
}

// holds applies the operator of the rule. A NaN value holds for no threshold.
func (r *Rule) holds(v, threshold float64) bool {
	switch r.Op {
	case ">":
		return v > threshold
	case ">=":
		return v >= threshold
	case "<":
		return v < threshold
	default:
		return v <= threshold
	}
}
