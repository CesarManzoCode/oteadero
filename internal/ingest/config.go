// Package ingest is the path from a parsed batch to the log: admission with
// a token bucket per tenant, bounded tenant queues served by weighted
// deficit round robin, and the single sequencer of D03 that assigns
// offsets, classifies late events by the watermark rules of §4.4, writes
// the log with group commit (D05) and acks producers only after the fsync.
// Every drop is counted per tenant and cause.
package ingest

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

// Cause is a drop cause of §4.1.
type Cause uint8

// Drop causes of §4.1, closed.
const (
	CauseQuota Cause = iota
	CauseQueueFull
	CauseInvalid
	CauseTooLarge
	CauseUnknownTenant
	CauseShuttingDown
	// NumCauses is the number of causes and the length of Stats.Dropped.
	NumCauses
)

var causeNames = [NumCauses]string{"quota", "queue_full", "invalid", "too_large", "unknown_tenant", "shutting_down"}

// String returns the cause as §4.1 spells it.
func (c Cause) String() string {
	if c < NumCauses {
		return causeNames[c]
	}
	return fmt.Sprintf("cause(%d)", uint8(c))
}

// Defaults of §4.4, D05 and the tenant configuration.
const (
	DefaultBucketMs      = 1000
	DefaultLatenessMs    = 2000
	DefaultIdleGraceMs   = 5000
	DefaultFsyncMaxBytes = 4 << 20
	DefaultFsyncMaxDelay = 2 * time.Millisecond
	DefaultQueueEvents   = 200_000

	// MaxFutureMs is how far ahead of the server clock an event time may be
	// before its batch is invalid (§4.1).
	MaxFutureMs = 10 * 60 * 1000

	// maxBurst keeps the token arithmetic, in nano-events, inside int64.
	maxBurst = 1_000_000_000
	// maxWeight keeps weight·quantumEvents inside int64 with room to spare.
	maxWeight = 1_000_000
)

// TenantConfig configures one tenant. Zero Burst, Weight and QueueEvents
// take their defaults: QuotaEPS, 1 and DefaultQueueEvents.
type TenantConfig struct {
	ID string
	// QuotaEPS is the sustained admission rate in events per second.
	QuotaEPS int64
	// Burst is the token bucket capacity in events.
	Burst int64
	// Weight scales the tenant's DRR quantum.
	Weight int64
	// QueueEvents bounds the events waiting in the tenant queue.
	QueueEvents int64
}

// Config configures a Sequencer.
type Config struct {
	Tenants []TenantConfig
	// BucketMs is S, LatenessMs is L and IdleGraceMs is G of §4.4; G = 0
	// disables TICK records.
	BucketMs    int64
	LatenessMs  int64
	IdleGraceMs int64
	// FsyncMaxBytes and FsyncMaxDelay trigger a group commit: pending log
	// bytes, or time since the first record that is not yet synced.
	FsyncMaxBytes int
	FsyncMaxDelay time.Duration
}

// DefaultConfig returns a configuration with the defaults and no tenants.
func DefaultConfig() Config {
	return Config{
		BucketMs:      DefaultBucketMs,
		LatenessMs:    DefaultLatenessMs,
		IdleGraceMs:   DefaultIdleGraceMs,
		FsyncMaxBytes: DefaultFsyncMaxBytes,
		FsyncMaxDelay: DefaultFsyncMaxDelay,
	}
}

// normalized validates c and returns a copy with tenant defaults filled in.
func (c Config) normalized() (Config, error) {
	switch {
	case c.BucketMs <= 0:
		return c, errors.New("ingest: bucket_ms must be positive")
	case c.LatenessMs < 0:
		return c, errors.New("ingest: lateness_ms must not be negative")
	case c.IdleGraceMs < 0:
		return c, errors.New("ingest: idle_grace_ms must not be negative")
	case c.FsyncMaxBytes <= 0:
		return c, errors.New("ingest: fsync max bytes must be positive")
	case c.FsyncMaxDelay < 0:
		return c, errors.New("ingest: fsync max delay must not be negative")
	}
	c.Tenants = slices.Clone(c.Tenants)
	seen := make(map[string]bool, len(c.Tenants))
	for i := range c.Tenants {
		t := &c.Tenants[i]
		if t.ID == "" {
			return c, errors.New("ingest: tenant with empty id")
		}
		if seen[t.ID] {
			return c, fmt.Errorf("ingest: duplicate tenant %q", t.ID)
		}
		seen[t.ID] = true
		if t.QuotaEPS < 0 || t.Burst < 0 || t.Weight < 0 || t.QueueEvents < 0 {
			return c, fmt.Errorf("ingest: tenant %q has a negative setting", t.ID)
		}
		if t.Burst == 0 {
			t.Burst = t.QuotaEPS
		}
		if t.Weight == 0 {
			t.Weight = 1
		}
		if t.QueueEvents == 0 {
			t.QueueEvents = DefaultQueueEvents
		}
		if t.Burst > maxBurst || t.QuotaEPS > maxBurst {
			return c, fmt.Errorf("ingest: tenant %q quota or burst above %d", t.ID, maxBurst)
		}
		if t.Weight > maxWeight {
			return c, fmt.Errorf("ingest: tenant %q weight above %d", t.ID, maxWeight)
		}
	}
	return c, nil
}
