// Package config is the JSON configuration of `oteadero serve`.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/CesarManzoCode/oteadero/internal/vfs"
)

// Tenant configures one tenant. Zero Burst, Weight and QueueEvents take the
// defaults of the ingest package.
type Tenant struct {
	ID                string   `json:"id"`
	QuotaEPS          int64    `json:"quota_eps"`
	Burst             int64    `json:"burst"`
	Weight            int64    `json:"weight"`
	QueueEvents       int64    `json:"queue_events"`
	QuantileMetrics   []string `json:"quantile_metrics"`
	CardinalityFields []string `json:"cardinality_fields"`
}

// Config is the configuration of serve.
type Config struct {
	DataDir              string   `json:"data_dir"`
	Listen               string   `json:"listen"`
	BucketMs             int64    `json:"bucket_ms"`
	LatenessMs           int64    `json:"lateness_ms"`
	IdleGraceMs          int64    `json:"idle_grace_ms"`
	Shards               int      `json:"shards"`
	FsyncMaxDelayMs      int64    `json:"fsync_max_delay_ms"`
	FsyncMaxBytes        int      `json:"fsync_max_bytes"`
	CheckpointIntervalMs int64    `json:"checkpoint_interval_ms"`
	CheckpointEvents     int64    `json:"checkpoint_events"`
	SegmentBytes         int64    `json:"segment_bytes"`
	MaxBodyBytes         int64    `json:"max_body_bytes"`
	TailMaxLagEvents     int64    `json:"tail_max_lag_events"`
	TailRetentionEvents  int64    `json:"tail_retention_events"`
	LeaseTTLMs           int64    `json:"lease_ttl_ms"`
	MemoryLimitBytes     int64    `json:"memory_limit_bytes"`
	Tenants              []Tenant `json:"tenants"`
}

// Default returns the configuration with every default set and no data
// directory or tenants.
func Default() Config {
	return Config{
		Listen:               "127.0.0.1:7070",
		BucketMs:             1000,
		LatenessMs:           2000,
		IdleGraceMs:          5000,
		Shards:               4,
		FsyncMaxDelayMs:      2,
		FsyncMaxBytes:        4194304,
		CheckpointIntervalMs: 10000,
		CheckpointEvents:     2000000,
		SegmentBytes:         268435456,
		MaxBodyBytes:         16777216,
		TailMaxLagEvents:     1000000,
		TailRetentionEvents:  50000000,
		LeaseTTLMs:           2000,
	}
}

// Parse decodes a configuration over the defaults, so that a field that is
// present keeps its value even when it is zero (idle_grace_ms = 0 means no
// ticks). An unknown field or anything after the object is an error.
func Parse(data []byte) (Config, error) {
	c := Default()
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return c, fmt.Errorf("config: %w", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return c, errors.New("config: unexpected data after the object")
	}
	return c, c.Validate()
}

// Load reads and parses the configuration file at path.
func Load(fsys vfs.FS, path string) (Config, error) {
	f, err := fsys.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	data := make([]byte, size)
	if _, err := f.ReadAt(data, 0); err != nil && err != io.EOF {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	return Parse(data)
}

// Validate checks the settings that later layers would read differently: a
// zero segment size, for one, would silently become the default. Duplicate
// tenants and the limits of a tenant are checked by the sequencer.
func (c *Config) Validate() error {
	if c.DataDir == "" {
		return errors.New("config: data_dir is required")
	}
	if c.Listen == "" {
		return errors.New("config: listen is required")
	}
	for _, f := range []struct {
		name  string
		value int64
	}{
		{"bucket_ms", c.BucketMs},
		{"shards", int64(c.Shards)},
		{"fsync_max_bytes", int64(c.FsyncMaxBytes)},
		{"checkpoint_interval_ms", c.CheckpointIntervalMs},
		{"checkpoint_events", c.CheckpointEvents},
		{"segment_bytes", c.SegmentBytes},
		{"max_body_bytes", c.MaxBodyBytes},
		{"tail_max_lag_events", c.TailMaxLagEvents},
		{"tail_retention_events", c.TailRetentionEvents},
		{"lease_ttl_ms", c.LeaseTTLMs},
	} {
		if f.value <= 0 {
			return fmt.Errorf("config: %s must be positive", f.name)
		}
	}
	for _, f := range []struct {
		name  string
		value int64
	}{
		{"lateness_ms", c.LatenessMs},
		{"idle_grace_ms", c.IdleGraceMs},
		{"fsync_max_delay_ms", c.FsyncMaxDelayMs},
		{"memory_limit_bytes", c.MemoryLimitBytes},
	} {
		if f.value < 0 {
			return fmt.Errorf("config: %s must not be negative", f.name)
		}
	}
	for i := range c.Tenants {
		if c.Tenants[i].QuotaEPS <= 0 {
			return fmt.Errorf("config: tenant %q needs a positive quota_eps", c.Tenants[i].ID)
		}
	}
	return nil
}
