package config

import (
	"reflect"
	"strings"
	"testing"

	"github.com/CesarManzoCode/oteadero/internal/vfs"
)

func TestDefaults(t *testing.T) {
	c, err := Parse([]byte(`{"data_dir": "d"}`))
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		DataDir:              "d",
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
	if !reflect.DeepEqual(c, want) {
		t.Fatalf("got %+v, want %+v", c, want)
	}
}

func TestPresentFieldsOverrideDefaults(t *testing.T) {
	c, err := Parse([]byte(`{
		"data_dir": "d",
		"listen": "127.0.0.1:0",
		"idle_grace_ms": 0,
		"memory_limit_bytes": 1024,
		"tenants": [{
			"id": "a", "quota_eps": 200000, "burst": 300000, "weight": 2, "queue_events": 5,
			"quantile_metrics": ["lat"], "cardinality_fields": ["user"]
		}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != "127.0.0.1:0" || c.IdleGraceMs != 0 || c.MemoryLimitBytes != 1024 || c.BucketMs != 1000 {
		t.Fatalf("config = %+v", c)
	}
	want := []Tenant{{
		ID: "a", QuotaEPS: 200000, Burst: 300000, Weight: 2, QueueEvents: 5,
		QuantileMetrics: []string{"lat"}, CardinalityFields: []string{"user"},
	}}
	if !reflect.DeepEqual(c.Tenants, want) {
		t.Fatalf("tenants = %+v", c.Tenants)
	}
}

func TestRejected(t *testing.T) {
	for _, tc := range []struct{ name, json, want string }{
		{"unknown field", `{"data_dir": "d", "nope": 1}`, "nope"},
		{"unknown tenant field", `{"data_dir": "d", "tenants": [{"id": "a", "quota_eps": 1, "nope": 1}]}`, "nope"},
		{"trailing data", `{"data_dir": "d"} {}`, "after the object"},
		{"not json", `{`, "config:"},
		{"no data dir", `{}`, "data_dir"},
		{"zero segment", `{"data_dir": "d", "segment_bytes": 0}`, "segment_bytes"},
		{"zero shards", `{"data_dir": "d", "shards": 0}`, "shards"},
		{"negative lateness", `{"data_dir": "d", "lateness_ms": -1}`, "lateness_ms"},
		{"no quota", `{"data_dir": "d", "tenants": [{"id": "a"}]}`, "quota_eps"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.json))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestLoad(t *testing.T) {
	mem := vfs.NewMem()
	if err := mem.MkdirAll("etc"); err != nil {
		t.Fatal(err)
	}
	f, err := mem.Create("etc/serve.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte(`{"data_dir": "d", "shards": 8}`)); err != nil {
		t.Fatal(err)
	}
	f.Close()

	c, err := Load(mem, "etc/serve.json")
	if err != nil {
		t.Fatal(err)
	}
	if c.DataDir != "d" || c.Shards != 8 {
		t.Fatalf("config = %+v", c)
	}
	if _, err := Load(mem, "etc/missing.json"); err == nil {
		t.Fatal("missing file loaded")
	}
}
