package event

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/CesarManzoCode/oteadero/internal/wal"
)

// metricBatch and metricVector are a metrics BATCH payload written by hand from §4.3.
var metricBatch = Batch{
	Tenant:   "t1",
	Kind:     KindMetrics,
	IngestMs: 1000,
	Metrics: []Metric{
		{Key: `up{a="1"}`, Value: 1, TsMs: 300},
		{Key: `up{}`, Value: -2.5, TsMs: 5},
	},
}

var metricVector = []byte{
	0x02, 't', '1', // tenant
	0x01,                                           // kind: metrics
	0xe8, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ingest_ms 1000
	0x02, // n
	0x09, 'u', 'p', '{', 'a', '=', '"', '1', '"', '}',
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, // 1.0
	0x2c, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ts 300
	0x04, 'u', 'p', '{', '}',
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0xc0, // -2.5
	0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ts 5
}

// logBatch and logVector are a logs BATCH payload written by hand from §4.3.
var logBatch = Batch{
	Tenant:   "a",
	Kind:     KindLogs,
	IngestMs: 0,
	Logs: []Log{
		{TsMs: 7, Labels: `{app="x"}`, Fields: `{}`, Msg: "hi"},
	},
}

var logVector = []byte{
	0x01, 'a', // tenant
	0x02,                                           // kind: logs
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ingest_ms 0
	0x01,                                           // n
	0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ts 7
	0x09, '{', 'a', 'p', 'p', '=', '"', 'x', '"', '}',
	0x02, '{', '}',
	0x02, 'h', 'i',
}

func TestBatchVectors(t *testing.T) {
	for _, c := range []struct {
		name   string
		batch  Batch
		vector []byte
	}{
		{"metrics", metricBatch, metricVector},
		{"logs", logBatch, logVector},
	} {
		got := AppendBatch(nil, &c.batch)
		if !bytes.Equal(got, c.vector) {
			t.Errorf("%s: encoded\n%x\nwant\n%x", c.name, got, c.vector)
		}
		back, err := DecodeBatch(c.vector)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if !reflect.DeepEqual(back, c.batch) {
			t.Errorf("%s: decoded %+v, want %+v", c.name, back, c.batch)
		}
		n, err := wal.BatchEvents(got)
		if err != nil || n != uint64(c.batch.Len()) {
			t.Errorf("%s: wal counts %d events (%v), batch has %d", c.name, n, err, c.batch.Len())
		}
	}
}

func TestBatchRoundTripFromParsers(t *testing.T) {
	ms, err := ParseMetrics(nil, []byte("m{b=\"\\n\",a=\"1\"} NaN 1\nm -Inf 2\nn 3.25 9223372036854775807\n"))
	if err != nil {
		t.Fatal(err)
	}
	b := Batch{Tenant: "tenant", Kind: KindMetrics, IngestMs: -1, Metrics: ms}
	back, err := DecodeBatch(AppendBatch(nil, &b))
	if err != nil {
		t.Fatal(err)
	}
	if len(back.Metrics) != 3 || !math.IsNaN(back.Metrics[0].Value) || back.Metrics[1] != ms[1] || back.Metrics[2] != ms[2] || back.IngestMs != -1 {
		t.Errorf("got %+v, want %+v", back, b)
	}
	ls, err := ParseLogs(nil, []byte(`{"ts": 1, "labels": {"k": "v"}, "fields": {"f": "g"}, "msg": ""}`))
	if err != nil {
		t.Fatal(err)
	}
	lb := Batch{Tenant: "tenant", Kind: KindLogs, Logs: ls}
	if back, err := DecodeBatch(AppendBatch(nil, &lb)); err != nil || !reflect.DeepEqual(back, lb) {
		t.Errorf("logs: got %+v, %v", back, err)
	}
}

func TestDecodeBatchRejects(t *testing.T) {
	badKind := bytes.Clone(metricVector)
	badKind[3] = 3
	hugeN := append(bytes.Clone(logVector[:11]), 0xff, 0xff, 0xff, 0xff, 0x0f)
	cases := map[string][]byte{
		"empty":          nil,
		"truncated":      metricVector[:len(metricVector)-1],
		"trailing bytes": append(bytes.Clone(metricVector), 0),
		"bad kind":       badKind,
		"huge count":     hugeN,
		"short header":   metricVector[:5],
	}
	for name, p := range cases {
		if _, err := DecodeBatch(p); !errors.Is(err, ErrBadBatch) {
			t.Errorf("%s: got %v, want ErrBadBatch", name, err)
		}
	}
}
