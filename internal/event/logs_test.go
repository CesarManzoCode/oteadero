package event

import (
	"errors"
	"strings"
	"testing"
)

func TestParseLogsValid(t *testing.T) {
	body := `{"ts": 1700000000000, "labels": {"svc": "api", "env": "prod"}, "fields": {"user": "u\"1"}, "msg": "hello"}` + "\n" +
		"\n" +
		`{"ts": 0, "msg": "bare"}` + "\r\n" +
		`  {"msg": "multi\nline", "labels": {"a": "x\\y"}, "ts": 5}  `
	got, err := ParseLogs(nil, []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	want := []Log{
		{1700000000000, `{env="prod",svc="api"}`, `{user="u\"1"}`, "hello"},
		{0, `{}`, `{}`, "bare"},
		{5, `{a="x\\y"}`, `{}`, "multi\nline"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d events, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("event %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestParseLogsInvalid(t *testing.T) {
	cases := map[string]string{
		"not json":            `ts=1 msg=x`,
		"missing ts":          `{"msg": "x"}`,
		"missing msg":         `{"ts": 1}`,
		"negative ts":         `{"ts": -1, "msg": "x"}`,
		"float ts":            `{"ts": 1.5, "msg": "x"}`,
		"string ts":           `{"ts": "1", "msg": "x"}`,
		"non-string label":    `{"ts": 1, "msg": "x", "labels": {"a": 1}}`,
		"bad label name":      `{"ts": 1, "msg": "x", "labels": {"a-b": "1"}}`,
		"bad field name":      `{"ts": 1, "msg": "x", "fields": {"": "1"}}`,
		"unknown key":         `{"ts": 1, "msg": "x", "level": "info"}`,
		"two objects":         `{"ts": 1, "msg": "x"} {"ts": 2, "msg": "y"}`,
		"array":               `[1, 2]`,
		"unterminated":        `{"ts": 1, "msg": "x"`,
		"invalid second line": "{\"ts\": 1, \"msg\": \"x\"}\n{\"ts\": 2}\n",
	}
	for name, body := range cases {
		dst := []Log{{Msg: "keep"}}
		got, err := ParseLogs(dst, []byte(body))
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: got err %v, want ErrInvalid", name, err)
		}
		if len(got) != 1 || got[0].Msg != "keep" {
			t.Errorf("%s: dst changed to %v", name, got)
		}
	}
}

func TestParseLogsTooLarge(t *testing.T) {
	line := `{"ts": 1, "msg": "x"}` + "\n"
	body := strings.Repeat(line, MaxEvents)
	got, err := ParseLogs(nil, []byte(body))
	if err != nil || len(got) != MaxEvents {
		t.Fatalf("at the limit: %d events, %v", len(got), err)
	}
	if _, err := ParseLogs(nil, []byte(body+line)); !errors.Is(err, ErrTooLarge) {
		t.Errorf("over the limit: got %v, want ErrTooLarge", err)
	}
}
