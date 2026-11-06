// Package stream is the live interface of §4.9 and D11: the tail of the log
// and the stream of closed buckets, both as Server-Sent Events with resume
// tokens `<logid>-<offset>` and `<logid>-m<seq>`.
package stream

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// Error codes of §4.9, plus the ones of a request that cannot be served.
const (
	codeUnknownLog   = "unknown_log"
	codeExpired      = "expired"
	codeLagged       = "lagged"
	codeInvalid      = "invalid"
	codeShuttingDown = "shutting_down"
)

var errBadToken = errors.New("stream: bad token")

// token is a parsed resume token.
type token struct {
	logID uint64
	live  bool
	n     uint64
}

func formatTail(logID, offset uint64) string {
	return fmt.Sprintf("%016x-%d", logID, offset)
}

func formatLive(logID, seq uint64) string {
	return fmt.Sprintf("%016x-m%d", logID, seq)
}

func parseToken(s string) (token, error) {
	id, rest, ok := strings.Cut(s, "-")
	if !ok || id == "" {
		return token{}, errBadToken
	}
	logID, err := strconv.ParseUint(id, 16, 64)
	if err != nil {
		return token{}, errBadToken
	}
	t := token{logID: logID}
	if after, ok := strings.CutPrefix(rest, "m"); ok {
		t.live = true
		rest = after
	}
	if rest == "" || rest[0] < '0' || rest[0] > '9' {
		return token{}, errBadToken
	}
	if t.n, err = strconv.ParseUint(rest, 10, 64); err != nil {
		return token{}, errBadToken
	}
	return t, nil
}

// requestToken returns the resume token of r: Last-Event-ID, which a
// reconnecting SSE client sends, or else ?after=. ok is false when there is
// none.
func requestToken(r *http.Request) (t token, ok bool, err error) {
	s := r.Header.Get("Last-Event-ID")
	if s == "" {
		s = r.URL.Query().Get("after")
	}
	if s == "" {
		return token{}, false, nil
	}
	t, err = parseToken(s)
	return t, err == nil, err
}

func writeCode(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	fmt.Fprintf(w, "{\"code\":%q}\n", code)
}

// startEvents sends the SSE response header right away, so that a client
// knows its starting point is fixed once it has the response.
func startEvents(w http.ResponseWriter) *http.ResponseController {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	rc.Flush()
	return rc
}

func appendEvent(dst []byte, id string, data []byte) []byte {
	dst = append(dst, "id: "...)
	dst = append(dst, id...)
	dst = append(dst, "\ndata: "...)
	dst = append(dst, data...)
	return append(dst, '\n', '\n')
}

// appendLagged is the `event: error` that ends a subscriber left behind;
// last is the token to resume from, empty if it consumed nothing.
func appendLagged(dst []byte, last string) []byte {
	return fmt.Appendf(dst, "event: error\ndata: {\"code\":%q,\"last\":%q}\n\n", codeLagged, last)
}
