// Command otrecv is the test notification receiver of §4.7. It fences stale
// epochs, deduplicates by Idempotency-Key and writes one JSONL line per
// request, synced before the response, so a 200 means the line is on disk.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	usage = "usage: otrecv --listen <addr> --log <file.jsonl>\n"

	statusAccepted  = "accepted"
	statusDuplicate = "duplicate"
	statusStale     = "stale"

	// statusInvalid marks a request without a key or a valid epoch, which
	// §4.7 does not cover; it is logged and counted like the others.
	statusInvalid = "invalid"

	maxBody = 1 << 20
)

// logLine is the JSONL record of §4.7.
type logLine struct {
	RecvMs    int64           `json:"recv_ms"`
	Key       string          `json:"key"`
	Epoch     uint64          `json:"epoch"`
	Evaluator string          `json:"evaluator"`
	Status    string          `json:"status"`
	Body      json.RawMessage `json:"body"`
}

type receiver struct {
	mu       sync.Mutex
	log      *os.File
	now      func() time.Time
	maxEpoch uint64
	accepted map[string]bool
	counts   map[string]int64
}

func newReceiver(log *os.File, now func() time.Time) *receiver {
	return &receiver{
		log:      log,
		now:      now,
		accepted: map[string]bool{},
		counts: map[string]int64{
			statusAccepted: 0, statusDuplicate: 0, statusStale: 0, statusInvalid: 0,
		},
	}
}

func (r *receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch {
	case req.URL.Path == "/notify" && req.Method == http.MethodPost:
		r.notify(w, req)
	case req.URL.Path == "/stats" && req.Method == http.MethodGet:
		r.stats(w)
	default:
		http.NotFound(w, req)
	}
}

func (r *receiver) notify(w http.ResponseWriter, req *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(req.Body, maxBody))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	key := req.Header.Get("Idempotency-Key")
	epoch, epochErr := strconv.ParseUint(req.Header.Get("X-Oteadero-Epoch"), 10, 64)
	var body struct {
		Evaluator string `json:"evaluator"`
	}
	bodyErr := json.Unmarshal(raw, &body)
	line := logLine{Key: key, Epoch: epoch, Evaluator: body.Evaluator, Body: raw}
	if bodyErr != nil {
		// Keep what arrived, quoted, so the log line stays valid JSON.
		line.Body, _ = json.Marshal(string(raw))
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	code := http.StatusOK
	switch {
	case key == "" || epochErr != nil || bodyErr != nil:
		line.Status, code = statusInvalid, http.StatusBadRequest
	case epoch < r.maxEpoch:
		line.Status, code = statusStale, http.StatusConflict
	case r.accepted[key]:
		line.Status = statusDuplicate
	default:
		line.Status = statusAccepted
	}
	line.RecvMs = r.now().UnixMilli()
	if err := r.append(line); err != nil {
		// Nothing changes: without the line on disk the request did not happen.
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if line.Status == statusAccepted || line.Status == statusDuplicate {
		r.maxEpoch = max(r.maxEpoch, epoch)
	}
	if line.Status == statusAccepted {
		r.accepted[key] = true
	}
	r.counts[line.Status]++
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	fmt.Fprintf(w, "{\"status\":%q}\n", line.Status)
}

func (r *receiver) append(line logLine) error {
	data, err := json.Marshal(line)
	if err != nil {
		return err
	}
	if _, err := r.log.Write(append(data, '\n')); err != nil {
		return err
	}
	return r.log.Sync()
}

func (r *receiver) stats(w http.ResponseWriter) {
	r.mu.Lock()
	data, err := json.Marshal(r.counts)
	r.mu.Unlock()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(append(data, '\n'))
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run returns 1 on failure and 2 on bad usage; it serves until it fails.
func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("otrecv", flag.ContinueOnError)
	flags.SetOutput(stderr)
	listen := flags.String("listen", "", "address to listen on, such as 127.0.0.1:0")
	logPath := flags.String("log", "", "JSONL file, appended to")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *listen == "" || *logPath == "" || flags.NArg() != 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	log, err := os.OpenFile(*logPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		fmt.Fprintf(stderr, "otrecv: %v\n", err)
		return 1
	}
	defer log.Close()
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintf(stderr, "otrecv: %v\n", err)
		return 1
	}
	// The actual address, so that a port 0 can be read by the caller.
	fmt.Fprintf(stdout, "listening %s\n", ln.Addr())
	err = http.Serve(ln, newReceiver(log, time.Now))
	fmt.Fprintf(stderr, "otrecv: %v\n", err)
	return 1
}
