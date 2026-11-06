package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/eval"
	evt "github.com/CesarManzoCode/oteadero/internal/event"
	"github.com/CesarManzoCode/oteadero/internal/ingest"
	"github.com/CesarManzoCode/oteadero/internal/server"
)

// Message and timer kinds of the engine scenario.
const (
	kindMetrics      = "ingest.metrics"
	kindLogs         = "ingest.logs"
	kindIngestReply  = "ingest.reply"
	kindLeaseRequest = "lease.request"
	kindLeaseReply   = "lease.reply"
	kindNotify       = "notify"
	kindNotifyReply  = "notify.reply"
	kindTailRequest  = "tail.request"
	kindTailData     = "tail.data"
	kindServeWake    = "serve.wake"
	kindEvalStep     = "eval.step"
	kindProduce      = "produce"
	kindTailPoll     = "tail.poll"
)

// tailTenant is the tenant whose logs the tail subscriber follows.
const tailTenant = "t1"

type ingestMsg struct {
	Tenant string `json:"tenant"`
	Events int    `json:"events"`
	Body   string `json:"body"`
}

type leaseRequestMsg struct {
	Holder string `json:"holder"`
	SentAt int64  `json:"sent_at"`
}

type leaseReplyMsg struct {
	Granted bool   `json:"granted"`
	Epoch   uint64 `json:"epoch"`
	Holder  string `json:"holder"`
	TTLMs   int64  `json:"ttl_ms"`
	SentAt  int64  `json:"sent_at"`
}

// notifyMsg is the request of §4.7: the two headers and the body.
type notifyMsg struct {
	Key   string          `json:"key"`
	Epoch uint64          `json:"epoch"`
	Body  json.RawMessage `json:"body"`
}

type notifyReplyMsg struct {
	Key    string `json:"key"`
	Epoch  uint64 `json:"epoch"`
	Status int    `json:"status"`
}

type tailRequestMsg struct {
	Seq   uint64 `json:"seq"`
	Token string `json:"token"`
	// Limit is how many bytes the subscriber reads before it disconnects;
	// zero reads everything up to the durable end.
	Limit int `json:"limit"`
}

type tailDataMsg struct {
	Seq    uint64 `json:"seq"`
	Status int    `json:"status"`
	Body   []byte `json:"body"`
}

// encode marshals a message. They are structs of strings and numbers, which
// always marshal.
func encode(v any) []byte {
	p, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return p
}

// decode unmarshals a message of the scenario; a bad one is a bug of the
// harness.
func decode(p []byte, v any) {
	if err := json.Unmarshal(p, v); err != nil {
		panic(fmt.Sprintf("sim: bad message: %v", err))
	}
}

// serveProc is a life of serve: the server of the engine on the node's disk,
// with its sequencer driven from the scheduler instead of its goroutine.
type serveProc struct {
	sc  *scenario
	env *Env
	srv *server.Server
	seq *ingest.Sequencer
	// armed is set while a wake is pending at wakeAt, on the monotonic clock.
	armed  bool
	wakeAt time.Duration
}

func (sc *scenario) startServe(env *Env) Process {
	sc.crashedServe = sc.crashedServe || sc.serve != nil
	p := &serveProc{sc: sc, env: env}
	sc.serve = p
	srv, err := server.New(sc.cfg, server.Deps{FS: env.FS(), Clock: env.Clock(), Rand: env.Rand()})
	if err == nil {
		_, err = srv.Recover()
	}
	if err != nil {
		sc.fail(serveNode, err)
		return p
	}
	p.srv, p.seq = srv, srv.Sequencer()
	p.pump()
	return p
}

func (p *serveProc) Receive(m Message) {
	if p.srv == nil {
		return
	}
	switch m.Kind {
	case kindMetrics, kindLogs:
		var x ingestMsg
		decode(m.Payload, &x)
		p.sc.arrived[x.Tenant] += uint64(x.Events)
		kind := evt.KindMetrics
		if m.Kind == kindLogs {
			kind = evt.KindLogs
		}
		p.srv.Ingest(x.Tenant, kind, []byte(x.Body), nil, func(r server.Reply) {
			p.env.Send(m.From, kindIngestReply, encode(r))
		})
	case kindLeaseRequest:
		var x leaseRequestMsg
		decode(m.Payload, &x)
		p.srv.Coordinator().Acquire(x.Holder, func(d eval.Decision) {
			// A log failure or a shutdown gets no answer, as over HTTP.
			if d.Err != nil {
				return
			}
			p.env.Send(m.From, kindLeaseReply, encode(leaseReplyMsg{
				Granted: d.Granted, Epoch: d.Epoch, Holder: d.Holder, TTLMs: d.TTLMs, SentAt: x.SentAt,
			}))
		})
	case kindTailRequest:
		var x tailRequestMsg
		decode(m.Payload, &x)
		status, body := p.tail(x)
		p.env.Send(m.From, kindTailData, encode(tailDataMsg{Seq: x.Seq, Status: status, Body: body}))
	}
	p.pump()
}

// tail serves one connection of the subscriber with the handler of serve. Its
// context is already cancelled, so the handler sends what is durable after the
// token and returns where it would wait for the next fsync: one connection is
// one request, and the next one resumes from the last complete event.
func (p *serveProc) tail(x tailRequestMsg) (int, []byte) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/v1/tail?tenant="+tailTenant+"&kind=logs", nil)
	if err != nil {
		panic(err)
	}
	if x.Token != "" {
		req.Header.Set("Last-Event-ID", x.Token)
	}
	w := &sseWriter{header: http.Header{}, limit: x.Limit}
	p.srv.Handler().ServeHTTP(w, req)
	return w.status, w.body.Bytes()
}

// pump polls the sequencer while it has work now and arms a wake for the
// next deadline it reports.
func (p *serveProc) pump() {
	for {
		if err := p.seq.Poll(); err != nil {
			p.sc.fail(serveNode, err)
			p.srv = nil
			return
		}
		d, ok := p.seq.NextWake()
		if !ok {
			return
		}
		if d > 0 {
			p.arm(d)
			return
		}
	}
}

// arm sets a wake d from now unless one at or before it is pending. A wake
// that an earlier one replaced still fires, and only polls.
func (p *serveProc) arm(d time.Duration) {
	at := p.env.Clock().Monotonic() + d
	if p.armed && p.wakeAt <= at {
		return
	}
	p.armed, p.wakeAt = true, at
	p.env.After(d, kindServeWake, func() {
		if p.wakeAt == at {
			p.armed = false
		}
		if p.srv != nil {
			p.pump()
		}
	})
}

// errDisconnected is what a write gets once the subscriber has gone.
var errDisconnected = errors.New("sim: subscriber disconnected")

// sseWriter is the connection of a tail subscriber: it takes up to limit
// bytes, if limit is positive, and then fails as a closed connection does.
type sseWriter struct {
	header http.Header
	status int
	body   bytes.Buffer
	limit  int
	cut    bool
}

func (w *sseWriter) Header() http.Header { return w.header }

func (w *sseWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
}

func (w *sseWriter) Write(p []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	if w.cut {
		return 0, errDisconnected
	}
	if w.limit > 0 && w.body.Len()+len(p) > w.limit {
		n := w.limit - w.body.Len()
		w.body.Write(p[:n])
		w.cut = true
		return n, errDisconnected
	}
	return w.body.Write(p)
}

// FlushError is what http.ResponseController calls to flush.
func (w *sseWriter) FlushError() error {
	if w.cut {
		return errDisconnected
	}
	return nil
}

// evalProc is a life of an evaluator: the eval.Core of the engine over the
// synced log of serve, stepped by a timer every eval.PollInterval.
type evalProc struct {
	sc   *scenario
	env  *Env
	core *eval.Core
}

func (sc *scenario) startEvaluator(env *Env) Process {
	id := env.Node()
	p := &evalProc{sc: sc, env: env}
	sc.evals[id] = p
	sc.roles[id] = role{}
	deps := eval.Deps{
		FS:        splitFS{own: env.FS(), log: env.Shared()},
		Clock:     env.Clock(),
		Rand:      env.Rand(),
		ExportDir: exportDir,
	}
	core, err := eval.NewCore(sc.cfg, id, sc.rules, deps, evalTransport{env: env}, func(leader bool, epoch uint64) {
		sc.roles[id] = role{leader: leader, epoch: epoch}
	})
	if err == nil {
		err = core.Recover()
	}
	if err != nil {
		sc.fail(id, err)
		return p
	}
	p.core = core
	env.After(eval.PollInterval, kindEvalStep, p.step)
	return p
}

func (p *evalProc) step() {
	if p.core == nil {
		return
	}
	more, err := p.core.Step()
	if err != nil {
		p.sc.fail(p.env.Node(), err)
		p.core = nil
		return
	}
	d := eval.PollInterval
	if more {
		d = 0
	}
	p.env.After(d, kindEvalStep, p.step)
}

func (p *evalProc) Receive(m Message) {
	if p.core == nil {
		return
	}
	n := p.core.Notifier()
	switch m.Kind {
	case kindLeaseReply:
		var x leaseReplyMsg
		decode(m.Payload, &x)
		if x.Granted {
			n.LeaseGranted(time.Duration(x.SentAt), eval.Grant{Epoch: x.Epoch, Holder: x.Holder, TTLMs: x.TTLMs})
		} else {
			n.LeaseDenied(time.Duration(x.SentAt), x.Holder)
		}
	case kindNotifyReply:
		var x notifyReplyMsg
		decode(m.Payload, &x)
		res := eval.ResultFailed
		switch {
		case x.Status >= 200 && x.Status < 300:
			res = eval.ResultConfirmed
		case x.Status == http.StatusConflict:
			res = eval.ResultStale
		}
		n.Delivered(x.Key, x.Epoch, res)
	}
}

// evalTransport is the eval.Transport of the simulation: requests are
// messages, and the replies come back to evalProc.Receive.
type evalTransport struct{ env *Env }

var _ eval.Transport = evalTransport{}

func (t evalTransport) RequestLease(r eval.LeaseRequest) {
	t.env.Send(serveNode, kindLeaseRequest, encode(leaseRequestMsg{Holder: r.Holder, SentAt: int64(r.SentAt)}))
}

func (t evalTransport) Send(x *eval.Notification) {
	t.env.Send(recvNode, kindNotify, encode(notifyMsg{Key: x.Key, Epoch: x.Epoch, Body: x.Body()}))
}

// ReceiverLine is a line of receiver.jsonl (§4.7).
type ReceiverLine struct {
	RecvMs    int64           `json:"recv_ms"`
	Key       string          `json:"key"`
	Epoch     uint64          `json:"epoch"`
	Evaluator string          `json:"evaluator"`
	Status    string          `json:"status"`
	Body      json.RawMessage `json:"body"`
}

// Statuses of the receiver (§4.7).
const (
	statusAccepted  = "accepted"
	statusDuplicate = "duplicate"
	statusStale     = "stale"
)

// recvProc is the receiver of §4.7, as otrecv: it fences epochs below the
// highest it has seen, answers a key it accepted as a duplicate and logs every
// request. It does not crash in the scenario.
type recvProc struct {
	sc       *scenario
	env      *Env
	maxEpoch uint64
	accepted map[string]bool
}

func (sc *scenario) startReceiver(env *Env) Process {
	return &recvProc{sc: sc, env: env, accepted: map[string]bool{}}
}

func (p *recvProc) Receive(m Message) {
	if m.Kind != kindNotify {
		return
	}
	var x notifyMsg
	decode(m.Payload, &x)
	var body struct {
		Evaluator string `json:"evaluator"`
	}
	decode(x.Body, &body)
	line := ReceiverLine{RecvMs: p.env.Clock().NowMs(), Key: x.Key, Epoch: x.Epoch, Evaluator: body.Evaluator, Body: x.Body}
	code := http.StatusOK
	switch {
	case x.Epoch < p.maxEpoch:
		line.Status, code = statusStale, http.StatusConflict
	case p.accepted[x.Key]:
		line.Status = statusDuplicate
	default:
		line.Status = statusAccepted
		p.accepted[x.Key] = true
	}
	if line.Status != statusStale {
		p.maxEpoch = max(p.maxEpoch, x.Epoch)
	}
	p.sc.receiver = append(p.sc.receiver, line)
	p.env.Send(m.From, kindNotifyReply, encode(notifyReplyMsg{Key: x.Key, Epoch: x.Epoch, Status: code}))
}

// tailSub is the tail subscriber: every tailPoll it connects with the token of
// the last complete event it got, unless the previous connection is still
// open, and now and then it disconnects in the middle of an event.
type tailSub struct {
	sc     *scenario
	env    *Env
	token  string
	seq    uint64
	open   bool
	sentAt time.Duration
}

const (
	tailPoll = 200 * time.Millisecond
	// tailTimeout is when a connection with no answer counts as lost.
	tailTimeout = time.Second
)

func (sc *scenario) startTail(env *Env) Process {
	p := &tailSub{sc: sc, env: env}
	p.poll()
	return p
}

func (p *tailSub) poll() {
	now := p.env.Clock().Monotonic()
	if !p.open || now-p.sentAt >= tailTimeout {
		p.seq++
		p.open, p.sentAt = true, now
		limit := 0
		if p.env.Rand().IntN(4) == 0 {
			limit = 200 + p.env.Rand().IntN(4000)
		}
		p.env.Send(serveNode, kindTailRequest, encode(tailRequestMsg{Seq: p.seq, Token: p.token, Limit: limit}))
	}
	p.env.After(tailPoll, kindTailPoll, p.poll)
}

func (p *tailSub) Receive(m Message) {
	if m.Kind != kindTailData {
		return
	}
	var x tailDataMsg
	decode(m.Payload, &x)
	if x.Seq != p.seq {
		// The answer of a connection given up on.
		return
	}
	p.open = false
	if x.Status != http.StatusOK {
		p.sc.tail.refused = append(p.sc.tail.refused, fmt.Sprintf("%d %s", x.Status, bytes.TrimSpace(x.Body)))
		return
	}
	body := x.Body
	// Only events followed by their blank line arrived whole.
	for {
		ev, rest, ok := bytes.Cut(body, []byte("\n\n"))
		if !ok {
			return
		}
		body = rest
		if err := p.event(string(ev)); err != nil {
			p.sc.tail.refused = append(p.sc.tail.refused, err.Error())
			return
		}
	}
}

// event takes one complete SSE event.
func (p *tailSub) event(ev string) error {
	var id, data string
	isError := false
	for line := range strings.Lines(ev) {
		line = strings.TrimSuffix(line, "\n")
		switch {
		case strings.HasPrefix(line, "id: "):
			id = line[len("id: "):]
		case strings.HasPrefix(line, "data: "):
			data = line[len("data: "):]
		case line == "event: error":
			isError = true
		}
	}
	if isError {
		var x struct {
			Code string `json:"code"`
			Last string `json:"last"`
		}
		if err := json.Unmarshal([]byte(data), &x); err != nil || x.Code != "lagged" {
			return fmt.Errorf("tail error event %q", data)
		}
		if x.Last != "" {
			p.token = x.Last
		}
		return nil
	}
	_, offset, ok := strings.Cut(id, "-")
	o, err := strconv.ParseUint(offset, 10, 64)
	if !ok || err != nil {
		return fmt.Errorf("tail event with id %q", id)
	}
	p.sc.tail.offsets = append(p.sc.tail.offsets, o)
	p.token = id
	return nil
}
