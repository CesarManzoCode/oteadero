package eval

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// requestTimeout bounds an HTTP request. A lease reply later than this is
	// close to worthless anyway: its lease counts from the send.
	requestTimeout = 2 * time.Second
	// maxReplyBytes bounds what is read of an answer.
	maxReplyBytes = 1 << 16
)

// HTTPTransport is the Transport of production: each request runs in its own
// goroutine and its reply waits on Replies for the goroutine that owns the
// Notifier.
type HTTPTransport struct {
	client      *http.Client
	coordinator string
	receiver    string
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	replies     chan func(*Notifier)
}

var _ Transport = (*HTTPTransport)(nil)

// NewHTTPTransport returns a transport to the coordinator and receiver URLs.
func NewHTTPTransport(coordinator, receiver string) *HTTPTransport {
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.MaxIdleConnsPerHost = maxInflight
	ctx, cancel := context.WithCancel(context.Background())
	return &HTTPTransport{
		client:      &http.Client{Transport: base, Timeout: requestTimeout},
		coordinator: strings.TrimRight(coordinator, "/"),
		receiver:    strings.TrimRight(receiver, "/"),
		ctx:         ctx,
		cancel:      cancel,
		replies:     make(chan func(*Notifier), 2*maxInflight),
	}
}

// Replies delivers the replies to apply to the Notifier.
func (t *HTTPTransport) Replies() <-chan func(*Notifier) { return t.replies }

// RequestLease implements Transport.
func (t *HTTPTransport) RequestLease(r LeaseRequest) {
	t.spawn(func() func(*Notifier) { return t.lease(r) })
}

// Send implements Transport.
func (t *HTTPTransport) Send(x *Notification) {
	t.spawn(func() func(*Notifier) { return t.notify(x) })
}

func (t *HTTPTransport) spawn(request func() func(*Notifier)) {
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		reply := request()
		if reply == nil {
			return
		}
		select {
		case t.replies <- reply:
		case <-t.ctx.Done():
		}
	}()
}

func (t *HTTPTransport) lease(r LeaseRequest) func(*Notifier) {
	// A struct with one string always marshals.
	body, _ := json.Marshal(struct {
		Holder string `json:"holder"`
	}{r.Holder})
	code, data, err := t.post(t.coordinator+"/v1/lease", body, nil)
	if err != nil {
		return nil
	}
	var answer struct {
		Epoch  uint64 `json:"epoch"`
		Holder string `json:"holder"`
		TTLMs  int64  `json:"ttl_ms"`
	}
	if json.Unmarshal(data, &answer) != nil {
		return nil
	}
	switch code {
	case http.StatusOK:
		g := Grant{Epoch: answer.Epoch, Holder: answer.Holder, TTLMs: answer.TTLMs}
		return func(n *Notifier) { n.LeaseGranted(r.SentAt, g) }
	case http.StatusConflict:
		return func(n *Notifier) { n.LeaseDenied(r.SentAt, answer.Holder) }
	}
	return nil
}

func (t *HTTPTransport) notify(x *Notification) func(*Notifier) {
	code, _, err := t.post(t.receiver+"/notify", x.Body(), map[string]string{
		"Idempotency-Key":  x.Key,
		"X-Oteadero-Epoch": strconv.FormatUint(x.Epoch, 10),
	})
	res := ResultFailed
	switch {
	case err != nil:
	case code >= 200 && code < 300:
		res = ResultConfirmed
	case code == http.StatusConflict:
		res = ResultStale
	}
	return func(n *Notifier) { n.Delivered(x.Key, x.Epoch, res) }
}

func (t *HTTPTransport) post(url string, body []byte, headers map[string]string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(t.ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxReplyBytes))
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, data, nil
}

// Close cancels the requests in flight and waits for their goroutines.
func (t *HTTPTransport) Close() {
	t.cancel()
	t.wg.Wait()
	t.client.CloseIdleConnections()
}
