// Package serversentevents implements Server-Sent Events (SSE) with
// resumable streams. The Broker numbers every event and keeps a bounded
// history. A client that reconnects with a Last-Event-ID header gets exactly
// the events it missed, then the live stream. A Client that parses the
// stream and reconnects automatically is included.
package serversentevents

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Event is one SSE message.
type Event struct {
	ID   uint64
	Type string // "" means the default "message" type
	Data string
}

// ResetEvent is sent when a client's Last-Event-ID is older than the
// retained history (or unknown, for example after a server restart). The
// client should refetch full state, because the events after it are only
// what history still holds.
const ResetEvent = "reset"

// Broker fans published events out to every connected SSE stream.
type Broker struct {
	// Heartbeat sends a comment line this often, so proxies don't close idle
	// connections. 0 means 15s.
	Heartbeat time.Duration
	// Retry is the reconnection delay advertised to clients. 0 means 1s.
	Retry time.Duration
	// BufferSize is the per-subscriber queue. A subscriber whose queue is
	// full is disconnected, and it catches up from history on reconnect. 0 means 64.
	BufferSize int

	historySize int

	mu      sync.Mutex
	nextID  uint64
	history []Event
	subs    map[chan Event]struct{}
	closed  bool
}

// NewBroker keeps the last historySize events for replay (min 1).
func NewBroker(historySize int) *Broker {
	return &Broker{historySize: max(historySize, 1), subs: map[chan Event]struct{}{}}
}

// Publish assigns the next ID, records the event, and delivers it to every
// subscriber without blocking.
func (b *Broker) Publish(typ, data string) Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextID++
	ev := Event{ID: b.nextID, Type: typ, Data: data}
	b.history = append(b.history, ev)
	if len(b.history) > b.historySize {
		b.history = b.history[len(b.history)-b.historySize:]
	}
	for ch := range b.subs {
		select {
		case ch <- ev:
		default: // slow consumer: drop it. It resumes from history.
			delete(b.subs, ch)
			close(ch)
		}
	}
	return ev
}

// subscribe registers a channel and returns the backlog after lastID. It
// does both under one lock, so no event is missed or duplicated between
// replay and live delivery.
func (b *Broker) subscribe(lastID uint64, resume bool) (backlog []Event, reset bool, ch chan Event, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, false, nil, false
	}
	if resume && len(b.history) > 0 {
		oldest := b.history[0].ID
		reset = lastID+1 < oldest || lastID > b.nextID
	} else if resume && lastID != b.nextID {
		reset = true
	}
	for _, ev := range b.history {
		if resume && (reset || ev.ID > lastID) {
			backlog = append(backlog, ev)
		}
	}
	size := b.BufferSize
	if size <= 0 {
		size = 64
	}
	ch = make(chan Event, size)
	b.subs[ch] = struct{}{}
	return backlog, reset, ch, true
}

func (b *Broker) unsubscribe(ch chan Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.subs[ch]; ok {
		delete(b.subs, ch)
		close(ch)
	}
}

// Close ends every stream and refuses new ones.
func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	for ch := range b.subs {
		delete(b.subs, ch)
		close(ch)
	}
}

// ServeHTTP streams events. New clients get only live events. Reconnecting
// clients (Last-Event-ID header, or ?lastEventId= for clients that can't set
// headers) get a replay first.
func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rc := http.NewResponseController(w)
	raw := r.Header.Get("Last-Event-ID")
	if raw == "" {
		raw = r.URL.Query().Get("lastEventId")
	}
	lastID, err := strconv.ParseUint(raw, 10, 64)
	resume := raw != "" && err == nil

	backlog, reset, ch, ok := b.subscribe(lastID, resume)
	if !ok {
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
		return
	}
	defer b.unsubscribe(ch)

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no") // disable proxy buffering (nginx)
	w.WriteHeader(http.StatusOK)

	retry, beat := b.Retry, b.Heartbeat
	if retry <= 0 {
		retry = time.Second
	}
	if beat <= 0 {
		beat = 15 * time.Second
	}
	fmt.Fprintf(w, "retry: %d\n\n", retry.Milliseconds())
	if reset {
		writeEvent(w, Event{Type: ResetEvent, Data: "history unavailable, refetch state"})
	}
	for _, ev := range backlog {
		writeEvent(w, ev)
	}
	if rc.Flush() != nil {
		return // streaming not supported by this ResponseWriter
	}

	ticker := time.NewTicker(beat)
	defer ticker.Stop()
	for {
		select {
		case ev, open := <-ch:
			if !open {
				return // dropped as slow, or broker closed
			}
			writeEvent(w, ev)
		case <-ticker.C:
			_, _ = io.WriteString(w, ": ping\n\n")
		case <-r.Context().Done():
			return
		}
		if rc.Flush() != nil {
			return
		}
	}
}

// writeEvent encodes ev in SSE wire format. Multi-line data becomes
// several data: lines. Newlines are stripped from the type so a payload
// can't inject fields.
func writeEvent(w io.Writer, ev Event) {
	var sb strings.Builder
	if ev.ID != 0 { // ID 0 leaves the client's last ID untouched
		fmt.Fprintf(&sb, "id: %d\n", ev.ID)
	}
	if t := strings.NewReplacer("\r", "", "\n", "").Replace(ev.Type); t != "" {
		fmt.Fprintf(&sb, "event: %s\n", t)
	}
	for _, line := range strings.Split(strings.ReplaceAll(ev.Data, "\r\n", "\n"), "\n") {
		fmt.Fprintf(&sb, "data: %s\n", strings.TrimSuffix(line, "\r"))
	}
	sb.WriteString("\n")
	_, _ = io.WriteString(w, sb.String())
}

// Client consumes an SSE stream and reconnects with Last-Event-ID.
type Client struct {
	HTTP       *http.Client  // nil means http.DefaultClient
	RetryDelay time.Duration // used until the server sends retry:. 0 means 1s
	// OnConnect, if set, is called once each connection is established
	// (the server has registered the stream), with the Last-Event-ID sent.
	OnConnect func(lastEventID string)

	mu     sync.Mutex
	lastID string
}

// LastEventID is the ID of the last event received.
func (c *Client) LastEventID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastID
}

// Subscribe streams events from url to handle until ctx is done,
// reconnecting after any disconnect. It returns ctx.Err().
func (c *Client) Subscribe(ctx context.Context, url string, handle func(Event)) error {
	httpc := c.HTTP
	if httpc == nil {
		httpc = http.DefaultClient
	}
	delay := c.RetryDelay
	if delay <= 0 {
		delay = time.Second
	}
	for {
		// Streams always end with some error (EOF, reset). Keep the retry hint anyway.
		if d, _ := c.stream(ctx, httpc, url, handle); d > 0 {
			delay = d
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// stream reads one connection until it ends and returns any server retry hint.
func (c *Client) stream(ctx context.Context, httpc *http.Client, url string, handle func(Event)) (time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "text/event-stream")
	sentID := c.LastEventID()
	if sentID != "" {
		req.Header.Set("Last-Event-ID", sentID)
	}
	resp, err := httpc.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %d", resp.StatusCode)
	}
	if c.OnConnect != nil {
		c.OnConnect(sentID)
	}

	var retry time.Duration
	var ev Event
	var data []string
	var idStr string
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" { // blank line: dispatch
			if idStr != "" {
				c.mu.Lock()
				c.lastID = idStr
				c.mu.Unlock()
				ev.ID, _ = strconv.ParseUint(idStr, 10, 64)
			}
			if data != nil {
				ev.Data = strings.Join(data, "\n")
				handle(ev)
			}
			ev, data, idStr = Event{}, nil, ""
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "": // comment / heartbeat
		case "id":
			idStr = value
		case "event":
			ev.Type = value
		case "data":
			data = append(data, value)
		case "retry":
			if ms, err := strconv.Atoi(value); err == nil && ms > 0 {
				retry = time.Duration(ms) * time.Millisecond
			}
		}
	}
	return retry, sc.Err()
}
