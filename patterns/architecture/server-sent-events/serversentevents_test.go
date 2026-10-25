package serversentevents

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWriteEventFormat(t *testing.T) {
	tests := []struct {
		name string
		ev   Event
		want string
	}{
		{"simple", Event{ID: 1, Data: "hi"}, "id: 1\ndata: hi\n\n"},
		{"typed", Event{ID: 2, Type: "price", Data: "42"}, "id: 2\nevent: price\ndata: 42\n\n"},
		{"multi-line data", Event{ID: 3, Data: "a\nb\r\nc"}, "id: 3\ndata: a\ndata: b\ndata: c\n\n"},
		{"type cannot inject fields", Event{ID: 4, Type: "x\ndata: evil", Data: "ok"}, "id: 4\nevent: xdata: evil\ndata: ok\n\n"},
		{"no id", Event{Type: ResetEvent, Data: "r"}, "event: reset\ndata: r\n\n"},
		{"empty data", Event{ID: 5}, "id: 5\ndata: \n\n"},
	}
	for _, tc := range tests {
		var sb strings.Builder
		writeEvent(&sb, tc.ev)
		if sb.String() != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, sb.String(), tc.want)
		}
	}
}

// collect subscribes with a Client and forwards events to a channel.
func collect(t *testing.T, url string, lastID string) (<-chan Event, context.CancelFunc, *Client) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan Event, 100)
	c := &Client{RetryDelay: 10 * time.Millisecond}
	c.lastID = lastID
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Subscribe(ctx, url, func(ev Event) { out <- ev })
	}()
	t.Cleanup(func() { cancel(); <-done })
	return out, cancel, c
}

func expect(t *testing.T, ch <-chan Event, want ...string) {
	t.Helper()
	for _, w := range want {
		select {
		case ev := <-ch:
			got := fmt.Sprintf("%d:%s", ev.ID, ev.Data)
			if ev.Type == ResetEvent {
				got = "reset"
			}
			if got != w {
				t.Fatalf("got %s, want %s", got, w)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for %s", w)
		}
	}
}

// waitSubs waits until the broker has n live subscribers.
func waitSubs(t *testing.T, b *Broker, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		b.mu.Lock()
		got := len(b.subs)
		b.mu.Unlock()
		if got == n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("broker never reached %d subscribers", n)
}

func TestReplayFromLastEventID(t *testing.T) {
	tests := []struct {
		name    string
		history int
		publish int
		lastID  string
		want    []string
	}{
		{"fresh client gets only live events", 10, 3, "", []string{"4:e4"}},
		{"resume replays the gap", 10, 3, "1", []string{"2:e2", "3:e3", "4:e4"}},
		{"up to date, nothing replayed", 10, 3, "3", []string{"4:e4"}},
		{"too old: reset, then what history has", 2, 5, "1", []string{"reset", "4:e4", "5:e5", "6:e6"}},
		{"from the future (server restarted)", 10, 2, "99", []string{"reset", "1:e1", "2:e2", "3:e3"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBroker(tc.history)
			srv := httptest.NewServer(b)
			defer srv.Close()
			defer b.Close()
			for i := 1; i <= tc.publish; i++ {
				b.Publish("", fmt.Sprint("e", i))
			}
			ch, _, _ := collect(t, srv.URL, tc.lastID)
			waitSubs(t, b, 1)
			b.Publish("", fmt.Sprint("e", tc.publish+1))
			expect(t, ch, tc.want...)
		})
	}
}

func TestClientReconnectsWithoutLossOrDuplicates(t *testing.T) {
	b := NewBroker(100)
	b.Retry = 10 * time.Millisecond
	srv := httptest.NewServer(b)
	defer srv.Close()
	defer b.Close()

	ch, _, client := collect(t, srv.URL, "")
	waitSubs(t, b, 1)
	b.Publish("", "a")
	b.Publish("", "b")
	expect(t, ch, "1:a", "2:b")

	srv.CloseClientConnections() // network blip
	waitSubs(t, b, 0)
	b.Publish("", "c") // published while the client is disconnected
	b.Publish("", "d")
	waitSubs(t, b, 1) // client reconnected with Last-Event-ID: 2
	b.Publish("", "e")
	expect(t, ch, "3:c", "4:d", "5:e")
	if client.LastEventID() != "5" {
		t.Errorf("last id = %s", client.LastEventID())
	}
	select {
	case ev := <-ch:
		t.Errorf("unexpected extra event %+v", ev)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestSlowSubscriberIsDropped(t *testing.T) {
	b := NewBroker(10)
	b.BufferSize = 2
	_, _, ch, _ := b.subscribe(0, false)
	for i := 0; i < 3; i++ { // the third publish overflows the buffer
		b.Publish("", "x")
	}
	n := 0
	for range ch { // drains 2 buffered events, then sees the close
		n++
	}
	if n != 2 || len(b.subs) != 0 {
		t.Errorf("got %d events, %d subs", n, len(b.subs))
	}
	b.unsubscribe(ch) // safe after the drop
}

func TestCloseEndsStreamsAndRefusesNew(t *testing.T) {
	b := NewBroker(10)
	srv := httptest.NewServer(b)
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("content-type = %s", ct)
	}
	b.Close()
	body, _ := io.ReadAll(resp.Body) // returns because the stream ended
	resp.Body.Close()
	if !strings.HasPrefix(string(body), "retry: 1000\n\n") {
		t.Errorf("body = %q", body)
	}
	resp, err = http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("after close status = %d", resp.StatusCode)
	}
}

func TestClientParser(t *testing.T) {
	raw := ": comment\n" +
		"retry: 5\n" +
		"id: 7\nevent: note\ndata: line1\ndata: line2\n\n" +
		"event: ignored-no-data\n\n" +
		"data:no-space\n\n" + // inherits last id 7
		"id: 9\ndata: x\n\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, raw)
	}))
	defer srv.Close()
	c := &Client{}
	var got []Event
	retry, _ := c.stream(context.Background(), http.DefaultClient, srv.URL, func(ev Event) { got = append(got, ev) })
	want := []Event{{ID: 7, Type: "note", Data: "line1\nline2"}, {Data: "no-space"}, {ID: 9, Data: "x"}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
	if retry != 5*time.Millisecond || c.LastEventID() != "9" {
		t.Errorf("retry=%v last=%s", retry, c.LastEventID())
	}
}
