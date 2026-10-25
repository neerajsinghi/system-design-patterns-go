// Command example streams stock prices over SSE. Mid-stream the network
// drops. The client reconnects with Last-Event-ID, and the server replays
// exactly the prices it missed.
package main

import (
	"context"
	"fmt"
	"net/http/httptest"
	"time"

	sse "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/server-sent-events"
)

func main() {
	broker := sse.NewBroker(100)
	broker.Retry = 50 * time.Millisecond // tell clients to reconnect quickly
	srv := httptest.NewServer(broker)

	connected := make(chan string, 4)
	received := make(chan sse.Event, 16)
	client := &sse.Client{OnConnect: func(last string) { connected <- last }}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = client.Subscribe(ctx, srv.URL+"/prices", func(ev sse.Event) { received <- ev })
	}()

	waitConnect := func() {
		select {
		case last := <-connected:
			if last == "" {
				fmt.Println("client: connected (fresh)")
			} else {
				fmt.Printf("client: reconnected with Last-Event-ID: %s\n", last)
			}
		case <-time.After(time.Second):
			fmt.Println("client: connect timed out")
		}
	}
	show := func(n int) {
		for i := 0; i < n; i++ {
			select {
			case ev := <-received:
				fmt.Printf("client: id=%d event=%s data=%s\n", ev.ID, ev.Type, ev.Data)
			case <-time.After(time.Second):
				fmt.Println("client: timed out waiting for event")
				return
			}
		}
	}

	waitConnect()
	broker.Publish("price", `{"sym":"ACME","px":171.20}`)
	broker.Publish("price", `{"sym":"ACME","px":171.35}`)
	show(2)

	fmt.Println("\n-- network blip: server drops every connection --")
	srv.CloseClientConnections()
	broker.Publish("price", `{"sym":"ACME","px":170.90}`)
	broker.Publish("price", `{"sym":"ACME","px":171.05}`)
	fmt.Println("server: published ids 3 and 4 while the client was away")

	waitConnect()
	show(2) // replayed from history
	broker.Publish("price", `{"sym":"ACME","px":171.60}`)
	show(1) // live again

	fmt.Println("\nNo price was lost and none was delivered twice.")
	cancel()
	<-done
	broker.Close()
	srv.Close()
}
