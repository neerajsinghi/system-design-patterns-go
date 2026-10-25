// Command example runs a search across four index shards on loopback.
// One shard is slow and one is broken. The query still answers within its
// 100ms budget with partial results, and says what is missing.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	sg "github.com/neerajsinghi/system-design-patterns-go/patterns/architecture/scatter-gather"
)

func shard(name string, delay time.Duration, fail bool, docs ...string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(delay):
		case <-r.Context().Done(): // the gatherer gave up on us
			return
		}
		if fail {
			http.Error(w, "disk error", http.StatusInternalServerError)
			return
		}
		q := r.URL.Query().Get("q")
		var hits []string
		for _, d := range docs {
			if strings.Contains(d, q) {
				hits = append(hits, d)
			}
		}
		_ = json.NewEncoder(w).Encode(hits)
	}))
}

func main() {
	shards := []struct {
		name  string
		delay time.Duration
		fail  bool
		docs  []string
	}{
		{"shard-a", 10 * time.Millisecond, false, []string{"go concurrency patterns", "go generics"}},
		{"shard-b", 30 * time.Millisecond, false, []string{"learning go", "rust vs go"}},
		{"shard-c", 400 * time.Millisecond, false, []string{"go in production"}}, // overloaded
		{"shard-d", 5 * time.Millisecond, true, []string{"go tooling"}},          // broken
	}

	var sources []sg.Source[[]string]
	for _, s := range shards {
		srv := shard(s.name, s.delay, s.fail, s.docs...)
		defer srv.Close()
		url := srv.URL + "/search?q=go"
		sources = append(sources, sg.Source[[]string]{Name: s.name, Fetch: func(ctx context.Context) ([]string, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				return nil, err
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return nil, err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return nil, fmt.Errorf("status %d", resp.StatusCode)
			}
			var hits []string
			return hits, json.NewDecoder(resp.Body).Decode(&hits)
		}})
	}

	fmt.Println(`query "go" -> 4 shards in parallel, 100ms budget`)
	start := time.Now()
	res := sg.Gather(context.Background(), sources, sg.Options{Timeout: 100 * time.Millisecond})
	elapsed := time.Since(start)

	var all []string
	for _, r := range res.Responses {
		fmt.Printf("  ok      %-8s %d hits  (%s)\n", r.Name, len(r.Value), r.Latency.Round(5*time.Millisecond))
		all = append(all, r.Value...)
	}
	for _, r := range res.Failed {
		fmt.Printf("  failed  %-8s %v\n", r.Name, r.Err)
	}
	for _, name := range res.Missing {
		fmt.Printf("  timeout %-8s (cancelled at the deadline)\n", name)
	}
	fmt.Printf("\nanswered in ~%dms with %d hits. complete=%v\n", elapsed.Round(10*time.Millisecond).Milliseconds(), len(all), res.Complete())
	fmt.Println("results:", all)
	fmt.Println(`The UI can show these results now, with a note like "some results may be missing".`)
}
