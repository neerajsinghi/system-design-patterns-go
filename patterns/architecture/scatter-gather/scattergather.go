// Package scattergather sends one request to many sources in parallel and
// gathers whatever comes back before a deadline. The caller gets partial
// results plus a record of which sources failed or timed out, instead of
// all-or-nothing. A search engine querying index shards, or a price
// comparison querying suppliers, works this way.
package scattergather

import (
	"context"
	"errors"
	"sort"
	"time"
)

// Source is one parallel target.
type Source[T any] struct {
	Name  string
	Fetch func(ctx context.Context) (T, error)
}

// Response is one source's outcome.
type Response[T any] struct {
	Name    string
	Value   T
	Err     error
	Latency time.Duration
}

// Result is what Gather returns.
type Result[T any] struct {
	Responses []Response[T] // successes, sorted by name
	Failed    []Response[T] // sources that returned an error, sorted by name
	Missing   []string      // sources that had not answered when Gather returned, sorted
}

// Complete reports whether every source succeeded.
func (r Result[T]) Complete() bool { return len(r.Failed) == 0 && len(r.Missing) == 0 }

// Options tune Gather.
type Options struct {
	// Timeout bounds the whole gather. 0 means rely on the parent context only.
	Timeout time.Duration
	// Need returns early once this many sources have succeeded (0 means
	// wait for all). Use it for "the first k answers are enough".
	Need int
}

// Gather fetches from every source concurrently. It returns when all have
// answered, when Need successes arrive, or when the timeout or parent context
// expires, whichever comes first. Outstanding fetches are then cancelled
// through their context. Their goroutines finish on their own, and they
// never block, because the results channel is buffered for every source.
func Gather[T any](ctx context.Context, sources []Source[T], opts Options) Result[T] {
	var res Result[T]
	if len(sources) == 0 {
		return res
	}
	var cancel context.CancelFunc
	if opts.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel() // stops stragglers once we return

	type indexed struct {
		i int
		r Response[T]
	}
	results := make(chan indexed, len(sources))
	for i, src := range sources {
		go func() {
			start := time.Now()
			v, err := safeFetch(ctx, src.Fetch)
			results <- indexed{i, Response[T]{Name: src.Name, Value: v, Err: err, Latency: time.Since(start)}}
		}()
	}

	pending := make(map[int]bool, len(sources)) // by index, so duplicate names are fine
	for i := range sources {
		pending[i] = true
	}
	record := func(in indexed) (enough bool) {
		if ctx.Err() != nil && isCtxErr(in.r.Err) {
			return false // aborted by our own deadline: report it as Missing, not Failed
		}
		delete(pending, in.i)
		if in.r.Err != nil {
			res.Failed = append(res.Failed, in.r)
			return false
		}
		res.Responses = append(res.Responses, in.r)
		return opts.Need > 0 && len(res.Responses) >= opts.Need
	}
loop:
	for len(pending) > 0 {
		select {
		case in := <-results:
			if record(in) {
				break loop
			}
		case <-ctx.Done():
			// select picks randomly among ready cases, so collect any
			// results that were already waiting when the deadline fired.
			for len(pending) > 0 {
				select {
				case in := <-results:
					record(in)
				default:
					break loop
				}
			}
		}
	}
	for i := range pending {
		res.Missing = append(res.Missing, sources[i].Name)
	}
	sort.Slice(res.Responses, func(i, j int) bool { return res.Responses[i].Name < res.Responses[j].Name })
	sort.Slice(res.Failed, func(i, j int) bool { return res.Failed[i].Name < res.Failed[j].Name })
	sort.Strings(res.Missing)
	return res
}

func isCtxErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// safeFetch turns a panicking source into an error, so one bad source can't
// crash the gatherer. A nil Fetch is reported as an error too.
func safeFetch[T any](ctx context.Context, fetch func(context.Context) (T, error)) (v T, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = &PanicError{Value: p}
		}
	}()
	if fetch == nil {
		return v, ErrNilFetch
	}
	return fetch(ctx)
}

// PanicError wraps a value recovered from a panicking source.
type PanicError struct{ Value any }

func (e *PanicError) Error() string { return "scattergather: source panicked" }

// ErrNilFetch is reported for a Source without a Fetch function.
var ErrNilFetch = errors.New("scattergather: nil Fetch")
