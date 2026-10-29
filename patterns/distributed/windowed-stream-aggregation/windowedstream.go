// Package windowedstream implements event-time windowed aggregation over an
// unbounded stream whose events arrive late and out of order.
//
// A surge-pricing service wants "ride requests vs available drivers, per
// geocell, per minute". The events come from phones, and phones on bad
// networks deliver them seconds or minutes after they happened, in no
// particular order. Grouping events by the minute they *arrive* (processing
// time) puts a delayed request in the wrong minute, so the surge for the
// minute it belonged to is too low and the next one too high. Storing every
// event and recomputing from scratch is correct but does not scale.
//
// This package groups events by the minute they *happened* (event time),
// keyed by any ordered key such as a geocell id:
//
//   - Windows are tumbling (size == slide) or sliding/hopping (size a
//     multiple of slide). Each window is [Start, End): start inclusive, end
//     exclusive, aligned to the Unix epoch. A sliding event lands in
//     size/slide windows.
//   - Each window keeps one running accumulator, updated by a user-supplied
//     fold, so state is O(open windows), not O(events).
//   - The watermark is the engine's estimate of event-time progress:
//     max event time seen − watermark delay (bounded out-of-orderness). It
//     only moves forward. When it reaches a window's End the window emits.
//   - A late event (its window has already emitted) still updates the window
//     while watermark < End + allowed lateness, and the window re-emits with
//     a higher Revision. Beyond that it is dropped and counted.
//   - Window state is deleted once the watermark passes End + allowed
//     lateness, because nothing can update it any more.
package windowedstream

import (
	"cmp"
	"errors"
	"slices"
	"sync"
	"time"
)

// Window is the half-open event-time interval [Start, End).
type Window struct {
	Start, End time.Time
}

// Contains reports whether t falls in the window: Start inclusive, End
// exclusive.
func (w Window) Contains(t time.Time) bool {
	return !t.Before(w.Start) && t.Before(w.End)
}

// Spec describes how events are assigned to windows.
type Spec struct {
	Size, Slide time.Duration
}

// Tumbling returns non-overlapping windows of the given size: every event is
// in exactly one window.
func Tumbling(size time.Duration) Spec { return Spec{Size: size, Slide: size} }

// Sliding returns overlapping (hopping) windows of the given size, a new one
// starting every slide. size must be a multiple of slide, so every event is
// in exactly size/slide windows.
func Sliding(size, slide time.Duration) Spec { return Spec{Size: size, Slide: slide} }

func (s Spec) validate() error {
	switch {
	case s.Size <= 0:
		return errors.New("windowedstream: window size must be positive")
	case s.Slide <= 0:
		return errors.New("windowedstream: window slide must be positive")
	case s.Slide > s.Size:
		return errors.New("windowedstream: slide must not exceed size (gaps would drop events)")
	case s.Size%s.Slide != 0:
		return errors.New("windowedstream: size must be a multiple of slide")
	}
	return nil
}

// Assign returns the windows containing t, ordered by Start, in UTC. The spec must be
// valid (see New).
func (s Spec) Assign(t time.Time) []Window {
	n, slide, size := t.UnixNano(), int64(s.Slide), int64(s.Size)
	last := floorDiv(n, slide) * slide // latest window start <= t
	out := make([]Window, 0, size/slide)
	for start := last - size + slide; start <= last; start += slide {
		out = append(out, Window{Start: time.Unix(0, start).UTC(), End: time.Unix(0, start+size).UTC()})
	}
	return out
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && a < 0 {
		q--
	}
	return q
}

// Event is one stream element with its own event-time timestamp.
type Event[K cmp.Ordered, E any] struct {
	Key   K
	Time  time.Time
	Value E
}

// Result is one emission of a window's aggregate.
type Result[K cmp.Ordered, A any] struct {
	Key    K
	Window Window
	Value  A
	// Events is how many events have been folded into Value.
	Events int
	// Revision is 0 for a window's first emission and increases by one for
	// each re-emission caused by a late event.
	Revision int
	// Watermark is the watermark at the moment of emission. For a late
	// update it is the watermark the late event arrived under.
	Watermark time.Time
}

// IsUpdate reports whether this result revises an earlier emission.
func (r Result[K, A]) IsUpdate() bool { return r.Revision > 0 }

// Stats counts what happened to events and windows.
type Stats struct {
	// Events is every event passed to Add.
	Events uint64
	// Late is events that arrived after a window they belong to had passed
	// the watermark, but were still accepted (within allowed lateness).
	Late uint64
	// Dropped is events too late for every window they belong to.
	Dropped uint64
	// Emitted is first emissions; Updates is re-emissions from late events.
	Emitted, Updates uint64
	// OpenWindows is how many (key, window) states are held right now.
	OpenWindows int
}

// Option configures an Aggregator.
type Option func(*options)

type options struct {
	delay, lateness time.Duration
}

// WithWatermarkDelay sets the bounded out-of-orderness: the watermark trails
// the largest event time seen by d. A larger d waits longer before the first
// emission but turns fewer events into late updates. Default 0.
func WithWatermarkDelay(d time.Duration) Option { return func(o *options) { o.delay = d } }

// WithAllowedLateness keeps a window's state for d after the watermark
// passes its End. Events arriving in that period update the window and
// re-emit it; later ones are dropped. Default 0: anything behind the
// watermark is dropped.
func WithAllowedLateness(d time.Duration) Option { return func(o *options) { o.lateness = d } }

type winKey[K cmp.Ordered] struct {
	key   K
	start int64 // UnixNano; End = start + size
}

type winState[A any] struct {
	acc   A
	n     int
	emits int // emissions so far; the next one has Revision == emits
}

// Aggregator is a concurrency-safe event-time windowed aggregator. Each
// (key, window) has an accumulator that starts as A's zero value and is
// updated by fold.
type Aggregator[K cmp.Ordered, E, A any] struct {
	spec            Spec
	fold            func(A, E) A
	delay, lateness time.Duration

	mu     sync.Mutex
	hasWM  bool
	wm     time.Time
	maxSet bool
	max    time.Time
	state  map[winKey[K]]*winState[A]
	stats  Stats
}

// New returns an Aggregator for the given window spec and fold function.
func New[K cmp.Ordered, E, A any](spec Spec, fold func(A, E) A, opts ...Option) (*Aggregator[K, E, A], error) {
	if err := spec.validate(); err != nil {
		return nil, err
	}
	if fold == nil {
		return nil, errors.New("windowedstream: fold must not be nil")
	}
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if o.delay < 0 {
		return nil, errors.New("windowedstream: watermark delay must not be negative")
	}
	if o.lateness < 0 {
		return nil, errors.New("windowedstream: allowed lateness must not be negative")
	}
	return &Aggregator[K, E, A]{
		spec: spec, fold: fold, delay: o.delay, lateness: o.lateness,
		state: make(map[winKey[K]]*winState[A]),
	}, nil
}

// Add folds e into every window it belongs to and returns the results this
// causes: re-emissions of already-emitted windows the event updated (late
// data), followed by windows the advancing watermark has now closed.
func (a *Aggregator[K, E, A]) Add(e Event[K, E]) []Result[K, A] {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.stats.Events++

	var out []Result[K, A]
	accepted, late := false, false
	for _, w := range a.spec.Assign(e.Time) {
		if a.hasWM && !w.End.Add(a.lateness).After(a.wm) {
			continue // past allowed lateness: state is gone, ignore for this window
		}
		k := winKey[K]{key: e.Key, start: w.Start.UnixNano()}
		st := a.state[k]
		if st == nil {
			st = &winState[A]{}
			a.state[k] = st
		}
		st.acc = a.fold(st.acc, e.Value)
		st.n++
		accepted = true
		if a.hasWM && !w.End.After(a.wm) {
			// The watermark already passed this window: emit (or re-emit) now.
			late = true
			out = append(out, a.emit(k, st))
		}
	}
	switch {
	case !accepted:
		a.stats.Dropped++
	case late:
		a.stats.Late++
	}

	if !a.maxSet || e.Time.After(a.max) {
		a.max, a.maxSet = e.Time, true
		out = append(out, a.advance(e.Time.Add(-a.delay))...)
	}
	return out
}

// AdvanceWatermark moves the watermark to t if that is later than the current
// one, and returns the windows that closes. Use it when a source goes idle
// (a heartbeat says "nothing older than t is coming") or to flush at the end
// of a bounded stream. An earlier t is ignored.
func (a *Aggregator[K, E, A]) AdvanceWatermark(t time.Time) []Result[K, A] {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.advance(t)
}

// Watermark returns the current watermark; ok is false before any event.
func (a *Aggregator[K, E, A]) Watermark() (wm time.Time, ok bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.wm, a.hasWM
}

// Stats returns a snapshot of the counters.
func (a *Aggregator[K, E, A]) Stats() Stats {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := a.stats
	s.OpenWindows = len(a.state)
	return s
}

func (a *Aggregator[K, E, A]) advance(t time.Time) []Result[K, A] {
	if a.hasWM && !t.After(a.wm) {
		return nil
	}
	a.wm, a.hasWM = t, true
	var out []Result[K, A]
	for k, st := range a.state {
		end := time.Unix(0, k.start).Add(a.spec.Size)
		if st.emits == 0 && !end.After(a.wm) {
			out = append(out, a.emit(k, st))
		}
		if !end.Add(a.lateness).After(a.wm) {
			delete(a.state, k) // nothing can update it any more
		}
	}
	slices.SortFunc(out, func(x, y Result[K, A]) int {
		if c := x.Window.End.Compare(y.Window.End); c != 0 {
			return c
		}
		if c := x.Window.Start.Compare(y.Window.Start); c != 0 {
			return c
		}
		return cmp.Compare(x.Key, y.Key)
	})
	return out
}

func (a *Aggregator[K, E, A]) emit(k winKey[K], st *winState[A]) Result[K, A] {
	start := time.Unix(0, k.start).UTC()
	r := Result[K, A]{
		Key: k.key, Window: Window{Start: start, End: start.Add(a.spec.Size)},
		Value: st.acc, Events: st.n, Revision: st.emits, Watermark: a.wm,
	}
	if st.emits == 0 {
		a.stats.Emitted++
	} else {
		a.stats.Updates++
	}
	st.emits++
	return r
}
