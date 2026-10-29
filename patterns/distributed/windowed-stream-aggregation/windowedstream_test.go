package windowedstream

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)

func at(d time.Duration) time.Time { return t0.Add(d) }

func count(acc int, _ int) int { return acc + 1 }

func ev(key string, d time.Duration) Event[string, int] {
	return Event[string, int]{Key: key, Time: at(d), Value: 1}
}

func mustNew(t *testing.T, spec Spec, opts ...Option) *Aggregator[string, int, int] {
	t.Helper()
	a, err := New[string](spec, count, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestNewValidation(t *testing.T) {
	tests := []struct {
		name string
		spec Spec
		fold func(int, int) int
		opts []Option
	}{
		{"zero size", Tumbling(0), count, nil},
		{"negative slide", Sliding(time.Minute, -time.Second), count, nil},
		{"slide larger than size", Sliding(time.Minute, 2*time.Minute), count, nil},
		{"size not a multiple of slide", Sliding(5*time.Minute, 2*time.Minute), count, nil},
		{"nil fold", Tumbling(time.Minute), nil, nil},
		{"negative delay", Tumbling(time.Minute), count, []Option{WithWatermarkDelay(-time.Second)}},
		{"negative lateness", Tumbling(time.Minute), count, []Option{WithAllowedLateness(-time.Second)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New[string](tt.spec, tt.fold, tt.opts...); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestTumblingBoundaries(t *testing.T) {
	spec := Tumbling(time.Minute)
	minute1 := Window{Start: at(time.Minute), End: at(2 * time.Minute)}
	tests := []struct {
		name string
		t    time.Time
		want Window
	}{
		{"start is inclusive", at(time.Minute), minute1},
		{"just before end", at(2*time.Minute - time.Nanosecond), minute1},
		{"end is exclusive", at(2 * time.Minute), Window{Start: at(2 * time.Minute), End: at(3 * time.Minute)}},
		{"before the epoch floors down", time.Unix(-1, 0), Window{Start: time.Unix(-60, 0), End: time.Unix(0, 0)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := spec.Assign(tt.t)
			if len(got) != 1 || !got[0].Start.Equal(tt.want.Start) || !got[0].End.Equal(tt.want.End) {
				t.Fatalf("Assign(%v) = %v, want [%v]", tt.t, got, tt.want)
			}
			if !got[0].Contains(tt.t) {
				t.Fatal("assigned window does not contain the event")
			}
		})
	}
	if minute1.Contains(minute1.End) {
		t.Fatal("End must be exclusive")
	}
	if !minute1.Contains(minute1.Start) {
		t.Fatal("Start must be inclusive")
	}
}

func TestSlidingAssignment(t *testing.T) {
	spec := Sliding(5*time.Minute, time.Minute)
	ts := at(7*time.Minute + 30*time.Second)
	got := spec.Assign(ts)
	if len(got) != 5 {
		t.Fatalf("got %d windows, want size/slide = 5", len(got))
	}
	for i, w := range got {
		wantStart := at(time.Duration(3+i) * time.Minute)
		if !w.Start.Equal(wantStart) || w.End.Sub(w.Start) != 5*time.Minute {
			t.Fatalf("window %d = [%v, %v), want start %v", i, w.Start, w.End, wantStart)
		}
		if !w.Contains(ts) {
			t.Fatalf("window %d does not contain the event", i)
		}
	}
	// On a slide boundary the event starts a window and is outside the one
	// that ends there.
	got = spec.Assign(at(5 * time.Minute))
	if !got[0].Start.Equal(at(time.Minute)) || !got[4].Start.Equal(at(5*time.Minute)) {
		t.Fatalf("boundary assignment = %v", got)
	}
}

func TestWatermarkTriggersEmission(t *testing.T) {
	a := mustNew(t, Tumbling(time.Minute), WithWatermarkDelay(10*time.Second))
	for _, d := range []time.Duration{5 * time.Second, 50 * time.Second, 69 * time.Second} {
		if out := a.Add(ev("cell", d)); len(out) != 0 {
			t.Fatalf("event at %v emitted %v before the watermark reached 1m", d, out)
		}
	}
	out := a.Add(ev("cell", 70*time.Second)) // watermark = 60s = End of [0, 60s)
	if len(out) != 1 {
		t.Fatalf("got %d results, want 1", len(out))
	}
	r := out[0]
	if r.Value != 2 || r.Events != 2 || r.Revision != 0 || r.IsUpdate() || !r.Window.End.Equal(at(time.Minute)) || !r.Watermark.Equal(at(time.Minute)) {
		t.Fatalf("unexpected result %+v", r)
	}
	if wm, ok := a.Watermark(); !ok || !wm.Equal(at(time.Minute)) {
		t.Fatalf("watermark = %v, %v", wm, ok)
	}
}

func TestWatermarkOnlyMovesForward(t *testing.T) {
	a := mustNew(t, Tumbling(time.Minute))
	a.Add(ev("cell", 90*time.Second))
	a.Add(ev("cell", 80*time.Second)) // out of order, inside the open window
	if out := a.AdvanceWatermark(at(30 * time.Second)); out != nil {
		t.Fatalf("earlier watermark emitted %v", out)
	}
	if wm, _ := a.Watermark(); !wm.Equal(at(90 * time.Second)) {
		t.Fatalf("watermark went backwards to %v", wm)
	}
	out := a.AdvanceWatermark(at(2 * time.Minute))
	if len(out) != 1 || out[0].Value != 2 {
		t.Fatalf("flush = %+v", out)
	}
}

func TestLateWithinAllowedLatenessUpdates(t *testing.T) {
	a := mustNew(t, Tumbling(time.Minute), WithAllowedLateness(time.Minute))
	a.Add(ev("cell", 10*time.Second))
	if out := a.Add(ev("cell", 65*time.Second)); len(out) != 1 || out[0].Value != 1 {
		t.Fatalf("first emission = %+v", out)
	}
	out := a.Add(ev("cell", 30*time.Second)) // late, watermark 65s < End+lateness 120s
	if len(out) != 1 {
		t.Fatalf("late event produced %d results, want 1 update", len(out))
	}
	if r := out[0]; r.Value != 2 || r.Revision != 1 || !r.IsUpdate() {
		t.Fatalf("update = %+v", r)
	}
	s := a.Stats()
	if s.Late != 1 || s.Dropped != 0 || s.Emitted != 1 || s.Updates != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestLateIntoEmptyWindowEmitsImmediately(t *testing.T) {
	a := mustNew(t, Tumbling(time.Minute), WithAllowedLateness(time.Minute))
	a.Add(ev("cell", 70*time.Second))
	out := a.Add(ev("other", 20*time.Second)) // window [0,60s) never held "other"
	if len(out) != 1 || out[0].Key != "other" || out[0].Revision != 0 || out[0].Value != 1 {
		t.Fatalf("got %+v", out)
	}
	if s := a.Stats(); s.Late != 1 || s.Emitted != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestBeyondLatenessDroppedAndCounted(t *testing.T) {
	a := mustNew(t, Tumbling(time.Minute), WithAllowedLateness(30*time.Second))
	a.Add(ev("cell", 10*time.Second))
	a.Add(ev("cell", 90*time.Second)) // watermark 90s = End+lateness of [0,60s)
	if out := a.Add(ev("cell", 40*time.Second)); len(out) != 0 {
		t.Fatalf("dropped event emitted %+v", out)
	}
	s := a.Stats()
	if s.Dropped != 1 || s.Late != 0 || s.Events != 3 {
		t.Fatalf("stats = %+v", s)
	}

	// With zero lateness, anything behind the watermark is dropped.
	z := mustNew(t, Tumbling(time.Minute))
	z.Add(ev("cell", 60*time.Second))
	z.Add(ev("cell", 59*time.Second))
	if s := z.Stats(); s.Dropped != 1 {
		t.Fatalf("zero lateness stats = %+v", s)
	}
}

func TestSlidingPartiallyLateIsNotDropped(t *testing.T) {
	a := mustNew(t, Sliding(3*time.Minute, time.Minute))
	a.Add(ev("cell", 4*time.Minute)) // watermark 4m: windows ending <= 4m are closed
	out := a.Add(ev("cell", 2*time.Minute+30*time.Second))
	// Windows of 2m30s: [0,3m) [1m,4m) closed and dropped for it; [2m,5m) open.
	if len(out) != 0 {
		t.Fatalf("got %+v", out)
	}
	if s := a.Stats(); s.Dropped != 0 || s.Late != 0 {
		t.Fatalf("an event accepted by one window must not count as dropped: %+v", s)
	}
	flushed := a.AdvanceWatermark(at(10 * time.Minute))
	var got int
	for _, r := range flushed {
		if r.Window.Start.Equal(at(2 * time.Minute)) {
			got = r.Value
		}
	}
	if got != 2 {
		t.Fatalf("window [2m,5m) = %d, want 2; all = %+v", got, flushed)
	}
}

func TestStateCleanup(t *testing.T) {
	a := mustNew(t, Tumbling(time.Minute), WithAllowedLateness(time.Minute))
	a.Add(ev("a", 10*time.Second))
	a.Add(ev("b", 20*time.Second))
	a.Add(ev("a", 70*time.Second)) // emits [0,60s) but keeps it for lateness
	if s := a.Stats(); s.OpenWindows != 3 {
		t.Fatalf("open = %d, want 3 (two emitted but still updatable, one live)", s.OpenWindows)
	}
	a.AdvanceWatermark(at(2 * time.Minute)) // End+lateness of [0,60s); [60s,120s) emits
	if s := a.Stats(); s.OpenWindows != 1 {
		t.Fatalf("open = %d, want 1", s.OpenWindows)
	}
	a.AdvanceWatermark(at(3 * time.Minute))
	if s := a.Stats(); s.OpenWindows != 0 {
		t.Fatalf("open = %d, want 0", s.OpenWindows)
	}
	// A cleaned-up window is not resurrected by a straggler.
	if out := a.Add(ev("a", 30*time.Second)); len(out) != 0 || a.Stats().OpenWindows != 0 {
		t.Fatalf("straggler recreated state: %+v", out)
	}
}

func TestPerKeyIsolation(t *testing.T) {
	type demand struct{ requests, drivers int }
	type obs struct{ request bool }
	fold := func(d demand, o obs) demand {
		if o.request {
			d.requests++
		} else {
			d.drivers++
		}
		return d
	}
	a, err := New[string](Tumbling(time.Minute), fold)
	if err != nil {
		t.Fatal(err)
	}
	add := func(key string, d time.Duration, req bool) []Result[string, demand] {
		return a.Add(Event[string, obs]{Key: key, Time: at(d), Value: obs{req}})
	}
	add("north", 1*time.Second, true)
	add("south", 2*time.Second, false)
	add("north", 3*time.Second, true)
	add("south", 4*time.Second, true)
	out := add("east", 61*time.Second, true)
	want := map[string]demand{"north": {2, 0}, "south": {1, 1}}
	if len(out) != 2 || out[0].Key != "north" || out[1].Key != "south" {
		t.Fatalf("want north then south, got %+v", out)
	}
	for _, r := range out {
		if r.Value != want[r.Key] {
			t.Errorf("%s = %+v, want %+v", r.Key, r.Value, want[r.Key])
		}
	}
}

func TestConcurrentAdds(t *testing.T) {
	a := mustNew(t, Sliding(2*time.Minute, time.Minute), WithAllowedLateness(time.Hour))
	const workers, perWorker = 8, 500
	var wg sync.WaitGroup
	var mu sync.Mutex
	latest := map[string]Result[string, int]{} // last revision per (key, window)
	record := func(rs []Result[string, int]) {
		mu.Lock()
		defer mu.Unlock()
		for _, r := range rs {
			id := fmt.Sprint(r.Key, r.Window.Start.Unix())
			if prev, ok := latest[id]; !ok || r.Revision > prev.Revision {
				latest[id] = r
			}
		}
	}
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWorker {
				key := fmt.Sprint("cell-", i%3)
				record(a.Add(ev(key, time.Duration(w*perWorker+i)*100*time.Millisecond)))
			}
		}()
	}
	wg.Wait()
	record(a.AdvanceWatermark(at(24 * time.Hour)))

	s := a.Stats()
	if s.Events != workers*perWorker || s.Dropped != 0 || s.OpenWindows != 0 {
		t.Fatalf("stats = %+v", s)
	}
	total := 0
	for _, r := range latest {
		total += r.Value
	}
	// Every event is in exactly two sliding windows.
	if total != 2*workers*perWorker {
		t.Fatalf("sum of final window counts = %d, want %d", total, 2*workers*perWorker)
	}
}
