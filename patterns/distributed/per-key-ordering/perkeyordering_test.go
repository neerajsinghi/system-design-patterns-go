package perkeyordering

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeClock) Now() time.Time          { f.mu.Lock(); defer f.mu.Unlock(); return f.t }
func (f *fakeClock) Advance(d time.Duration) { f.mu.Lock(); f.t = f.t.Add(d); f.mu.Unlock() }

func newClock() *fakeClock { return &fakeClock{t: time.Unix(1_000, 0)} }

func ev(key string, seq uint64) Event { return Event{Key: key, Seq: seq} }

// describe renders items compactly: "A1 A2 gap(A3-4) A5".
func describe(items []Item) string {
	s := ""
	for i, it := range items {
		if i > 0 {
			s += " "
		}
		if it.Gap != nil {
			s += fmt.Sprintf("gap(%s%d-%d)", it.Gap.Key, it.Gap.First, it.Gap.Last)
		} else {
			s += fmt.Sprintf("%s%d", it.Event.Key, it.Event.Seq)
		}
	}
	return s
}

func TestPartitionFor(t *testing.T) {
	for _, n := range []int{1, 2, 7, 16} {
		for i := range 500 {
			key := fmt.Sprintf("order-%d", i)
			p := PartitionFor(key, n)
			if p < 0 || p >= n {
				t.Fatalf("PartitionFor(%q, %d) = %d, out of range", key, n, p)
			}
			if PartitionFor(key, n) != p {
				t.Fatalf("PartitionFor(%q, %d) not stable", key, n)
			}
		}
	}
	if got := PartitionFor("x", 0); got != 0 {
		t.Fatalf("PartitionFor with 0 partitions = %d, want 0", got)
	}
}

func TestNewDispatcherValidation(t *testing.T) {
	h := func(int, Event) {}
	tests := []struct {
		name       string
		partitions int
		handle     Handler
		opts       []DispatcherOption
	}{
		{"zero partitions", 0, h, nil},
		{"nil handler", 4, nil, nil},
		{"zero queue", 4, h, []DispatcherOption{WithQueueSize(0)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewDispatcher(tt.partitions, tt.handle, tt.opts...); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestDispatcherSubmitErrors(t *testing.T) {
	d, err := NewDispatcher(2, func(int, Event) {})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Submit(Event{}); err == nil {
		t.Fatal("empty key accepted")
	}
	d.Close()
	d.Close() // idempotent
	if err := d.Submit(ev("a", 1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Submit after Close = %v, want ErrClosed", err)
	}
}

// Many producers submit concurrently (each owns its keys); every key must be
// handled in submission order, on its own partition, and the work must be
// spread over several partitions.
func TestDispatcherPerKeyOrderUnderParallelism(t *testing.T) {
	const partitions, producers, keysPerProducer, eventsPerKey = 8, 6, 30, 40
	type rec struct {
		last      map[string]uint64
		bad, seen int
	}
	recs := make([]rec, partitions) // each touched only by its partition's worker
	for i := range recs {
		recs[i].last = map[string]uint64{}
	}
	d, err := NewDispatcher(partitions, func(p int, e Event) {
		r := &recs[p]
		r.seen++
		if PartitionFor(e.Key, partitions) != p || e.Seq != r.last[e.Key]+1 {
			r.bad++
		}
		r.last[e.Key] = e.Seq
	}, WithQueueSize(4))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for pr := range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(pr), 7))
			next := make([]uint64, keysPerProducer)
			for remaining := keysPerProducer * eventsPerKey; remaining > 0; {
				k := rng.IntN(keysPerProducer)
				if next[k] == eventsPerKey {
					continue
				}
				next[k]++
				if err := d.Submit(ev(fmt.Sprintf("p%d-k%d", pr, k), next[k])); err != nil {
					t.Error(err)
					return
				}
				remaining--
			}
		}()
	}
	wg.Wait()
	d.Close()

	total, used := 0, 0
	for p, r := range recs {
		if r.bad != 0 {
			t.Errorf("partition %d: %d out-of-order or misrouted events", p, r.bad)
		}
		total += r.seen
		if r.seen > 0 {
			used++
		}
	}
	if want := producers * keysPerProducer * eventsPerKey; total != want {
		t.Fatalf("handled %d events, want %d", total, want)
	}
	if used < partitions/2 {
		t.Fatalf("only %d of %d partitions used", used, partitions)
	}
}

func TestNewResequencerValidation(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
		opts    []Option
	}{
		{"zero timeout", 0, nil},
		{"negative timeout", -time.Second, nil},
		{"zero buffer", time.Second, []Option{WithMaxBuffer(0)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewResequencer(tt.timeout, tt.opts...); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	r, err := NewResequencer(time.Second, WithClock(nil)) // nil clock ignored
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Offer(Event{Seq: 1}); err == nil {
		t.Fatal("empty key accepted")
	}
	if _, err := r.Offer(ev("a", 0)); err == nil {
		t.Fatal("sequence 0 accepted")
	}
}

func TestResequencerReleasesInOrder(t *testing.T) {
	tests := []struct {
		name    string
		arrival []uint64
	}{
		{"in order", []uint64{1, 2, 3, 4}},
		{"reversed", []uint64{4, 3, 2, 1}},
		{"shuffled", []uint64{3, 1, 4, 2}},
		{"first last", []uint64{2, 3, 4, 5, 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, _ := NewResequencer(time.Minute, WithClock(newClock().Now))
			var got []Item
			for _, s := range tt.arrival {
				items, err := r.Offer(ev("A", s))
				if err != nil {
					t.Fatal(err)
				}
				got = append(got, items...)
			}
			want := ""
			for i := range tt.arrival {
				if i > 0 {
					want += " "
				}
				want += fmt.Sprintf("A%d", i+1)
			}
			if describe(got) != want {
				t.Fatalf("released %q, want %q", describe(got), want)
			}
			if st := r.Stats(); st.Buffered != 0 || st.Released != uint64(len(tt.arrival)) {
				t.Fatalf("stats %+v", st)
			}
		})
	}
}

func TestResequencerIgnoresDuplicates(t *testing.T) {
	r, _ := NewResequencer(time.Minute, WithClock(newClock().Now))
	var got []Item
	for _, s := range []uint64{1, 1, 3, 3, 2, 2, 3} {
		items, err := r.Offer(ev("A", s))
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, items...)
	}
	if describe(got) != "A1 A2 A3" {
		t.Fatalf("released %q", describe(got))
	}
	if st := r.Stats(); st.Duplicates != 4 {
		t.Fatalf("duplicates = %d, want 4", st.Duplicates)
	}
}

func TestGapTimeoutSkipsAndNotifies(t *testing.T) {
	clk := newClock()
	r, _ := NewResequencer(100*time.Millisecond, WithClock(clk.Now))
	mustOffer := func(seq uint64) []Item {
		items, err := r.Offer(ev("A", seq))
		if err != nil {
			t.Fatal(err)
		}
		return items
	}
	if got := describe(mustOffer(1)); got != "A1" {
		t.Fatalf("got %q", got)
	}
	mustOffer(3)
	clk.Advance(20 * time.Millisecond)
	mustOffer(4)
	clk.Advance(79 * time.Millisecond) // 99ms since A3 arrived
	if got := r.Tick(); len(got) != 0 {
		t.Fatalf("released %q before the timeout", describe(got))
	}
	clk.Advance(time.Millisecond)
	got := r.Tick()
	if describe(got) != "gap(A2-2) A3 A4" {
		t.Fatalf("after timeout released %q", describe(got))
	}
	if got[0].Gap.Waited != 100*time.Millisecond {
		t.Fatalf("waited %v, want 100ms", got[0].Gap.Waited)
	}
	// The missing event finally turns up: too late, ignored.
	if late := mustOffer(2); len(late) != 0 {
		t.Fatalf("late event released %q", describe(late))
	}
	if got := describe(mustOffer(5)); got != "A5" {
		t.Fatalf("got %q", got)
	}
	st := r.Stats()
	if st.Gaps != 1 || st.Skipped != 1 || st.Duplicates != 1 || st.Released != 4 {
		t.Fatalf("stats %+v", st)
	}
}

func TestGapTimeoutResolvedOnOffer(t *testing.T) {
	clk := newClock()
	r, _ := NewResequencer(time.Second, WithClock(clk.Now))
	r.Offer(ev("A", 4)) // 1-3 missing
	clk.Advance(2 * time.Second)
	items, err := r.Offer(ev("A", 6)) // overdue gap is resolved first, then 6 waits for 5
	if err != nil {
		t.Fatal(err)
	}
	if describe(items) != "gap(A1-3) A4" {
		t.Fatalf("got %q", describe(items))
	}
	if r.NextSeq("A") != 5 {
		t.Fatalf("next = %d, want 5", r.NextSeq("A"))
	}
	// 6 has its own, fresh wait.
	clk.Advance(999 * time.Millisecond)
	if got := r.Tick(); len(got) != 0 {
		t.Fatalf("released %q early", describe(got))
	}
	clk.Advance(time.Millisecond)
	if got := describe(r.Tick()); got != "gap(A5-5) A6" {
		t.Fatalf("got %q", got)
	}
}

func TestMultipleGapsSkippedTogether(t *testing.T) {
	clk := newClock()
	r, _ := NewResequencer(time.Second, WithClock(clk.Now))
	for _, s := range []uint64{1, 3, 6} {
		r.Offer(ev("A", s))
	}
	clk.Advance(time.Second)
	if got := describe(r.Tick()); got != "gap(A2-2) A3 gap(A4-5) A6" {
		t.Fatalf("got %q", got)
	}
	if st := r.Stats(); st.Gaps != 2 || st.Skipped != 3 || st.Buffered != 0 {
		t.Fatalf("stats %+v", st)
	}
}

func TestNoCrossKeyBlocking(t *testing.T) {
	clk := newClock()
	r, _ := NewResequencer(time.Minute, WithClock(clk.Now))
	var got []Item
	for _, e := range []Event{ev("A", 1), ev("A", 3), ev("B", 1), ev("B", 2), ev("C", 1), ev("A", 4), ev("B", 3)} {
		items, err := r.Offer(e)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, items...)
	}
	if describe(got) != "A1 B1 B2 C1 B3" {
		t.Fatalf("released %q; B and C must not wait for A's gap", describe(got))
	}
	if r.NextSeq("A") != 2 || r.Stats().Buffered != 2 {
		t.Fatalf("A next=%d buffered=%d", r.NextSeq("A"), r.Stats().Buffered)
	}
}

func TestBufferBound(t *testing.T) {
	r, _ := NewResequencer(time.Minute, WithClock(newClock().Now), WithMaxBuffer(2))
	for _, s := range []uint64{2, 3} {
		if _, err := r.Offer(ev("A", s)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Offer(ev("A", 4)); !errors.Is(err, ErrBufferFull) {
		t.Fatalf("third early event: err = %v, want ErrBufferFull", err)
	}
	// Another key has its own buffer.
	if _, err := r.Offer(ev("B", 5)); err != nil {
		t.Fatalf("B affected by A's full buffer: %v", err)
	}
	items, _ := r.Offer(ev("A", 1))
	if describe(items) != "A1 A2 A3" {
		t.Fatalf("got %q", describe(items))
	}
	items, err := r.Offer(ev("A", 4)) // redelivery now fits
	if err != nil || describe(items) != "A4" {
		t.Fatalf("redelivery: %q, %v", describe(items), err)
	}
	if st := r.Stats(); st.Rejected != 1 {
		t.Fatalf("rejected = %d, want 1", st.Rejected)
	}
}

// Each goroutine owns some keys and offers their events shuffled, while
// another goroutine ticks. With no gap ever timing out, every key must come
// out exactly 1..n.
func TestResequencerConcurrent(t *testing.T) {
	const owners, keysPerOwner, n = 8, 20, 25
	clk := newClock()
	r, _ := NewResequencer(time.Hour, WithClock(clk.Now))
	stop := make(chan struct{})
	tickerDone := make(chan struct{})
	go func() {
		defer close(tickerDone)
		for {
			select {
			case <-stop:
				return
			default:
				if items := r.Tick(); len(items) != 0 {
					t.Errorf("tick released %q", describe(items))
				}
			}
		}
	}()
	var wg sync.WaitGroup
	for o := range owners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(o), 99))
			for k := range keysPerOwner {
				key := fmt.Sprintf("o%d-k%d", o, k)
				seqs := make([]uint64, n)
				for i := range seqs {
					seqs[i] = uint64(i + 1)
				}
				rng.Shuffle(n, func(i, j int) { seqs[i], seqs[j] = seqs[j], seqs[i] })
				var want uint64 = 1
				for _, s := range seqs {
					items, err := r.Offer(ev(key, s))
					if err != nil {
						t.Error(err)
						return
					}
					for _, it := range items {
						if it.Gap != nil || it.Event.Seq != want {
							t.Errorf("%s: got %q, want seq %d", key, describe([]Item{it}), want)
							return
						}
						want++
					}
				}
				if want != n+1 {
					t.Errorf("%s: released %d events, want %d", key, want-1, n)
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	<-tickerDone
	if st := r.Stats(); st.Released != owners*keysPerOwner*n || st.Buffered != 0 {
		t.Fatalf("stats %+v", st)
	}
}
