package materializedview

import (
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"testing"
)

func TestTableErrors(t *testing.T) {
	tb := NewTable()
	_ = tb.Insert(Order{ID: "o1", Customer: "ada", Amount: 1})
	tests := []struct {
		name string
		err  error
		want error
	}{
		{"duplicate insert", tb.Insert(Order{ID: "o1"}), ErrExists},
		{"update missing", tb.Update("nope", func(*Order) {}), ErrNotFound},
		{"delete missing", tb.Delete("nope"), ErrNotFound},
	}
	for _, tt := range tests {
		if !errors.Is(tt.err, tt.want) {
			t.Errorf("%s: got %v want %v", tt.name, tt.err, tt.want)
		}
	}
	if n := len(tb.ChangesSince(0)); n != 1 {
		t.Fatalf("failed mutations must not emit changes: %d", n)
	}
}

func TestIncrementalDeltas(t *testing.T) {
	tb := NewTable()
	v := NewView()
	steps := []struct {
		name string
		do   func() error
		want []CustomerStats
	}{
		{"insert", func() error { return tb.Insert(Order{"o1", "ada", "placed", 1000}) },
			[]CustomerStats{{"ada", 1, 1000}}},
		{"insert another customer", func() error { return tb.Insert(Order{"o2", "bob", "placed", 500}) },
			[]CustomerStats{{"ada", 1, 1000}, {"bob", 1, 500}}},
		{"update amount", func() error { return tb.Update("o1", func(o *Order) { o.Amount = 1200 }) },
			[]CustomerStats{{"ada", 1, 1200}, {"bob", 1, 500}}},
		{"cancel removes revenue but keeps count", func() error {
			return tb.Update("o2", func(o *Order) { o.Status = "cancelled" })
		}, []CustomerStats{{"ada", 1, 1200}, {"bob", 1, 0}}},
		{"move order to another customer", func() error {
			return tb.Update("o1", func(o *Order) { o.Customer = "bob" })
		}, []CustomerStats{{"bob", 2, 1200}}},
		{"delete", func() error { return tb.Delete("o2") }, []CustomerStats{{"bob", 1, 1200}}},
		{"delete last order drops the group", func() error { return tb.Delete("o1") }, []CustomerStats{}},
	}
	for _, s := range steps {
		if err := s.do(); err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		if _, err := v.CatchUp(tb); err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		if got := v.All(); !slices.Equal(got, s.want) {
			t.Fatalf("%s: got %v want %v", s.name, got, s.want)
		}
	}
}

func TestApplyIsIdempotentAndDetectsGaps(t *testing.T) {
	tb := NewTable()
	for i := range 3 {
		_ = tb.Insert(Order{ID: fmt.Sprint(i), Customer: "ada", Status: "placed", Amount: 10})
	}
	ch := tb.ChangesSince(0)
	v := NewView()
	_ = v.Apply(ch[0])
	_ = v.Apply(ch[0]) // redelivered
	if s, _ := v.Get("ada"); s.Orders != 1 {
		t.Fatalf("duplicate applied twice: %+v", s)
	}
	if err := v.Apply(ch[2]); !errors.Is(err, ErrGap) {
		t.Fatalf("want ErrGap, got %v", err)
	}
	if n, err := v.CatchUp(tb); err != nil || n != 2 || v.LSN() != 3 {
		t.Fatalf("catch-up n=%d err=%v lsn=%d", n, err, v.LSN())
	}
}

// TestMatchesRecompute is the key property: after any sequence of changes,
// the incrementally maintained view equals a full recomputation.
func TestMatchesRecompute(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	tb := NewTable()
	v := NewView()
	customers := []string{"ada", "bob", "cy", "dee"}
	statuses := []string{"placed", "shipped", "cancelled"}
	ids := []string{}
	for i := range 3000 {
		switch r := rng.Intn(10); {
		case r < 4 || len(ids) == 0:
			id := fmt.Sprint("o", i)
			_ = tb.Insert(Order{id, customers[rng.Intn(4)], statuses[rng.Intn(3)], int64(rng.Intn(10_000))})
			ids = append(ids, id)
		case r < 8:
			id := ids[rng.Intn(len(ids))]
			_ = tb.Update(id, func(o *Order) {
				o.Status = statuses[rng.Intn(3)]
				o.Amount = int64(rng.Intn(10_000))
				if rng.Intn(4) == 0 {
					o.Customer = customers[rng.Intn(4)]
				}
			})
		default:
			j := rng.Intn(len(ids))
			_ = tb.Delete(ids[j])
			ids = slices.Delete(ids, j, j+1)
		}
		if i%97 == 0 { // the view catches up at irregular intervals
			if _, err := v.CatchUp(tb); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := v.CatchUp(tb); err != nil {
		t.Fatal(err)
	}
	if got, want := v.All(), Recompute(tb.Rows()); !slices.Equal(got, want) {
		t.Fatalf("incremental %v\nrecompute   %v", got, want)
	}
}

func TestConcurrentWritersAndCatchUp(t *testing.T) {
	tb := NewTable()
	v := NewView()
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				_ = tb.Insert(Order{ID: fmt.Sprintf("%d-%d", w, i), Customer: fmt.Sprint(w), Status: "placed", Amount: 1})
				if i%20 == 0 {
					if _, err := v.CatchUp(tb); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	_, _ = v.CatchUp(tb)
	if got, want := v.All(), Recompute(tb.Rows()); !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}
