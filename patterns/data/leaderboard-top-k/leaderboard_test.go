package leaderboard

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"sync"
	"testing"
)

// oracle sorts a plain map, the "ORDER BY score DESC, member" answer.
func oracle(scores map[string]int64) []Entry {
	out := make([]Entry, 0, len(scores))
	for m, s := range scores {
		out = append(out, Entry{Member: m, Score: s})
	}
	sort.Slice(out, func(i, j int) bool { return ahead(out[i].Score, out[i].Member, out[j].Score, out[j].Member) })
	for i := range out {
		out[i].Rank = i
	}
	return out
}

func mustSet(t *testing.T, opts ...Option) *SortedSet {
	t.Helper()
	s, err := NewSortedSet(opts...)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func checkAgainstOracle(t *testing.T, s *SortedSet, want map[string]int64) {
	t.Helper()
	exp := oracle(want)
	if s.Len() != len(exp) {
		t.Fatalf("Len = %d, want %d", s.Len(), len(exp))
	}
	got := s.Range(0, -1)
	if len(got) != len(exp) {
		t.Fatalf("Range(0,-1) has %d entries, want %d", len(got), len(exp))
	}
	for i := range exp {
		if got[i] != exp[i] {
			t.Fatalf("rank %d: got %+v, want %+v", i, got[i], exp[i])
		}
		r, ok := s.Rank(exp[i].Member)
		if !ok || r != i {
			t.Fatalf("Rank(%s) = %d,%v, want %d", exp[i].Member, r, ok, i)
		}
		if one := s.Range(i, i); len(one) != 1 || one[0] != exp[i] {
			t.Fatalf("Range(%d,%d) = %+v, want %+v", i, i, one, exp[i])
		}
	}
}

func TestRankMatchesOracleUnderRandomOps(t *testing.T) {
	for _, seed := range []uint64{1, 2, 3} {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			r := rand.New(rand.NewPCG(seed, 99))
			s := mustSet(t, WithSeed(seed, 7))
			want := map[string]int64{}
			for op := range 4000 {
				m := fmt.Sprintf("p%03d", r.IntN(300))
				switch r.IntN(10) {
				case 0, 1, 2:
					sc := r.Int64N(50) // small range: many ties
					if err := s.Add(m, sc); err != nil {
						t.Fatal(err)
					}
					want[m] = sc
				case 3, 4, 5, 6, 7:
					d := r.Int64N(21) - 10
					got, err := s.IncrBy(m, d)
					if err != nil {
						t.Fatal(err)
					}
					want[m] += d
					if got != want[m] {
						t.Fatalf("IncrBy = %d, want %d", got, want[m])
					}
				default:
					_, had := want[m]
					if s.Remove(m) != had {
						t.Fatalf("Remove(%s) mismatch", m)
					}
					delete(want, m)
				}
				if op%250 == 0 {
					checkAgainstOracle(t, s, want)
				}
			}
			checkAgainstOracle(t, s, want)
			for m, sc := range want {
				if got, ok := s.Score(m); !ok || got != sc {
					t.Fatalf("Score(%s) = %d,%v want %d", m, got, ok, sc)
				}
			}
		})
	}
}

func TestTiesBrokenByMemberName(t *testing.T) {
	s := mustSet(t)
	for _, m := range []string{"carol", "alice", "dave", "bob"} {
		s.Add(m, 100)
	}
	s.Add("eve", 200)
	var got []string
	for _, e := range s.Range(0, -1) {
		got = append(got, e.Member)
	}
	want := []string{"eve", "alice", "bob", "carol", "dave"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	// Moving a tied player keeps the others' relative order.
	s.IncrBy("carol", 0)
	s.IncrBy("bob", -1)
	if r, _ := s.Rank("bob"); r != 4 {
		t.Fatalf("bob rank = %d, want 4", r)
	}
	if r, _ := s.Rank("carol"); r != 2 {
		t.Fatalf("carol rank = %d, want 2", r)
	}
}

func names(es []Entry) string {
	var out []string
	for _, e := range es {
		out = append(out, e.Member)
	}
	return fmt.Sprint(out)
}

func fiveSet(t *testing.T) *SortedSet {
	s := mustSet(t)
	for i, m := range []string{"a", "b", "c", "d", "e"} {
		s.Add(m, int64(50-i*10)) // a=50 ... e=10
	}
	return s
}

func TestRangeBounds(t *testing.T) {
	s := fiveSet(t)
	tests := []struct {
		start, stop int
		want        string
	}{
		{0, 2, "[a b c]"},
		{0, -1, "[a b c d e]"},
		{-2, -1, "[d e]"},
		{3, 100, "[d e]"},
		{-100, 0, "[a]"},
		{4, 4, "[e]"},
		{5, 10, "[]"},
		{3, 2, "[]"},
		{-1, -2, "[]"},
	}
	for _, tt := range tests {
		got := s.Range(tt.start, tt.stop)
		if names(got) != tt.want {
			t.Errorf("Range(%d,%d) = %s, want %s", tt.start, tt.stop, names(got), tt.want)
		}
		for _, e := range got {
			if r, _ := s.Rank(e.Member); r != e.Rank {
				t.Errorf("entry %+v has wrong Rank, want %d", e, r)
			}
		}
	}
	if got := s.Top(2); names(got) != "[a b]" {
		t.Errorf("Top(2) = %s", names(got))
	}
	if s.Top(0) != nil {
		t.Error("Top(0) should be nil")
	}
	empty := mustSet(t)
	if empty.Range(0, -1) != nil {
		t.Error("Range on empty set should be nil")
	}
}

func TestAroundEdges(t *testing.T) {
	s := fiveSet(t)
	tests := []struct {
		member string
		n      int
		want   string
	}{
		{"c", 1, "[b c d]"},
		{"a", 2, "[a b c]"}, // top: cut short above
		{"e", 2, "[c d e]"}, // bottom: cut short below
		{"c", 0, "[c]"},
		{"c", 100, "[a b c d e]"},
		{"c", math.MaxInt, "[a b c d e]"},
	}
	for _, tt := range tests {
		got, err := s.Around(tt.member, tt.n)
		if err != nil || names(got) != tt.want {
			t.Errorf("Around(%s,%d) = %s,%v want %s", tt.member, tt.n, names(got), err, tt.want)
		}
	}
	if _, err := s.Around("zed", 1); !errors.Is(err, ErrNotMember) {
		t.Errorf("Around(missing) err = %v", err)
	}
	if _, err := s.Around("c", -1); !errors.Is(err, ErrNegativeN) {
		t.Errorf("Around(n<0) err = %v", err)
	}
}

func TestRemove(t *testing.T) {
	s := fiveSet(t)
	if !s.Remove("c") || s.Remove("c") {
		t.Fatal("Remove should succeed once")
	}
	if _, ok := s.Rank("c"); ok {
		t.Fatal("removed member still ranked")
	}
	if _, ok := s.Score("c"); ok {
		t.Fatal("removed member still scored")
	}
	if names(s.Range(0, -1)) != "[a b d e]" {
		t.Fatalf("after remove: %s", names(s.Range(0, -1)))
	}
	if r, _ := s.Rank("d"); r != 2 {
		t.Fatalf("d rank = %d, want 2", r)
	}
	for _, m := range []string{"a", "b", "d", "e"} {
		s.Remove(m)
	}
	if s.Len() != 0 || s.Range(0, -1) != nil {
		t.Fatal("set should be empty")
	}
	s.Add("x", 1) // reusable after emptying
	if r, ok := s.Rank("x"); !ok || r != 0 {
		t.Fatal("re-add after empty failed")
	}
}

func TestRankIsLogarithmic(t *testing.T) {
	const n = 50_000
	s := mustSet(t, WithSeed(5, 6))
	r := rand.New(rand.NewPCG(1, 1))
	for i := range n {
		s.Add(fmt.Sprintf("p%d", i), r.Int64N(1_000_000))
	}
	total := 0
	for range 1000 {
		_, steps, ok := s.RankSteps(fmt.Sprintf("p%d", r.IntN(n)))
		if !ok {
			t.Fatal("missing member")
		}
		total += steps
	}
	avg := float64(total) / 1000
	// Expected about (1/p)·log_{1/p}(n) ≈ 4·7.8 ≈ 31; a linear scan is 50,000.
	if avg > 80 {
		t.Fatalf("average rank steps = %.1f, want O(log n)", avg)
	}
}

func TestValidation(t *testing.T) {
	bad := []struct {
		name string
		opts []Option
	}{
		{"max level 0", []Option{WithMaxLevel(0)}},
		{"max level 65", []Option{WithMaxLevel(65)}},
		{"p 0", []Option{WithP(0)}},
		{"p 1", []Option{WithP(1)}},
		{"p NaN", []Option{WithP(math.NaN())}},
	}
	for _, tt := range bad {
		if _, err := NewSortedSet(tt.opts...); err == nil {
			t.Errorf("%s: expected error", tt.name)
		}
		if _, err := NewLeaderboard(tt.opts...); err == nil {
			t.Errorf("%s: expected error from NewLeaderboard", tt.name)
		}
	}
	s := mustSet(t, WithMaxLevel(1)) // a plain sorted linked list still works
	one := map[string]int64{}
	for i := range 20 {
		s.Add(fmt.Sprint(i), int64(i%4))
		one[fmt.Sprint(i)] = int64(i % 4)
	}
	checkAgainstOracle(t, s, one)
	if err := s.Add("", 1); !errors.Is(err, ErrEmptyMember) {
		t.Errorf("Add empty: %v", err)
	}
	if _, err := s.IncrBy("", 1); !errors.Is(err, ErrEmptyMember) {
		t.Errorf("IncrBy empty: %v", err)
	}
	s.Add("max", math.MaxInt64)
	if _, err := s.IncrBy("max", 1); !errors.Is(err, ErrOverflow) {
		t.Errorf("IncrBy overflow: %v", err)
	}
	s.Add("min", math.MinInt64)
	if _, err := s.IncrBy("min", -1); !errors.Is(err, ErrOverflow) {
		t.Errorf("IncrBy underflow: %v", err)
	}
	if v, _ := s.Score("max"); v != math.MaxInt64 {
		t.Errorf("score changed on overflow: %d", v)
	}
}

func TestLeaderboardConcurrent(t *testing.T) {
	lb, err := NewLeaderboard(WithSeed(3, 3))
	if err != nil {
		t.Fatal(err)
	}
	const writers, players, rounds = 8, 50, 200
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range rounds {
				lb.IncrBy(fmt.Sprintf("p%d", (w*rounds+i)%players), 1)
			}
		}()
	}
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range rounds {
				lb.Top(10)
				lb.Rank("p1")
				lb.Score("p2")
				lb.Range(5, 15)
				lb.Around("p3", 2)
				lb.Len()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range rounds {
			lb.Add("temp", 1)
			lb.Remove("temp")
		}
	}()
	wg.Wait()
	var total int64
	for _, e := range lb.Range(0, -1) {
		total += e.Score
	}
	if total != writers*rounds {
		t.Fatalf("total score = %d, want %d (lost updates)", total, writers*rounds)
	}
	if lb.Len() != players {
		t.Fatalf("Len = %d, want %d", lb.Len(), players)
	}
}
