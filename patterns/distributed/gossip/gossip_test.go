package gossip

import (
	"errors"
	"testing"
)

func TestConfigValidation(t *testing.T) {
	bad := []Config{
		{Nodes: 1, Fanout: 1, SuspectAfter: 1},
		{Nodes: 3, Fanout: 0, SuspectAfter: 1},
		{Nodes: 3, Fanout: 1, SuspectAfter: 0},
		{Nodes: 3, Fanout: 1, SuspectAfter: 1, DropRate: 1},
		{Nodes: 3, Fanout: 1, SuspectAfter: 1, DropRate: -0.1},
	}
	for _, cfg := range bad {
		if _, err := NewCluster(cfg); !errors.Is(err, ErrBadConfig) {
			t.Errorf("NewCluster(%+v) err=%v", cfg, err)
		}
	}
}

func TestConvergence(t *testing.T) {
	tests := []struct {
		name      string
		nodes     int
		fanout    int
		drop      float64
		maxRounds int
	}{
		{"small cluster", 8, 1, 0, 15},
		{"64 nodes fanout 2", 64, 2, 0, 20},
		{"64 nodes with 30% message loss", 64, 2, 0.3, 30},
		{"128 nodes fanout 3", 128, 3, 0.1, 25},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for seed := uint64(1); seed <= 3; seed++ {
				c, err := NewCluster(Config{Nodes: tt.nodes, Fanout: tt.fanout, DropRate: tt.drop, SuspectAfter: 10, Seed: seed})
				if err != nil {
					t.Fatal(err)
				}
				last := c.ids[len(c.ids)-1]
				if err := c.Set(last, "config", "v1"); err != nil {
					t.Fatal(err)
				}
				rounds, ok := c.RunUntilConverged("config", tt.maxRounds)
				if !ok {
					t.Fatalf("seed %d: not converged after %d rounds", seed, rounds)
				}
				if v, _ := c.Get("n0", "config"); v.Value != "v1" || v.Origin != last {
					t.Fatalf("seed %d: n0 sees %+v", seed, v)
				}
			}
		})
	}
}

func TestNewerVersionWinsEverywhere(t *testing.T) {
	c, _ := NewCluster(Config{Nodes: 16, Fanout: 2, SuspectAfter: 10, Seed: 7})
	_ = c.Set("n3", "k", "old")
	c.RunUntilConverged("k", 30)
	_ = c.Set("n3", "k", "new") // version 2 at n3
	_ = c.Set("n9", "k", "tie") // n9 had version 1 -> also version 2, origin n9 > n3 wins ties
	if _, ok := c.RunUntilConverged("k", 30); !ok {
		t.Fatal("not converged")
	}
	for _, id := range c.ids {
		if v, _ := c.Get(id, "k"); v.Value != "tie" || v.Version != 2 {
			t.Fatalf("%s sees %+v", id, v)
		}
	}
}

func TestFailureDetection(t *testing.T) {
	c, _ := NewCluster(Config{Nodes: 20, Fanout: 2, SuspectAfter: 8, Seed: 3})
	_ = c.Set("n0", "k", "v")
	if _, ok := c.RunUntilConverged("k", 30); !ok {
		t.Fatal("not converged")
	}
	for i := 0; i < 20; i++ {
		c.Round()
	}
	for _, id := range c.ids {
		if s := c.Suspects(id); len(s) != 0 {
			t.Fatalf("healthy cluster: %s suspects %v", id, s)
		}
	}
	_ = c.Crash("n5")
	for i := 0; i < 20; i++ {
		c.Round()
	}
	for _, id := range c.ids {
		if id == "n5" {
			continue
		}
		if s := c.Suspects(id); len(s) != 1 || s[0] != "n5" {
			t.Fatalf("%s suspects %v, want [n5]", id, s)
		}
	}
	if err := c.Set("n5", "k", "x"); !errors.Is(err, ErrUnknownNode) {
		t.Fatal("crashed node must reject writes")
	}
}

func TestDeterministicWithSeed(t *testing.T) {
	run := func() (int, int) {
		c, _ := NewCluster(Config{Nodes: 32, Fanout: 2, DropRate: 0.2, SuspectAfter: 10, Seed: 42})
		_ = c.Set("n31", "k", "v")
		r, _ := c.RunUntilConverged("k", 50)
		return r, c.Messages()
	}
	r1, m1 := run()
	r2, m2 := run()
	if r1 != r2 || m1 != m2 {
		t.Fatalf("runs differ: (%d,%d) vs (%d,%d)", r1, m1, r2, m2)
	}
}
