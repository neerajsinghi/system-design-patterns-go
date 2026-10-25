package lamportvectorclocks

import (
	"sync"
	"testing"
)

func TestLamportClock(t *testing.T) {
	var c LamportClock
	if c.Tick() != 1 || c.Tick() != 2 {
		t.Fatal("tick")
	}
	if got := c.Observe(10); got != 11 {
		t.Fatalf("observe ahead: %d", got)
	}
	if got := c.Observe(3); got != 12 {
		t.Fatalf("observe behind: %d", got)
	}
	if c.Time() != 12 {
		t.Fatal("time")
	}
}

func TestLamportConcurrentSafe(t *testing.T) {
	var c LamportClock
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				c.Tick()
			}
		}()
	}
	wg.Wait()
	if c.Time() != 800 {
		t.Fatalf("time=%d", c.Time())
	}
}

func TestLamportStampTotalOrder(t *testing.T) {
	a := LamportStamp{3, "A"}
	b := LamportStamp{3, "B"}
	c := LamportStamp{4, "A"}
	if !a.Less(b) || b.Less(a) || !b.Less(c) || a.Less(a) {
		t.Fatal("total order broken")
	}
}

func TestVectorCompare(t *testing.T) {
	tests := []struct {
		name string
		a, b VectorClock
		want Ordering
	}{
		{"both empty", VectorClock{}, VectorClock{}, Equal},
		{"missing entries are zero", VectorClock{"A": 0}, VectorClock{}, Equal},
		{"strictly before", VectorClock{"A": 1}, VectorClock{"A": 2, "B": 1}, Before},
		{"after", VectorClock{"A": 2, "B": 1}, VectorClock{"A": 2}, After},
		{"concurrent", VectorClock{"A": 2, "B": 0}, VectorClock{"A": 1, "B": 1}, Concurrent},
		{"concurrent disjoint", VectorClock{"A": 1}, VectorClock{"B": 1}, Concurrent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Compare(tt.b); got != tt.want {
				t.Fatalf("got %v want %v", got, tt.want)
			}
			// Compare must be antisymmetric.
			inverse := map[Ordering]Ordering{Equal: Equal, Before: After, After: Before, Concurrent: Concurrent}
			if got := tt.b.Compare(tt.a); got != inverse[tt.want] {
				t.Fatalf("reverse got %v", got)
			}
		})
	}
}

func TestVectorImmutability(t *testing.T) {
	v := VectorClock{"A": 1}
	w := v.Increment("A")
	m := v.Merge(VectorClock{"B": 5})
	if v["A"] != 1 || len(v) != 1 || w["A"] != 2 || m["B"] != 5 {
		t.Fatalf("v=%v w=%v m=%v", v, w, m)
	}
	if s := (VectorClock{"B": 1, "A": 2}).String(); s != "{A:2 B:1}" {
		t.Fatalf("String=%s", s)
	}
}

// TestMessageScenario: A sends to B, B sends to C, while C also has an
// independent local event.
func TestMessageScenario(t *testing.T) {
	A, B, C := NewProcess("A"), NewProcess("B"), NewProcess("C")
	a1 := A.Local()
	m1 := A.Send("m1")
	c1 := C.Local()
	b1 := B.Receive(m1)
	m2 := B.Send("m2")
	c2 := C.Receive(m2)

	tests := []struct {
		name string
		x, y Stamp
		want Ordering
	}{
		{"a1 -> b1 via message", a1, b1, Before},
		{"a1 -> c2 transitively", a1, c2, Before},
		{"c1 || send m1", c1, m1.Stamp, Concurrent},
		{"c1 || b1", c1, b1, Concurrent},
		{"c1 -> c2 same process", c1, c2, Before},
		{"m2 send -> c2", m2.Stamp, c2, Before},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.x.Vector.Compare(tt.y.Vector); got != tt.want {
				t.Fatalf("got %v want %v (%v vs %v)", got, tt.want, tt.x.Vector, tt.y.Vector)
			}
			// Clock condition: happens-before implies smaller Lamport time.
			if tt.want == Before && tt.x.Lamport.Time >= tt.y.Lamport.Time {
				t.Fatalf("lamport violates clock condition: %d >= %d", tt.x.Lamport.Time, tt.y.Lamport.Time)
			}
		})
	}
	// Lamport cannot tell concurrency apart: c1 has a smaller time than m1's
	// send although they are unrelated.
	if !(c1.Lamport.Time < m1.Stamp.Lamport.Time) {
		t.Fatal("expected misleading lamport order for concurrent events")
	}
	if Ordering(99).String() != "unknown" {
		t.Fatal("out of range ordering")
	}
}
