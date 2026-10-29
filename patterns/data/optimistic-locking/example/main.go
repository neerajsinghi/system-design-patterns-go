// Command example books concert seats three ways: naive read-then-write
// (double-books), optimistic locking with compare-and-set (exactly one
// winner), and a lock per key (no retries, but writers wait in line). It then
// compares optimistic and pessimistic locking under low and high contention by
// counting conflicts, which is deterministic, rather than timing anything.
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	optimisticlocking "github.com/neerajsinghi/system-design-patterns-go/patterns/data/optimistic-locking"
)

var (
	errTaken   = errors.New("seat already taken")
	errSoldOut = errors.New("sold out")
)

type store = optimisticlocking.Store[string, string]

func book(buyer string) optimisticlocking.UpdateFunc[string] {
	return func(cur optimisticlocking.Versioned[string]) (string, error) {
		if cur.Value != "" {
			return "", errTaken
		}
		return buyer, nil
	}
}

func step1Naive() {
	fmt.Println("1) Naive read-then-write: five buyers open seat A1 at the same moment")
	s := optimisticlocking.NewStore[string, string]()
	buyers := []string{"alice", "bob", "carol", "dave", "erin"}
	confirmed := 0
	reads := make([]optimisticlocking.Versioned[string], len(buyers))
	for i := range buyers { // everyone loads the page before anyone clicks "book"
		reads[i] = s.Get("A1")
	}
	for i, b := range buyers {
		if reads[i].Value == "" { // looked free when they read it
			s.Put("A1", b)
			confirmed++
		}
	}
	fmt.Printf("   confirmation emails sent: %d for 1 seat; seat now belongs to %q\n", confirmed, s.Get("A1").Value)
	fmt.Printf("   %d buyers will turn up holding a ticket the database no longer knows about\n\n", confirmed-1)
}

func step2CAS() {
	fmt.Println("2) Same interleaving with a version check (CompareAndSet)")
	s := optimisticlocking.NewStore[string, string]()
	a, b := s.Get("A1"), s.Get("A1")
	fmt.Printf("   alice and bob both read A1: free at version %d\n", a.Version)
	v, _ := s.CompareAndSet("A1", a.Version, "alice")
	fmt.Printf("   alice writes at version %d -> ok, A1 is now version %d\n", a.Version, v)
	_, err := s.CompareAndSet("A1", b.Version, "bob")
	fmt.Printf("   bob writes at version %d   -> %v\n", b.Version, err)
	_, err = s.Update(context.Background(), "A1", book("bob"), 3)
	fmt.Printf("   bob retries with Update: re-reads, sees alice -> %v\n\n", err)
}

func step3Goroutines() {
	const buyers = 1000
	fmt.Printf("3) %d goroutines call Update for seat A1 at once (real concurrency)\n", buyers)
	s := optimisticlocking.NewStore[string, string]()
	var wins, taken, other atomic.Int64
	var wg sync.WaitGroup
	for i := range buyers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Update(context.Background(), "A1", book(fmt.Sprint("buyer-", i)), 5)
			switch {
			case err == nil:
				wins.Add(1)
			case errors.Is(err, errTaken):
				taken.Add(1)
			default:
				other.Add(1)
			}
		}()
	}
	wg.Wait()
	fmt.Printf("   booked: %d, told \"seat already taken\": %d, other errors: %d, writes to A1: %d\n",
		wins.Load(), taken.Load(), other.Load(), s.Stats().Writes)
	fmt.Println("   (how many of the losers hit a conflict first depends on scheduling; the outcome does not)")
	fmt.Println()
}

// client is one buyer in the lock-step simulation.
type client struct {
	key      string
	fn       func(cur string) (string, error)
	attempts int
	done     bool
	err      error
}

// lockstep runs clients in rounds. In each round every unfinished client
// reads its key, then each one in turn computes its new value and tries
// CompareAndSet. This is the worst case for optimism: everyone who is still
// trying reads at the same instant. It is deterministic.
func lockstep(s *store, clients []*client, maxAttempts int) {
	for {
		var active []*client
		for _, c := range clients {
			if !c.done {
				active = append(active, c)
			}
		}
		if len(active) == 0 {
			return
		}
		reads := make([]optimisticlocking.Versioned[string], len(active))
		for i, c := range active {
			reads[i] = s.Get(c.key)
		}
		for i, c := range active {
			c.attempts++
			next, err := c.fn(reads[i].Value)
			if err != nil {
				c.done, c.err = true, err
				continue
			}
			if _, err := s.CompareAndSet(c.key, reads[i].Version, next); err == nil {
				c.done = true
			} else if c.attempts == maxAttempts {
				c.done, c.err = true, optimisticlocking.ErrTooManyConflicts
			}
		}
	}
}

type outcome struct{ sold, gaveUp, soldOut, attempts, conflicts int }

func summarise(s *store, clients []*client) outcome {
	var o outcome
	for _, c := range clients {
		o.attempts += c.attempts
		switch {
		case c.err == nil:
			o.sold++
		case errors.Is(c.err, optimisticlocking.ErrTooManyConflicts):
			o.gaveUp++
		default:
			o.soldOut++
		}
	}
	o.conflicts = int(s.Stats().Conflicts)
	return o
}

const buyers = 100

func lowContention(maxAttempts int) outcome {
	s := optimisticlocking.NewStore[string, string]()
	clients := make([]*client, buyers)
	for i := range clients {
		name := fmt.Sprint("buyer-", i)
		clients[i] = &client{key: fmt.Sprint("seat-", i), fn: func(cur string) (string, error) {
			if cur != "" {
				return "", errTaken
			}
			return name, nil
		}}
	}
	lockstep(s, clients, maxAttempts)
	return summarise(s, clients)
}

// decrement takes one ticket from a general-admission pool stored as a
// decimal string, so every buyer writes the same key.
func decrement(cur string) (string, error) {
	var n int
	fmt.Sscan(cur, &n)
	if n == 0 {
		return "", errSoldOut
	}
	return fmt.Sprint(n - 1), nil
}

func highContention(maxAttempts int) outcome {
	s := optimisticlocking.NewStore[string, string]()
	s.Put("ga-pool", fmt.Sprint(buyers))
	clients := make([]*client, buyers)
	for i := range clients {
		clients[i] = &client{key: "ga-pool", fn: decrement}
	}
	lockstep(s, clients, maxAttempts)
	return summarise(s, clients)
}

// lockedPool is the pessimistic alternative: a mutex per key, held across
// the whole read-modify-write. No conflicts are possible, so nothing retries.
type lockedPool struct {
	mu     sync.Mutex
	locks  map[string]*sync.Mutex
	values map[string]string
	writes int
}

func (p *lockedPool) lockFor(key string) *sync.Mutex {
	p.mu.Lock()
	defer p.mu.Unlock()
	l, ok := p.locks[key]
	if !ok {
		l = &sync.Mutex{}
		p.locks[key] = l
	}
	return l
}

func (p *lockedPool) update(key string, fn func(string) (string, error)) error {
	l := p.lockFor(key)
	l.Lock()
	defer l.Unlock()
	p.mu.Lock()
	cur := p.values[key]
	p.mu.Unlock()
	next, err := fn(cur) // the key is locked while we think
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.values[key] = next
	p.writes++
	p.mu.Unlock()
	return nil
}

func pessimistic() (sold, writes int) {
	p := &lockedPool{locks: map[string]*sync.Mutex{}, values: map[string]string{"ga-pool": fmt.Sprint(buyers)}}
	var ok atomic.Int64
	var wg sync.WaitGroup
	for range buyers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if p.update("ga-pool", decrement) == nil {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	return int(ok.Load()), p.writes
}

func printOutcome(label string, o outcome) {
	fmt.Printf("   %-48s sold %3d  gave up %3d  CAS attempts %5d  conflicts %5d\n",
		label, o.sold, o.gaveUp, o.attempts, o.conflicts)
}

func step4Contention() {
	fmt.Printf("4) Contention: %d buyers, worst-case lock-step rounds (all retrying buyers read at the same instant)\n", buyers)
	printOutcome("low: each buyer books a different seat, max 5", lowContention(5))
	printOutcome("high: all decrement one 100-ticket pool, max 5", highContention(5))
	printOutcome("high: same, max 100 attempts", highContention(100))
	sold, writes := pessimistic()
	fmt.Printf("   %-48s sold %3d  gave up %3d  writes %d, conflicts 0 (buyers queue on the lock instead)\n",
		"pessimistic: one mutex for the pool", sold, buyers-sold, writes)
	fmt.Println()
	fmt.Println("   Low contention: every first attempt succeeds, so optimism costs nothing.")
	fmt.Println("   High contention: each round only one buyer can win the pool, so a retry budget of 5")
	fmt.Println("   sells 5 tickets and turns 95 buyers away from a pool that still has tickets. A big")
	fmt.Println("   budget sells all 100 but does 5050 attempts (about 50 per sale). For a hot key like this,")
	fmt.Println("   a lock per key or a queue that applies the decrements one by one is the better fit.")
}

func main() {
	step1Naive()
	step2CAS()
	step3Goroutines()
	step4Contention()
}
