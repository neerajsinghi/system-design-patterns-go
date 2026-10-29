package modelrouter

import (
	"errors"
	"sync"
	"time"
)

// Decision is what the budget allows for the next request.
type Decision int

const (
	// Allow routes normally.
	Allow Decision = iota
	// Downgrade forces the cheapest tier and disables escalation.
	Downgrade
	// Reject refuses the request with ErrBudgetExceeded.
	Reject
)

func (d Decision) String() string {
	switch d {
	case Allow:
		return "allow"
	case Downgrade:
		return "downgrade"
	default:
		return "reject"
	}
}

// Budget tracks dollars spent per tenant per UTC day. Past the soft limit
// requests are downgraded; at or past the hard limit they are rejected. The
// counters reset at UTC midnight. It is safe for concurrent use.
//
// Check and Charge are separate steps, so concurrent in-flight requests can
// overshoot the hard limit by up to one request each. Production systems
// either accept that or reserve an estimated cost up front.
type Budget struct {
	soft, hard float64
	now        func() time.Time

	mu    sync.Mutex
	spent map[string]*daySpend
}

type daySpend struct {
	day   time.Time
	spent float64
}

// BudgetOption configures a Budget.
type BudgetOption func(*Budget)

// WithBudgetClock injects a clock so day boundaries are testable.
func WithBudgetClock(now func() time.Time) BudgetOption {
	return func(b *Budget) {
		if now != nil {
			b.now = now
		}
	}
}

// NewBudget returns a budget with the given soft and hard daily limits in
// dollars per tenant. Use soft == hard to reject without a downgrade phase.
func NewBudget(soft, hard float64, opts ...BudgetOption) (*Budget, error) {
	if soft <= 0 || hard <= 0 {
		return nil, errors.New("modelrouter: budget limits must be positive")
	}
	if soft > hard {
		return nil, errors.New("modelrouter: soft budget limit must not exceed the hard limit")
	}
	b := &Budget{soft: soft, hard: hard, now: time.Now, spent: map[string]*daySpend{}}
	for _, opt := range opts {
		opt(b)
	}
	return b, nil
}

func dayOf(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// current returns tenant's record for today, resetting it on a new day.
// The caller must hold b.mu.
func (b *Budget) current(tenant string) *daySpend {
	today := dayOf(b.now())
	s := b.spent[tenant]
	if s == nil {
		s = &daySpend{day: today}
		b.spent[tenant] = s
	}
	if !s.day.Equal(today) {
		s.day, s.spent = today, 0
	}
	return s
}

// Check returns the decision for tenant's next request.
func (b *Budget) Check(tenant string) Decision {
	b.mu.Lock()
	defer b.mu.Unlock()
	spent := b.current(tenant).spent
	switch {
	case spent >= b.hard:
		return Reject
	case spent >= b.soft:
		return Downgrade
	default:
		return Allow
	}
}

// Charge adds cost to tenant's spend for today.
func (b *Budget) Charge(tenant string, cost float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.current(tenant).spent += cost
}

// Spent returns tenant's spend for today.
func (b *Budget) Spent(tenant string) float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.current(tenant).spent
}
