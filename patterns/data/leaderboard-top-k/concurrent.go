package leaderboard

import "sync"

// Leaderboard is a SortedSet guarded by a read-write lock, safe for
// concurrent use. Reads (Rank, Score, Range, Around) share the lock; writes
// are serialised, and IncrBy is an atomic read-modify-write, like a single
// Redis command.
type Leaderboard struct {
	mu  sync.RWMutex
	set *SortedSet
}

// NewLeaderboard returns an empty concurrent leaderboard.
func NewLeaderboard(opts ...Option) (*Leaderboard, error) {
	s, err := NewSortedSet(opts...)
	if err != nil {
		return nil, err
	}
	return &Leaderboard{set: s}, nil
}

// Add sets member's score.
func (l *Leaderboard) Add(member string, score int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.set.Add(member, score)
}

// IncrBy adds delta to member's score and returns the new score.
func (l *Leaderboard) IncrBy(member string, delta int64) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.set.IncrBy(member, delta)
}

// Remove deletes member.
func (l *Leaderboard) Remove(member string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.set.Remove(member)
}

// Len returns the number of members.
func (l *Leaderboard) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.set.Len()
}

// Score returns member's score.
func (l *Leaderboard) Score(member string) (int64, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.set.Score(member)
}

// Rank returns member's 0-based rank.
func (l *Leaderboard) Rank(member string) (int, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.set.Rank(member)
}

// Top returns the first n members.
func (l *Leaderboard) Top(n int) []Entry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.set.Top(n)
}

// Range returns ranks start..stop inclusive; see SortedSet.Range.
func (l *Leaderboard) Range(start, stop int) []Entry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.set.Range(start, stop)
}

// Around returns member and up to n neighbours on each side.
func (l *Leaderboard) Around(member string, n int) ([]Entry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.set.Around(member, n)
}
