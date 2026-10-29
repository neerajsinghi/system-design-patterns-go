// Package leaderboard implements the two data structures behind "top N"
// features: a ranked sorted set for leaderboards, and bounded-memory top-K
// summaries for trending items in a stream.
//
// A game or contest leaderboard with millions of players must answer three
// questions on almost every request: who is in the top 100, what is my rank,
// and who is just above and below me. Against a plain table those become
// "ORDER BY score DESC LIMIT 100" and "COUNT(*) WHERE score > mine", and with
// heavy score updates neither an index-free sort nor a full count scales.
//
// SortedSet is the structure Redis uses for ZSETs: a skip list ordered by
// score, where every forward pointer also records its span (how many nodes it
// jumps over), plus a hash map from member to score. Summing spans while
// searching gives a member's rank in O(log n) expected steps, and a rank can be
// turned back into a node the same way, so Rank, Range and Around are all
// O(log n + m). Leaderboard wraps it with a lock for concurrent use.
//
// For trending items ("top hashtags in the last hour") the key set is
// unbounded, so exact counts may not fit. TopK picks the top K from exact
// counts with a size-K heap when the key set is bounded; SpaceSaving keeps
// only k counters and still guarantees that every item with more than N/k
// occurrences is present, with each count overestimated by at most the
// smallest counter.
package leaderboard

import (
	"errors"
	"math/rand/v2"
)

// Errors returned by SortedSet and Leaderboard.
var (
	ErrEmptyMember = errors.New("leaderboard: member must not be empty")
	ErrNotMember   = errors.New("leaderboard: member not found")
	ErrOverflow    = errors.New("leaderboard: score overflow")
	ErrNegativeN   = errors.New("leaderboard: n must not be negative")
)

// Entry is one member in rank order. Rank is 0-based: the leader has rank 0.
type Entry struct {
	Rank   int
	Member string
	Score  int64
}

type level struct {
	forward *node
	span    int // number of nodes this link moves forward by
}

type node struct {
	member string
	score  int64
	levels []level
}

// ahead reports whether (aScore, aMember) ranks ahead of (bScore, bMember):
// higher score first, and on equal scores the lexicographically smaller
// member name first.
func ahead(aScore int64, aMember string, bScore int64, bMember string) bool {
	if aScore != bScore {
		return aScore > bScore
	}
	return aMember < bMember
}

type config struct {
	seed1, seed2 uint64
	maxLevel     int
	p            float64
}

// Option configures a SortedSet or Leaderboard.
type Option func(*config)

// WithSeed seeds the generator that picks node levels, so the skip list's
// shape (and step counts) are reproducible. The default seed is (1, 2).
func WithSeed(s1, s2 uint64) Option { return func(c *config) { c.seed1, c.seed2 = s1, s2 } }

// WithMaxLevel caps the number of levels (1..64, default 32). 32 levels at
// p = 0.25 are enough for 2^64 members, which is why Redis uses the same cap.
func WithMaxLevel(n int) Option { return func(c *config) { c.maxLevel = n } }

// WithP sets the probability that a node is promoted to the next level
// (0 < p < 1, default 0.25). Smaller p means fewer pointers per node but more
// steps per level.
func WithP(p float64) Option { return func(c *config) { c.p = p } }

func buildConfig(opts []Option) (config, error) {
	c := config{seed1: 1, seed2: 2, maxLevel: 32, p: 0.25}
	for _, o := range opts {
		o(&c)
	}
	if c.maxLevel < 1 || c.maxLevel > 64 {
		return c, errors.New("leaderboard: max level must be between 1 and 64")
	}
	if !(c.p > 0 && c.p < 1) {
		return c, errors.New("leaderboard: p must be in (0, 1)")
	}
	return c, nil
}

// SortedSet is a skip list with span counts plus a member-to-score map, the
// same layout as a Redis sorted set. Members are ordered by score descending;
// ties are broken by member name ascending, so the order is total and
// deterministic.
//
// SortedSet is not safe for concurrent use; see Leaderboard.
type SortedSet struct {
	head     *node
	level    int // levels currently in use, at least 1
	length   int
	scores   map[string]int64
	rng      *rand.Rand
	maxLevel int
	p        float64

	// scratch space reused by link and remove
	update []*node
	rank   []int
}

// NewSortedSet returns an empty sorted set.
func NewSortedSet(opts ...Option) (*SortedSet, error) {
	c, err := buildConfig(opts)
	if err != nil {
		return nil, err
	}
	return &SortedSet{
		head:     &node{levels: make([]level, c.maxLevel)},
		level:    1,
		scores:   make(map[string]int64),
		rng:      rand.New(rand.NewPCG(c.seed1, c.seed2)),
		maxLevel: c.maxLevel,
		p:        c.p,
		update:   make([]*node, c.maxLevel),
		rank:     make([]int, c.maxLevel),
	}, nil
}

// Len returns the number of members.
func (s *SortedSet) Len() int { return s.length }

// Add sets member's score, inserting the member if it is new.
func (s *SortedSet) Add(member string, score int64) error {
	if member == "" {
		return ErrEmptyMember
	}
	if old, ok := s.scores[member]; ok {
		if old == score {
			return nil
		}
		// Unlink and relink the same node at its new position, keeping its
		// height, as Redis does: no allocation per score update.
		n := s.remove(member, old)
		n.score = score
		s.link(n)
	} else {
		s.link(&node{member: member, score: score, levels: make([]level, s.randomLevel())})
	}
	s.scores[member] = score
	return nil
}

// IncrBy adds delta to member's score (a new member starts at 0) and returns
// the new score, like Redis ZINCRBY.
func (s *SortedSet) IncrBy(member string, delta int64) (int64, error) {
	if member == "" {
		return 0, ErrEmptyMember
	}
	old := s.scores[member]
	next := old + delta
	if (delta > 0 && next < old) || (delta < 0 && next > old) {
		return old, ErrOverflow
	}
	return next, s.Add(member, next)
}

// Score returns member's score.
func (s *SortedSet) Score(member string) (int64, bool) {
	v, ok := s.scores[member]
	return v, ok
}

// Remove deletes member and reports whether it was present.
func (s *SortedSet) Remove(member string) bool {
	score, ok := s.scores[member]
	if !ok {
		return false
	}
	s.remove(member, score)
	delete(s.scores, member)
	return true
}

// Rank returns member's 0-based rank (0 = highest score), like ZREVRANK.
func (s *SortedSet) Rank(member string) (int, bool) {
	r, _, ok := s.RankSteps(member)
	return r, ok
}

// RankSteps is Rank, and also returns how many skip-list nodes the search
// examined. It exists to show the O(log n) cost next to a linear scan.
func (s *SortedSet) RankSteps(member string) (rank, steps int, ok bool) {
	score, ok := s.scores[member]
	if !ok {
		return 0, 0, false
	}
	x, r := s.head, 0
	for i := s.level - 1; i >= 0; i-- {
		for {
			fw := x.levels[i].forward
			if fw == nil {
				break
			}
			steps++
			if fw.member != member && !ahead(fw.score, fw.member, score, member) {
				break
			}
			r += x.levels[i].span
			x = fw
			if x.member == member {
				return r - 1, steps, true
			}
		}
	}
	return 0, steps, false // unreachable while the map and list agree
}

// Top returns the first n members, like ZREVRANGE key 0 n-1 WITHSCORES.
func (s *SortedSet) Top(n int) []Entry {
	if n <= 0 {
		return nil
	}
	return s.Range(0, n-1)
}

// Range returns the members with ranks start..stop inclusive, like
// ZREVRANGE. Negative indexes count from the end (-1 is the last member), and
// out-of-range bounds are clamped. An empty range returns nil.
func (s *SortedSet) Range(start, stop int) []Entry {
	n := s.length
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	start = max(start, 0)
	stop = min(stop, n-1)
	if start > stop || start >= n {
		return nil
	}
	out := make([]Entry, 0, stop-start+1)
	x := s.byRank(start + 1)
	for r := start; r <= stop && x != nil; r++ {
		out = append(out, Entry{Rank: r, Member: x.member, Score: x.score})
		x = x.levels[0].forward
	}
	return out
}

// Around returns member with up to n members ranked directly above it and up
// to n directly below, in rank order. Near the top or bottom of the board the
// window is cut short rather than shifted.
func (s *SortedSet) Around(member string, n int) ([]Entry, error) {
	if n < 0 {
		return nil, ErrNegativeN
	}
	r, ok := s.Rank(member)
	if !ok {
		return nil, ErrNotMember
	}
	lo := max(r-n, 0)
	hi := r + n
	if hi < r { // overflow for huge n
		hi = s.length - 1
	}
	return s.Range(lo, hi), nil
}

// byRank returns the node with 1-based rank r, walking spans down the levels.
func (s *SortedSet) byRank(r int) *node {
	x, traversed := s.head, 0
	for i := s.level - 1; i >= 0; i-- {
		for x.levels[i].forward != nil && traversed+x.levels[i].span <= r {
			traversed += x.levels[i].span
			x = x.levels[i].forward
		}
		if traversed == r {
			return x
		}
	}
	return nil
}

func (s *SortedSet) randomLevel() int {
	lvl := 1
	for lvl < s.maxLevel && s.rng.Float64() < s.p {
		lvl++
	}
	return lvl
}

// link inserts n at its sorted position. rank[i] is the 1-based rank of
// update[i], the last node at level i that stays ahead of n.
func (s *SortedSet) link(n *node) {
	member, score := n.member, n.score
	update, rank := s.update, s.rank
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		if i == s.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for fw := x.levels[i].forward; fw != nil && ahead(fw.score, fw.member, score, member); fw = x.levels[i].forward {
			rank[i] += x.levels[i].span
			x = fw
		}
		update[i] = x
	}
	lvl := len(n.levels)
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			rank[i] = 0
			update[i] = s.head
			s.head.levels[i].span = s.length
		}
		s.level = lvl
	}
	for i := range lvl {
		n.levels[i].forward = update[i].levels[i].forward
		update[i].levels[i].forward = n
		// update[i] used to jump span nodes; the new node sits
		// (rank[0]-rank[i]+1) nodes after it.
		n.levels[i].span = update[i].levels[i].span - (rank[0] - rank[i])
		update[i].levels[i].span = rank[0] - rank[i] + 1
	}
	// Links above the new node's height now jump over one more node.
	for i := lvl; i < s.level; i++ {
		update[i].levels[i].span++
	}
	s.length++
}

// remove unlinks and returns the node for member, which must be present.
func (s *SortedSet) remove(member string, score int64) *node {
	update := s.update
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for fw := x.levels[i].forward; fw != nil && ahead(fw.score, fw.member, score, member); fw = x.levels[i].forward {
			x = fw
		}
		update[i] = x
	}
	x = x.levels[0].forward
	if x == nil || x.member != member {
		panic("leaderboard: skip list and score map disagree")
	}
	for i := 0; i < s.level; i++ {
		if update[i].levels[i].forward == x {
			update[i].levels[i].span += x.levels[i].span - 1
			update[i].levels[i].forward = x.levels[i].forward
		} else {
			update[i].levels[i].span--
		}
	}
	for s.level > 1 && s.head.levels[s.level-1].forward == nil {
		s.level--
	}
	s.length--
	for i := range x.levels {
		x.levels[i] = level{}
	}
	return x
}
