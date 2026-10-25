// Package fanoutfeed builds home timelines three ways:
//
//   - Push (fan-out-on-write): a post is copied into every follower's inbox.
//     Reads are cheap. Writes cost O(followers).
//   - Pull (fan-out-on-read): a post goes only into the author's outbox.
//     Reads merge the outboxes of everyone you follow.
//   - Hybrid: push for normal users, pull for "celebrities" whose follower
//     count is at or above a threshold, so one post by a celebrity doesn't
//     trigger millions of inbox writes.
//
// The Stats counters make each strategy's cost visible.
package fanoutfeed

import (
	"sort"
	"sync"
	"time"
)

// Strategy selects how posts reach timelines.
type Strategy int

// Strategies.
const (
	Push Strategy = iota
	Pull
	Hybrid
)

func (s Strategy) String() string {
	switch s {
	case Push:
		return "push"
	case Pull:
		return "pull"
	case Hybrid:
		return "hybrid"
	}
	return "unknown"
}

// Post is one item in a feed. IDs increase monotonically.
type Post struct {
	ID        int64
	Author    string
	Text      string
	CreatedAt time.Time
}

// Stats counts the work done, which is how the strategies differ.
type Stats struct {
	InboxWrites  int // posts copied into follower inboxes
	OutboxReads  int // author outboxes scanned at read time
	PostsWritten int
}

// Feed is an in-memory social graph and post store. Safe for concurrent use.
type Feed struct {
	strategy  Strategy
	threshold int // Hybrid: authors with >= threshold followers are pulled
	inboxCap  int
	now       func() time.Time

	mu        sync.RWMutex
	nextID    int64
	followers map[string]map[string]bool // author -> followers
	following map[string]map[string]bool // user -> authors
	outbox    map[string][]Post          // author -> own posts, oldest first
	inbox     map[string][]Post          // user -> pushed posts, oldest first
	stats     Stats
}

// New creates a feed. celebrityThreshold only matters for Hybrid. inboxCap
// bounds each pushed inbox to between inboxCap and 2*inboxCap entries (0
// means 1000), as real systems keep only the newest N timeline entries in
// cache. Timelines deeper than inboxCap are not guaranteed for pushed posts. A nil now uses time.Now.
func New(s Strategy, celebrityThreshold, inboxCap int, now func() time.Time) *Feed {
	if inboxCap <= 0 {
		inboxCap = 1000
	}
	if now == nil {
		now = time.Now
	}
	return &Feed{
		strategy: s, threshold: celebrityThreshold, inboxCap: inboxCap, now: now,
		followers: map[string]map[string]bool{}, following: map[string]map[string]bool{},
		outbox: map[string][]Post{}, inbox: map[string][]Post{},
	}
}

// Follow makes user follow author. Push mode does not backfill old posts,
// so a new follower sees the author's posts from now on, as on most real
// feeds.
func (f *Feed) Follow(user, author string) {
	if user == author || user == "" || author == "" {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	addEdge(f.followers, author, user)
	addEdge(f.following, user, author)
}

// Unfollow removes the edge. Already-pushed posts are filtered out at read
// time, not deleted from the inbox.
func (f *Feed) Unfollow(user, author string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.followers[author], user)
	delete(f.following[user], author)
}

func addEdge(m map[string]map[string]bool, from, to string) {
	if m[from] == nil {
		m[from] = map[string]bool{}
	}
	m[from][to] = true
}

// pulled reports whether author's posts are fetched at read time rather
// than pushed. Caller holds f.mu.
func (f *Feed) pulled(author string) bool {
	switch f.strategy {
	case Pull:
		return true
	case Hybrid:
		return len(f.followers[author]) >= f.threshold
	}
	return false
}

// Publish stores a post and fans it out according to the strategy.
func (f *Feed) Publish(author, text string) Post {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	p := Post{ID: f.nextID, Author: author, Text: text, CreatedAt: f.now()}
	f.outbox[author] = append(f.outbox[author], p)
	f.stats.PostsWritten++
	if !f.pulled(author) {
		for follower := range f.followers[author] {
			box := append(f.inbox[follower], p)
			if len(box) > 2*f.inboxCap { // trim in batches so each push is amortised O(1)
				box = append([]Post(nil), box[len(box)-f.inboxCap:]...)
			}
			f.inbox[follower] = box
			f.stats.InboxWrites++
		}
	}
	return p
}

// Timeline returns up to limit posts from authors user follows, newest
// first.
func (f *Feed) Timeline(user string, limit int) []Post {
	if limit <= 0 {
		return nil
	}
	// Writes to Stats need the write lock. Timeline is still cheap for push.
	f.mu.Lock()
	defer f.mu.Unlock()
	follows := f.following[user]
	seen := map[int64]bool{}
	var out []Post
	add := func(p Post) {
		if follows[p.Author] && !seen[p.ID] {
			seen[p.ID] = true
			out = append(out, p)
		}
	}
	// Each source is sorted oldest-first, so its newest `limit` entries are
	// enough. Anything older can't make the merged top `limit`.
	box := f.inbox[user]
	for i, kept := len(box)-1, 0; i >= 0 && kept < limit; i-- {
		if follows[box[i].Author] {
			add(box[i])
			kept++
		}
	}
	for author := range follows {
		// A post pushed before an author became a celebrity can be in both
		// the inbox and the outbox. The seen set dedups it.
		if !f.pulled(author) {
			continue
		}
		f.stats.OutboxReads++
		posts := f.outbox[author]
		for i := len(posts) - 1; i >= 0 && i >= len(posts)-limit; i-- {
			add(posts[i])
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID > out[j].ID })
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

// Stats returns the work counters.
func (f *Feed) Stats() Stats {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.stats
}
