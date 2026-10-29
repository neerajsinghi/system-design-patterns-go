package materializedview

// Denormalized feed rows: why there is no JOIN on the read path.
//
// SocialDB is a normalized model: users, posts, follows and likes, each in
// its own table. Building a home feed from it means joining all four: find
// who the viewer follows, read their posts, then for every post look up the
// author row and count the like rows. FeedView is the alternative: a
// materialized view with one row per (viewer, post) that already embeds the
// author's name and avatar and the like count, maintained from SocialDB's
// change stream. A feed read becomes one partition range scan. The cost moves
// to the write path: a new post is copied into every follower's partition,
// and an author rename rewrites every row that embedded the old name.
//
// Both paths report a Cost (index lookups and rows touched) instead of a
// timing, so the trade-off is visible as numbers that do not depend on the
// machine.

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"sync"
)

// ErrInvalid is returned for changes that make no sense (e.g. self-follow).
var ErrInvalid = errors.New("materializedview: invalid change")

// User is a row in the users table.
type User struct {
	ID     string
	Name   string
	Avatar string
}

// Post is a row in the posts table. Seq is its creation order (newer posts
// have a larger Seq) and is assigned by SocialDB.
type Post struct {
	ID       string
	AuthorID string
	Text     string
	Seq      uint64
}

// SocialOp is the kind of a SocialChange.
type SocialOp int

// Social change operations.
const (
	UserAdded SocialOp = iota + 1
	UserUpdated
	PostCreated
	PostDeleted
	Followed
	Unfollowed
	Liked
	Unliked
)

// SocialChange is one event on SocialDB's change stream. User is set for
// UserAdded/UserUpdated (the after image), Post for PostCreated/PostDeleted,
// Follower and Followee for Followed/Unfollowed, and UserID and PostID for
// Liked/Unliked.
type SocialChange struct {
	LSN      uint64
	Op       SocialOp
	User     *User
	Post     *Post
	Follower string
	Followee string
	UserID   string
	PostID   string
}

// Cost counts the work an operation does: Lookups are index probes (point
// lookups or the start of a range scan) and Rows are rows read or written.
type Cost struct {
	Lookups int
	Rows    int
}

// Add returns c + o.
func (c Cost) Add(o Cost) Cost { return Cost{c.Lookups + o.Lookups, c.Rows + o.Rows} }

// FeedItem is one entry of a home feed, as either read path returns it.
type FeedItem struct {
	PostID       string
	AuthorID     string
	AuthorName   string
	AuthorAvatar string
	Text         string
	Seq          uint64
	Likes        int
}

type set map[string]struct{}

func (s set) add(k string)      { s[k] = struct{}{} }
func (s set) has(k string) bool { _, ok := s[k]; return ok }

func setOf(m map[string]set, k string) set {
	s := m[k]
	if s == nil {
		s = make(set)
		m[k] = s
	}
	return s
}

// SocialDB is the normalized source of truth.
type SocialDB struct {
	mu            sync.RWMutex
	users         map[string]User
	posts         map[string]Post
	postsByAuthor map[string]set // author -> post IDs (index on posts.author_id)
	following     map[string]set // follower -> followees (index on follows.follower)
	likes         map[string]set // post -> likers (index on likes.post_id)
	log           []SocialChange
}

// NewSocialDB returns an empty database.
func NewSocialDB() *SocialDB {
	return &SocialDB{
		users:         make(map[string]User),
		posts:         make(map[string]Post),
		postsByAuthor: make(map[string]set),
		following:     make(map[string]set),
		likes:         make(map[string]set),
	}
}

func (db *SocialDB) emit(c SocialChange) {
	c.LSN = uint64(len(db.log)) + 1
	db.log = append(db.log, c)
}

func (db *SocialDB) needUser(id string) error {
	if _, ok := db.users[id]; !ok {
		return fmt.Errorf("%w: user %s", ErrNotFound, id)
	}
	return nil
}

// AddUser inserts a user.
func (db *SocialDB) AddUser(u User) error {
	if u.ID == "" {
		return fmt.Errorf("%w: empty user id", ErrInvalid)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, ok := db.users[u.ID]; ok {
		return fmt.Errorf("%w: user %s", ErrExists, u.ID)
	}
	db.users[u.ID] = u
	db.emit(SocialChange{Op: UserAdded, User: &u})
	return nil
}

// UpdateUser changes a user's profile (name, avatar) through fn.
func (db *SocialDB) UpdateUser(id string, fn func(*User)) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	u, ok := db.users[id]
	if !ok {
		return fmt.Errorf("%w: user %s", ErrNotFound, id)
	}
	if fn != nil {
		fn(&u)
	}
	u.ID = id
	db.users[id] = u
	db.emit(SocialChange{Op: UserUpdated, User: &u})
	return nil
}

// CreatePost inserts a post by author and returns it with its Seq.
func (db *SocialDB) CreatePost(id, author, text string) (Post, error) {
	if id == "" {
		return Post{}, fmt.Errorf("%w: empty post id", ErrInvalid)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.needUser(author); err != nil {
		return Post{}, err
	}
	if _, ok := db.posts[id]; ok {
		return Post{}, fmt.Errorf("%w: post %s", ErrExists, id)
	}
	p := Post{ID: id, AuthorID: author, Text: text, Seq: uint64(len(db.log)) + 1}
	db.posts[id] = p
	setOf(db.postsByAuthor, author).add(id)
	db.emit(SocialChange{Op: PostCreated, Post: &p})
	return p, nil
}

// DeletePost removes a post and its likes.
func (db *SocialDB) DeletePost(id string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	p, ok := db.posts[id]
	if !ok {
		return fmt.Errorf("%w: post %s", ErrNotFound, id)
	}
	delete(db.posts, id)
	delete(db.postsByAuthor[p.AuthorID], id)
	delete(db.likes, id)
	db.emit(SocialChange{Op: PostDeleted, Post: &p})
	return nil
}

// Follow makes follower follow followee.
func (db *SocialDB) Follow(follower, followee string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if follower == followee {
		return fmt.Errorf("%w: self-follow %s", ErrInvalid, follower)
	}
	if err := cmp.Or(db.needUser(follower), db.needUser(followee)); err != nil {
		return err
	}
	s := setOf(db.following, follower)
	if s.has(followee) {
		return fmt.Errorf("%w: %s already follows %s", ErrExists, follower, followee)
	}
	s.add(followee)
	db.emit(SocialChange{Op: Followed, Follower: follower, Followee: followee})
	return nil
}

// Unfollow removes a follow edge.
func (db *SocialDB) Unfollow(follower, followee string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if !db.following[follower].has(followee) {
		return fmt.Errorf("%w: %s does not follow %s", ErrNotFound, follower, followee)
	}
	delete(db.following[follower], followee)
	db.emit(SocialChange{Op: Unfollowed, Follower: follower, Followee: followee})
	return nil
}

// Like records that user liked post.
func (db *SocialDB) Like(user, post string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.needUser(user); err != nil {
		return err
	}
	if _, ok := db.posts[post]; !ok {
		return fmt.Errorf("%w: post %s", ErrNotFound, post)
	}
	s := setOf(db.likes, post)
	if s.has(user) {
		return fmt.Errorf("%w: %s already likes %s", ErrExists, user, post)
	}
	s.add(user)
	db.emit(SocialChange{Op: Liked, UserID: user, PostID: post})
	return nil
}

// Unlike removes a like.
func (db *SocialDB) Unlike(user, post string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if !db.likes[post].has(user) {
		return fmt.Errorf("%w: %s does not like %s", ErrNotFound, user, post)
	}
	delete(db.likes[post], user)
	db.emit(SocialChange{Op: Unliked, UserID: user, PostID: post})
	return nil
}

// ChangesSince returns changes with LSN > lsn.
func (db *SocialDB) ChangesSince(lsn uint64) []SocialChange {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if lsn >= uint64(len(db.log)) {
		return nil
	}
	return slices.Clone(db.log[lsn:])
}

func newestFirst(a, b FeedItem) int { return cmp.Compare(b.Seq, a.Seq) }

// HomeFeedJoin builds viewer's newest-first home feed (at most limit items)
// from the normalized tables, the way a SQL query would:
//
//	SELECT p.*, u.name, u.avatar, (SELECT COUNT(*) FROM likes l WHERE l.post_id = p.id)
//	FROM follows f JOIN posts p ON p.author_id = f.followee JOIN users u ON u.id = p.author_id
//	WHERE f.follower = ? ORDER BY p.seq DESC LIMIT ?
//
// It counts one lookup for the follows index, one per followee for the posts
// index, and, for every post kept, one lookup on users (1 row) and one on
// likes (a row per like, which is what COUNT(*) has to read).
func (db *SocialDB) HomeFeedJoin(viewer string, limit int) ([]FeedItem, Cost) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var c Cost
	c.Lookups++ // follows WHERE follower = viewer
	followees := db.following[viewer]
	c.Rows += len(followees)
	var items []FeedItem
	for f := range followees {
		c.Lookups++ // posts WHERE author_id = f
		for id := range db.postsByAuthor[f] {
			c.Rows++
			p := db.posts[id]
			items = append(items, FeedItem{PostID: p.ID, AuthorID: p.AuthorID, Text: p.Text, Seq: p.Seq})
		}
	}
	slices.SortFunc(items, newestFirst)
	if limit >= 0 && len(items) > limit {
		items = items[:limit]
	}
	for i := range items {
		c.Lookups++ // users WHERE id = author
		c.Rows++
		u := db.users[items[i].AuthorID]
		items[i].AuthorName, items[i].AuthorAvatar = u.Name, u.Avatar
		c.Lookups++ // likes WHERE post_id = post
		n := len(db.likes[items[i].PostID])
		c.Rows += n
		items[i].Likes = n
	}
	return items, c
}

// LikeMode chooses whether the volatile like count is copied into every
// feed row or kept in one counter row per post that the read looks up.
type LikeMode int

const (
	// EmbedLikes copies the like count into every feed row of the post:
	// reads need no extra lookup, but one like rewrites a row per follower.
	EmbedLikes LikeMode = iota
	// ReferenceLikes keeps one counter row per post: a like writes one row,
	// and every feed read does one point lookup per returned row.
	ReferenceLikes
)

// FeedOption configures a FeedView.
type FeedOption func(*FeedView)

// WithLikeMode selects how like counts are stored (default EmbedLikes).
func WithLikeMode(m LikeMode) FeedOption { return func(v *FeedView) { v.likeMode = m } }

// FeedRow is one denormalized row of the view: partition key Viewer,
// clustering key Seq (descending). Author fields and, in EmbedLikes mode,
// Likes are copies.
type FeedRow struct {
	Viewer string
	FeedItem
}

// FeedView is the denormalized home-feed view, maintained from SocialDB's
// change stream. Besides the feed partitions it keeps the small amount of
// state its maintainer needs to route changes (each author's followers and
// posts), the same way a stream processor keeps local state.
type FeedView struct {
	mu       sync.RWMutex
	lsn      uint64
	likeMode LikeMode

	feeds     map[string][]*FeedRow          // viewer -> rows, newest first
	byPost    map[string]map[string]*FeedRow // post -> viewer -> row (secondary index)
	users     map[string]User
	posts     map[string]Post
	byAuthor  map[string]set // author -> post IDs
	followers map[string]set // author -> followers
	likes     map[string]int // post -> like count (the counter rows)

	writes Cost // cumulative maintenance cost
}

// NewFeedView returns an empty view at LSN 0.
func NewFeedView(opts ...FeedOption) (*FeedView, error) {
	v := &FeedView{
		feeds:     make(map[string][]*FeedRow),
		byPost:    make(map[string]map[string]*FeedRow),
		users:     make(map[string]User),
		posts:     make(map[string]Post),
		byAuthor:  make(map[string]set),
		followers: make(map[string]set),
		likes:     make(map[string]int),
	}
	for _, o := range opts {
		o(v)
	}
	if v.likeMode != EmbedLikes && v.likeMode != ReferenceLikes {
		return nil, fmt.Errorf("%w: unknown like mode %d", ErrInvalid, v.likeMode)
	}
	return v, nil
}

func (v *FeedView) row(viewer string, p Post) *FeedRow {
	u := v.users[p.AuthorID]
	r := &FeedRow{Viewer: viewer, FeedItem: FeedItem{
		PostID: p.ID, AuthorID: p.AuthorID, AuthorName: u.Name, AuthorAvatar: u.Avatar, Text: p.Text, Seq: p.Seq,
	}}
	if v.likeMode == EmbedLikes {
		r.Likes = v.likes[p.ID]
	}
	return r
}

func (v *FeedView) insertRow(r *FeedRow) {
	rows := v.feeds[r.Viewer]
	i, _ := slices.BinarySearchFunc(rows, r.Seq, func(x *FeedRow, seq uint64) int { return cmp.Compare(seq, x.Seq) })
	v.feeds[r.Viewer] = slices.Insert(rows, i, r)
	idx := v.byPost[r.PostID]
	if idx == nil {
		idx = make(map[string]*FeedRow)
		v.byPost[r.PostID] = idx
	}
	idx[r.Viewer] = r
}

func (v *FeedView) removeRow(viewer, postID string) {
	rows := v.feeds[viewer]
	if i := slices.IndexFunc(rows, func(x *FeedRow) bool { return x.PostID == postID }); i >= 0 {
		v.feeds[viewer] = slices.Delete(rows, i, i+1)
	}
	delete(v.byPost[postID], viewer)
	if len(v.byPost[postID]) == 0 {
		delete(v.byPost, postID)
	}
}

// forRows calls fn on every feed row of post p (one per follower of its
// author) and returns how many rows it touched.
func (v *FeedView) forRows(p Post, fn func(*FeedRow)) int {
	for _, r := range v.byPost[p.ID] {
		fn(r)
	}
	return len(v.byPost[p.ID])
}

// Apply folds one change into the view and returns the view rows it wrote.
// Like View.Apply, duplicates are skipped and gaps return ErrGap.
func (v *FeedView) Apply(c SocialChange) (Cost, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if c.LSN <= v.lsn {
		return Cost{}, nil
	}
	if c.LSN != v.lsn+1 {
		return Cost{}, fmt.Errorf("%w: at %d, got %d", ErrGap, v.lsn, c.LSN)
	}
	var cost Cost
	switch c.Op {
	case UserAdded:
		v.users[c.User.ID] = *c.User
	case UserUpdated:
		// The fan-out: every embedded copy of the profile is rewritten, one
		// row per (follower, post by this author).
		u := *c.User
		v.users[u.ID] = u
		for id := range v.byAuthor[u.ID] {
			cost.Lookups++
			cost.Rows += v.forRows(v.posts[id], func(r *FeedRow) { r.AuthorName, r.AuthorAvatar = u.Name, u.Avatar })
		}
	case PostCreated:
		p := *c.Post
		v.posts[p.ID] = p
		setOf(v.byAuthor, p.AuthorID).add(p.ID)
		for f := range v.followers[p.AuthorID] {
			cost.Lookups++
			cost.Rows++
			v.insertRow(v.row(f, p))
		}
	case PostDeleted:
		p := *c.Post
		for f := range v.followers[p.AuthorID] {
			cost.Lookups++
			cost.Rows++
			v.removeRow(f, p.ID)
		}
		delete(v.posts, p.ID)
		delete(v.byAuthor[p.AuthorID], p.ID)
		delete(v.likes, p.ID)
	case Followed:
		setOf(v.followers, c.Followee).add(c.Follower)
		cost.Lookups++ // backfill the followee's posts into the new follower's partition
		for id := range v.byAuthor[c.Followee] {
			cost.Rows++
			v.insertRow(v.row(c.Follower, v.posts[id]))
		}
	case Unfollowed:
		delete(v.followers[c.Followee], c.Follower)
		cost.Lookups++
		for id := range v.byAuthor[c.Followee] {
			cost.Rows++
			v.removeRow(c.Follower, id)
		}
	case Liked, Unliked:
		d := 1
		if c.Op == Unliked {
			d = -1
		}
		v.likes[c.PostID] += d
		n := v.likes[c.PostID]
		if v.likeMode == ReferenceLikes {
			cost = Cost{Lookups: 1, Rows: 1} // one counter row
		} else {
			p := v.posts[c.PostID]
			cost.Lookups = len(v.followers[p.AuthorID])
			cost.Rows = v.forRows(p, func(r *FeedRow) { r.Likes = n })
		}
	default:
		return Cost{}, fmt.Errorf("%w: unknown op %d", ErrInvalid, c.Op)
	}
	v.lsn = c.LSN
	v.writes = v.writes.Add(cost)
	return cost, nil
}

// CatchUp applies every change from db since the view's LSN and returns the
// write cost of the changes it applied.
func (v *FeedView) CatchUp(db *SocialDB) (Cost, error) {
	var total Cost
	for _, c := range db.ChangesSince(v.LSN()) {
		cost, err := v.Apply(c)
		if err != nil {
			return total, err
		}
		total = total.Add(cost)
	}
	return total, nil
}

// LSN is the last change applied.
func (v *FeedView) LSN() uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.lsn
}

// WriteCost is the cumulative maintenance cost of every change applied.
func (v *FeedView) WriteCost() Cost {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.writes
}

// HomeFeed reads viewer's newest-first feed (at most limit items) from the
// view: one lookup to find the partition and one row per item returned. In
// ReferenceLikes mode each row also costs a point lookup on the post's
// counter row. There is no join.
func (v *FeedView) HomeFeed(viewer string, limit int) ([]FeedItem, Cost) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	rows := v.feeds[viewer]
	if limit >= 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	c := Cost{Lookups: 1, Rows: len(rows)}
	out := make([]FeedItem, len(rows))
	for i, r := range rows {
		out[i] = r.FeedItem
		if v.likeMode == ReferenceLikes {
			c.Lookups++
			c.Rows++
			out[i].Likes = v.likes[r.PostID]
		}
	}
	return out, c
}

// Rows returns the total number of feed rows stored (the storage cost of
// copying every post into every follower's partition).
func (v *FeedView) Rows() int {
	v.mu.RLock()
	defer v.mu.RUnlock()
	n := 0
	for _, rows := range v.feeds {
		n += len(rows)
	}
	return n
}
