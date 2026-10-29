// Package joinfreefeed simulates the "home feed JOIN melts the database"
// incident. A photo-sharing app builds every home feed with one query that
// joins follows, posts, users and likes, sorts by time and keeps 20 rows. At
// launch each user follows a few accounts and the query is cheap. As people
// follow hundreds of accounts and posts collect hundreds of likes, every page
// view reads thousands of rows, and the evening peak pushes the database past
// what it can serve: the queue grows, feed latency goes to seconds and views
// time out.
//
// Broken runs the JOIN (materializedview.SocialDB.HomeFeedJoin) on every feed
// view. Fixed separates writes from reads (CQRS): writes still go to the
// normalized tables, a projector consumes their change stream and maintains a
// denormalized feed view (materializedview.FeedView) with one row per
// (viewer, post), so a feed read is one partition range scan. Like counts are
// referenced, not embedded (ReferenceLikes), so a like writes one counter row
// instead of a row per follower. Celebrities are not fanned out on write: each
// gets one "outbox" partition, and a reader merges the outboxes of the
// celebrities they follow at read time, the hybrid strategy of the fan-out
// feed pattern.
//
// Cost is counted, not timed: every operation reports index lookups and rows
// touched (materializedview.Cost). One work unit is one lookup or one row,
// and the database serves DBCapacity units per second. A simple FIFO fluid
// queue turns that into latency: a request waits for the work queued ahead of
// it, and one that would wait longer than Timeout is counted as timed out.
package joinfreefeed

import (
	"cmp"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	mv "github.com/neerajsinghi/system-design-patterns-go/patterns/data/materialized-view"
)

// The seeded social graph (scaled down so a run takes a fraction of a second).
const (
	RegularUsers    = 400
	Celebrities     = 3
	FollowsPerUser  = 80   // regular accounts each regular user follows
	CelebFollowRate = 0.80 // chance a regular user follows each celebrity
	PostsPerUser    = 5
	CelebPosts      = 20
	// CelebrityThreshold: authors with more followers than this are pulled at
	// read time instead of being fanned out on write.
	CelebrityThreshold = 200
)

// Traffic and capacity.
const (
	PageSize    = 20
	MaxFeedWork = 300 // invariant: work units (lookups + rows) per feed view
	// DBCapacity is how many work units (an index lookup, a row read or a row
	// written) the database cluster serves per second.
	DBCapacity = 60_000
	Timeout    = time.Second

	Duration    = 60 // simulated seconds
	PeakFrom    = 20
	PeakTo      = 40 // the evening peak is [PeakFrom, PeakTo)
	BaseViews   = 10 // feed views per second off-peak
	PeakViews   = 30 // feed views per second at peak
	PostsPerSec = 1
	LikesPerSec = 40 // on the RecentPosts newest posts
	RecentPosts = 100

	CelebPostAt   = 25 // a celebrity posts at peak
	RenameAt      = 30 // a regular user renames themselves
	CelebRenameAt = 35 // a celebrity renames themselves
)

// WriteAmp is rows written per write, by kind of write: the normalized row
// plus every view row it caused.
type WriteAmp struct {
	Post, CelebPost, Rename, CelebRename, Like float64
}

// Report is what a run measured.
type Report struct {
	Name string
	// ViewWork is the work units each feed view cost, in arrival order.
	ViewWork []int
	// Latency is each feed view's latency under the queueing model.
	Latency []time.Duration
	// Load[s] is the work units sent to the database in second s.
	Load [Duration]int
	// TimedOut[s] counts feed views in second s that waited past Timeout.
	TimedOut [Duration]int
	Writes   WriteAmp
	// StaleViews counts feed views whose posts or author names differed from
	// what the JOIN would have returned at that instant (the view had not
	// caught up yet). StaleLikeViews counts views that had the right posts
	// but at least one like count that was behind.
	StaleViews, StaleLikeViews int
	// Mismatches counts users whose feed differs from the JOIN after the view
	// has caught up at the end of the run. Must be 0.
	Mismatches int
	// MaxLagChanges is the most changes the projector applied in one
	// catch-up; the view is at most one second behind the tables.
	MaxLagChanges int
	// FeedRows is how many denormalized rows the view stores.
	FeedRows int
}

// TotalTimedOut sums TimedOut.
func (r Report) TotalTimedOut() int {
	n := 0
	for _, t := range r.TimedOut {
		n += t
	}
	return n
}

// PeakUtilisation is the busiest second's load as a fraction of DBCapacity.
func (r Report) PeakUtilisation() float64 {
	p := 0
	for _, l := range r.Load {
		p = max(p, l)
	}
	return float64(p) / DBCapacity
}

// WorkP50 and WorkMax summarise ViewWork.
func (r Report) WorkP50() int { return pct(r.ViewWork, 0.50) }

// WorkMax is the most expensive feed view.
func (r Report) WorkMax() int { return pct(r.ViewWork, 1) }

// LatencyP is the p-quantile of feed view latency.
func (r Report) LatencyP(p float64) time.Duration { return pct(r.Latency, p) }

func pct[T cmp.Ordered](xs []T, p float64) T {
	var zero T
	if len(xs) == 0 {
		return zero
	}
	s := slices.Clone(xs)
	slices.Sort(s)
	return s[min(len(s)-1, int(p*float64(len(s))))]
}

// Print writes a timeline and a summary.
func (r Report) Print(w io.Writer) {
	fmt.Fprintf(w, "%s\n", r.Name)
	fmt.Fprintf(w, "%d users (+%d celebrities), each following %d accounts; DB capacity %d work units/s; timeout %s\n",
		RegularUsers, Celebrities, FollowsPerUser, DBCapacity, Timeout)
	fmt.Fprintf(w, "feed views/s: %d off-peak, %d at peak (t=%d-%ds)\n\n", BaseViews, PeakViews, PeakFrom, PeakTo-1)
	fmt.Fprintln(w, "  window      DB load                 timed-out views")
	for start := 0; start < Duration; start += 5 {
		load, out := 0, 0
		for s := start; s < start+5; s++ {
			load += r.Load[s]
			out += r.TimedOut[s]
		}
		u := 100 * float64(load) / float64(5*DBCapacity)
		note := ""
		if start >= PeakFrom && start < PeakTo {
			note = "  (peak)"
		}
		fmt.Fprintf(w, "  t=%2d-%2ds  %5.0f%% %-15s  %4d%s\n", start, start+4, u, strings.Repeat("█", min(15, int(u/10))), out, note)
	}
	fmt.Fprintf(w, "\n  work per feed view: p50 %d, max %d (invariant ≤ %d)\n", r.WorkP50(), r.WorkMax(), MaxFeedWork)
	fmt.Fprintf(w, "  peak DB utilisation: %.0f%%\n", 100*r.PeakUtilisation())
	fmt.Fprintf(w, "  feed latency: p50 %s, p99 %s; timed-out views: %d of %d\n",
		r.LatencyP(0.50).Round(time.Millisecond), r.LatencyP(0.99).Round(time.Millisecond), r.TotalTimedOut(), len(r.ViewWork))
	fmt.Fprintf(w, "  rows written per post %.0f, celebrity post %.0f, rename %.0f, celebrity rename %.0f, like %.0f\n",
		r.Writes.Post, r.Writes.CelebPost, r.Writes.Rename, r.Writes.CelebRename, r.Writes.Like)
	if r.FeedRows > 0 {
		fmt.Fprintf(w, "  view: %d stored feed rows; ≤1s behind the tables (max %d changes per catch-up)\n", r.FeedRows, r.MaxLagChanges)
		fmt.Fprintf(w, "  views that differed from the JOIN at read time: %d missing a post or showing an old name, %d with a like count behind\n",
			r.StaleViews, r.StaleLikeViews)
		fmt.Fprintf(w, "  feeds that differ from the JOIN after catch-up: %d of %d\n", r.Mismatches, RegularUsers)
	}
}

func userID(i int) string  { return fmt.Sprintf("u%03d", i) }
func celebID(i int) string { return fmt.Sprintf("c%d", i) }

// seed builds the identical social graph both designs start from and
// returns every post ID.
func seed(db *mv.SocialDB) []string {
	r := rand.New(rand.NewPCG(1, 2))
	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	for i := range RegularUsers {
		must(db.AddUser(mv.User{ID: userID(i), Name: "user " + userID(i), Avatar: userID(i) + ".jpg"}))
	}
	for i := range Celebrities {
		must(db.AddUser(mv.User{ID: celebID(i), Name: "star " + celebID(i), Avatar: celebID(i) + ".jpg"}))
	}
	for i := range RegularUsers {
		n := 0
		for _, j := range r.Perm(RegularUsers) {
			if j == i {
				continue
			}
			must(db.Follow(userID(i), userID(j)))
			if n++; n == FollowsPerUser {
				break
			}
		}
		for c := range Celebrities {
			if r.Float64() < CelebFollowRate {
				must(db.Follow(userID(i), celebID(c)))
			}
		}
	}
	// Posts in rounds, so celebrity and regular posts interleave in time.
	var posts []string
	for round := range CelebPosts {
		var authors []string
		for c := range Celebrities {
			authors = append(authors, celebID(c))
		}
		if round%(CelebPosts/PostsPerUser) == 0 {
			for i := range RegularUsers {
				authors = append(authors, userID(i))
			}
		}
		r.Shuffle(len(authors), func(a, b int) { authors[a], authors[b] = authors[b], authors[a] })
		for _, a := range authors {
			id := fmt.Sprintf("p%s-%d", a, round)
			_, err := db.CreatePost(id, a, "photo")
			must(err)
			posts = append(posts, id)
		}
	}
	// Likes: regular posts get 0-40, celebrity posts 200-350.
	all := RegularUsers + Celebrities
	for _, p := range posts {
		n := r.IntN(41)
		if p[1] == 'c' {
			n = 200 + r.IntN(151)
		}
		start := r.IntN(all)
		for k := range n {
			j := (start + k) % all
			liker := userID(j)
			if j >= RegularUsers {
				liker = celebID(j - RegularUsers)
			}
			must(db.Like(liker, p))
		}
	}
	return posts
}

type opKind int

const (
	opPost opKind = iota
	opCelebPost
	opRename
	opCelebRename
	opLike
)

type op struct {
	kind       opKind
	user, post string
}

// script is the identical traffic both designs receive.
type script struct {
	writes [Duration][]op
	views  [Duration][]string
}

func newScript(posts []string) *script {
	r := rand.New(rand.NewPCG(3, 4))
	s := &script{}
	posts = slices.Clone(posts)
	for sec := range Duration {
		for i := range PostsPerSec {
			id := fmt.Sprintf("n%d-%d", sec, i)
			s.writes[sec] = append(s.writes[sec], op{kind: opPost, user: userID(r.IntN(RegularUsers)), post: id})
			posts = append(posts, id)
		}
		switch sec {
		case CelebPostAt:
			id := fmt.Sprintf("n%d-celeb", sec)
			s.writes[sec] = append(s.writes[sec], op{kind: opCelebPost, user: celebID(0), post: id})
			posts = append(posts, id)
		case RenameAt:
			s.writes[sec] = append(s.writes[sec], op{kind: opRename, user: userID(r.IntN(RegularUsers))})
		case CelebRenameAt:
			s.writes[sec] = append(s.writes[sec], op{kind: opCelebRename, user: celebID(1)})
		}
		for range LikesPerSec {
			s.writes[sec] = append(s.writes[sec], op{kind: opLike, user: userID(r.IntN(RegularUsers)), post: posts[len(posts)-1-r.IntN(RecentPosts)]})
		}
		n := BaseViews
		if sec >= PeakFrom && sec < PeakTo {
			n = PeakViews
		}
		for range n {
			s.views[sec] = append(s.views[sec], userID(r.IntN(RegularUsers)))
		}
	}
	return s
}

// apply runs a write against the normalized tables. A failed write (a
// duplicate like) does nothing and costs nothing.
func apply(db *mv.SocialDB, o op) bool {
	var err error
	switch o.kind {
	case opPost, opCelebPost:
		_, err = db.CreatePost(o.post, o.user, "photo")
	case opRename, opCelebRename:
		err = db.UpdateUser(o.user, func(u *mv.User) { u.Name += " (renamed)" })
	case opLike:
		err = db.Like(o.user, o.post)
	}
	return err == nil
}

// normalizedWrite is the cost of one write to the normalized tables: find
// the row, write the row.
var normalizedWrite = mv.Cost{Lookups: 1, Rows: 1}

func work(c mv.Cost) int { return c.Lookups + c.Rows }

// queue is a FIFO fluid model of the database: it drains DBCapacity work
// units per second, and a request's latency is the work queued ahead of it
// plus its own, divided by capacity. A timed-out request still runs; the
// database does not know the client gave up.
type queue struct {
	backlog, t float64
}

func (q *queue) arrive(t float64, units int) time.Duration {
	q.backlog = max(0, q.backlog-(t-q.t)*DBCapacity)
	q.t = t
	q.backlog += float64(units)
	return time.Duration(q.backlog / DBCapacity * float64(time.Second))
}

// design is what differs between Broken and Fixed.
type design interface {
	write(o op) // after the write is applied to the tables
	read(viewer string) ([]mv.FeedItem, mv.Cost)
	endOfSecond() mv.Cost
}

type writeTally struct {
	rows  [5]int
	count [5]int
}

func (t *writeTally) add(k opKind, rows int) { t.rows[k] += rows }

func (t *writeTally) amp() WriteAmp {
	f := func(k opKind) float64 {
		if t.count[k] == 0 {
			return 0
		}
		return float64(t.rows[k]) / float64(t.count[k])
	}
	return WriteAmp{f(opPost), f(opCelebPost), f(opRename), f(opCelebRename), f(opLike)}
}

func run(r *Report, db *mv.SocialDB, sc *script, d design, tally *writeTally, checkStale bool) {
	q := &queue{}
	for sec := range Duration {
		t := float64(sec)
		for _, o := range sc.writes[sec] {
			if !apply(db, o) {
				continue
			}
			tally.count[o.kind]++
			tally.add(o.kind, normalizedWrite.Rows)
			d.write(o)
			r.Load[sec] += work(normalizedWrite)
			q.arrive(t, work(normalizedWrite))
		}
		views := sc.views[sec]
		for i, v := range views {
			items, c := d.read(v)
			u := work(c)
			r.ViewWork = append(r.ViewWork, u)
			r.Load[sec] += u
			lat := q.arrive(t+float64(i)/float64(len(views)), u)
			r.Latency = append(r.Latency, lat)
			if lat > Timeout {
				r.TimedOut[sec]++
			}
			if checkStale {
				want, _ := db.HomeFeedJoin(v, PageSize)
				switch {
				case !slices.EqualFunc(items, want, sameExceptLikes):
					r.StaleViews++
				case !slices.Equal(items, want):
					r.StaleLikeViews++
				}
			}
		}
		c := d.endOfSecond()
		r.Load[sec] += work(c)
		q.arrive(t+0.999, work(c))
	}
	r.Writes = tally.amp()
}

func sameExceptLikes(a, b mv.FeedItem) bool {
	a.Likes, b.Likes = 0, 0
	return a == b
}

type joinDesign struct{ db *mv.SocialDB }

func (joinDesign) write(op) {}
func (j joinDesign) read(v string) ([]mv.FeedItem, mv.Cost) {
	return j.db.HomeFeedJoin(v, PageSize)
}
func (joinDesign) endOfSecond() mv.Cost { return mv.Cost{} }

// Broken: every feed view runs the four-table JOIN.
func Broken() Report {
	r := Report{Name: "BROKEN: every feed view runs follows ⋈ posts ⋈ users ⋈ likes ORDER BY time LIMIT 20"}
	db := mv.NewSocialDB()
	sc := newScript(seed(db))
	run(&r, db, sc, joinDesign{db}, &writeTally{}, false)
	return r
}

// projector is the CQRS read side's maintainer. It consumes the normalized
// tables' change stream and feeds a FeedView, except for follow edges that
// point at a celebrity: those go into a small per-viewer pull list instead,
// so a celebrity's posts land only in the celebrity's own outbox partition.
// Because it drops some changes, it renumbers the ones it forwards so the
// view still sees a gap-free stream.
type projector struct {
	db       *mv.SocialDB
	view     *mv.FeedView
	pos, out uint64
	celeb    map[string]bool
	pulls    map[string][]string // viewer -> celebrities they follow
}

func outbox(celeb string) string { return "outbox:" + celeb }

func newProjector(db *mv.SocialDB, celebs []string) (*projector, error) {
	v, err := mv.NewFeedView(mv.WithLikeMode(mv.ReferenceLikes))
	if err != nil {
		return nil, err
	}
	p := &projector{db: db, view: v, celeb: map[string]bool{}, pulls: map[string][]string{}}
	for _, c := range celebs {
		p.celeb[c] = true
		// The outbox partition "follows" the celebrity, so each of their posts
		// is written once, with the author's profile embedded.
		if _, err := p.forward(mv.SocialChange{Op: mv.Followed, Follower: outbox(c), Followee: c}); err != nil {
			return nil, err
		}
	}
	return p, nil
}

func (p *projector) forward(c mv.SocialChange) (mv.Cost, error) {
	p.out++
	c.LSN = p.out
	return p.view.Apply(c)
}

// catchUp applies every change since the last catch-up, calling fn with each
// change and the view rows it cost. It returns how many changes it applied.
func (p *projector) catchUp(fn func(mv.SocialChange, mv.Cost)) (int, error) {
	changes := p.db.ChangesSince(p.pos)
	for _, c := range changes {
		var cost mv.Cost
		switch {
		case (c.Op == mv.Followed || c.Op == mv.Unfollowed) && p.celeb[c.Followee]:
			if c.Op == mv.Followed {
				p.pulls[c.Follower] = append(p.pulls[c.Follower], c.Followee)
			} else {
				p.pulls[c.Follower] = slices.DeleteFunc(p.pulls[c.Follower], func(s string) bool { return s == c.Followee })
			}
			cost = mv.Cost{Lookups: 1, Rows: 1}
		default:
			var err error
			if cost, err = p.forward(c); err != nil {
				return 0, err
			}
		}
		p.pos = c.LSN
		if fn != nil {
			fn(c, cost)
		}
	}
	return len(changes), nil
}

// homeFeed reads the viewer's partition and the outboxes of the celebrities
// they follow, and merges them newest first.
func (p *projector) homeFeed(viewer string) ([]mv.FeedItem, mv.Cost) {
	items, cost := p.view.HomeFeed(viewer, PageSize)
	for _, c := range p.pulls[viewer] {
		more, mc := p.view.HomeFeed(outbox(c), PageSize)
		items = append(items, more...)
		cost = cost.Add(mc)
	}
	slices.SortFunc(items, func(a, b mv.FeedItem) int { return cmp.Compare(b.Seq, a.Seq) })
	if len(items) > PageSize {
		items = items[:PageSize]
	}
	return items, cost
}

type cqrsDesign struct {
	p      *projector
	tally  *writeTally
	maxLag int
}

func (cqrsDesign) write(op) {}
func (d *cqrsDesign) read(v string) ([]mv.FeedItem, mv.Cost) {
	return d.p.homeFeed(v)
}

// endOfSecond is the projector's once-a-second catch-up; its view writes are
// database load too.
func (d *cqrsDesign) endOfSecond() mv.Cost {
	var total mv.Cost
	n, err := d.p.catchUp(func(c mv.SocialChange, cost mv.Cost) {
		total = total.Add(cost)
		if k, ok := d.kind(c); ok {
			d.tally.add(k, cost.Rows)
		}
	})
	if err != nil {
		panic(err)
	}
	d.maxLag = max(d.maxLag, n)
	return total
}

func (d *cqrsDesign) kind(c mv.SocialChange) (opKind, bool) {
	switch c.Op {
	case mv.PostCreated:
		if d.p.celeb[c.Post.AuthorID] {
			return opCelebPost, true
		}
		return opPost, true
	case mv.UserUpdated:
		if d.p.celeb[c.User.ID] {
			return opCelebRename, true
		}
		return opRename, true
	case mv.Liked:
		return opLike, true
	}
	return 0, false
}

func celebIDs() []string {
	var out []string
	for i := range Celebrities {
		out = append(out, celebID(i))
	}
	return out
}

// Fixed: CQRS with a denormalized feed view, referenced like counts and
// celebrity posts merged at read time. The view's initial build happens
// before the run (off-peak), so it is not counted as peak load.
func Fixed() Report {
	r := Report{Name: "FIXED: CQRS feed view (one range scan per read), referenced likes, celebrities merged at read"}
	db := mv.NewSocialDB()
	sc := newScript(seed(db))
	p, err := newProjector(db, celebIDs())
	if err != nil {
		panic(err)
	}
	if _, err := p.catchUp(nil); err != nil {
		panic(err)
	}
	d := &cqrsDesign{p: p, tally: &writeTally{}}
	run(&r, db, sc, d, d.tally, true)
	r.MaxLagChanges = d.maxLag
	for i := range RegularUsers {
		got, _ := p.homeFeed(userID(i))
		if want, _ := db.HomeFeedJoin(userID(i), PageSize); !slices.Equal(got, want) {
			r.Mismatches++
		}
	}
	r.FeedRows = p.view.Rows()
	return r
}
