package joinfreefeed

import (
	"slices"
	"sync"
	"testing"

	mv "github.com/neerajsinghi/system-design-patterns-go/patterns/data/materialized-view"
)

var (
	broken = sync.OnceValue(Broken)
	fixed  = sync.OnceValue(Fixed)
)

// The invariant: every feed view costs at most MaxFeedWork work units, the
// database stays within capacity at peak, no feed view times out, and once the
// view has caught up every feed equals what the JOIN returns.
func TestBrokenViolatesInvariant(t *testing.T) {
	r := broken()
	if r.WorkMax() <= MaxFeedWork || r.PeakUtilisation() <= 1 || r.TotalTimedOut() == 0 {
		t.Fatalf("broken run should overload the database: max work %d, peak utilisation %.2f, timed out %d",
			r.WorkMax(), r.PeakUtilisation(), r.TotalTimedOut())
	}
	// Off-peak, the JOIN is fine: that is why it shipped.
	for s := range PeakFrom {
		if r.TimedOut[s] != 0 {
			t.Fatalf("broken run timed out %d views off-peak at t=%ds", r.TimedOut[s], s)
		}
	}
}

func TestFixedHoldsInvariant(t *testing.T) {
	r := fixed()
	if r.WorkMax() > MaxFeedWork {
		t.Fatalf("fixed feed view cost %d work units, want ≤ %d", r.WorkMax(), MaxFeedWork)
	}
	if r.PeakUtilisation() > 1 || r.TotalTimedOut() != 0 {
		t.Fatalf("fixed run: peak utilisation %.2f, timed out %d", r.PeakUtilisation(), r.TotalTimedOut())
	}
	if r.Mismatches != 0 {
		t.Fatalf("%d feeds differ from the JOIN after catch-up", r.Mismatches)
	}
	if len(r.ViewWork) != len(broken().ViewWork) {
		t.Fatalf("traffic differs: %d views fixed, %d broken", len(r.ViewWork), len(broken().ViewWork))
	}
}

// The fix's cost is on the write path: a post is copied to every follower,
// a rename rewrites every embedded copy, but likes and celebrity posts stay
// cheap because of referenced likes and the celebrity outbox.
func TestFixedWriteAmplification(t *testing.T) {
	w := fixed().Writes
	if w.Post < FollowsPerUser/2 {
		t.Errorf("regular post wrote %.0f rows, want about one per follower (%d)", w.Post, FollowsPerUser)
	}
	if w.Rename < PostsPerUser*FollowsPerUser/2 {
		t.Errorf("rename wrote %.0f rows, want about posts × followers (%d)", w.Rename, PostsPerUser*FollowsPerUser)
	}
	if w.CelebPost > 2 || w.Like > 2 {
		t.Errorf("celebrity post wrote %.0f rows, like wrote %.0f, want ≤ 2 each", w.CelebPost, w.Like)
	}
	if w.CelebRename > CelebPosts+CelebPosts/2+1 {
		t.Errorf("celebrity rename wrote %.0f rows, want about one per post (%d)", w.CelebRename, CelebPosts)
	}
	if r := fixed(); r.StaleViews == 0 || r.StaleLikeViews == 0 {
		t.Error("expected some reads to observe the view's lag")
	}
}

// Without the hybrid split and with embedded likes, a celebrity post and a
// like on it would each write a row per follower.
func TestNaiveDenormalizationAmplifiesCelebrityWrites(t *testing.T) {
	db := mv.NewSocialDB()
	seed(db)
	v, err := mv.NewFeedView() // EmbedLikes, every author fanned out
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.CatchUp(db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreatePost("big", celebID(0), "photo"); err != nil {
		t.Fatal(err)
	}
	post, _ := v.CatchUp(db)
	if err := db.Like(userID(0), "big"); err != nil {
		t.Fatal(err)
	}
	like, _ := v.CatchUp(db)
	t.Logf("naive: celebrity post wrote %d rows, one like on it %d rows", post.Rows, like.Rows)
	if post.Rows <= CelebrityThreshold || like.Rows <= CelebrityThreshold {
		t.Fatalf("naive view: celebrity post %d rows, like %d rows; want > %d each", post.Rows, like.Rows, CelebrityThreshold)
	}
}

// Eventual consistency on a tiny graph: a new post is invisible until the
// projector catches up, then appears in followers' feeds; a celebrity's post
// reaches followers through the read-time merge; a rename rewrites rows.
func TestEventualConsistency(t *testing.T) {
	db := mv.NewSocialDB()
	for _, id := range []string{"ann", "bob", "cat", "star"} {
		if err := db.AddUser(mv.User{ID: id, Name: id}); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range [][2]string{{"ann", "bob"}, {"cat", "bob"}, {"ann", "star"}} {
		if err := db.Follow(f[0], f[1]); err != nil {
			t.Fatal(err)
		}
	}
	p, err := newProjector(db, []string{"star"})
	if err != nil {
		t.Fatal(err)
	}
	ids := func(items []mv.FeedItem) []string {
		var out []string
		for _, it := range items {
			out = append(out, it.PostID)
		}
		return out
	}
	mustCatchUp := func() mv.Cost {
		var total mv.Cost
		if _, err := p.catchUp(func(_ mv.SocialChange, c mv.Cost) { total = total.Add(c) }); err != nil {
			t.Fatal(err)
		}
		return total
	}
	mustCatchUp()

	if _, err := db.CreatePost("b1", "bob", "hi"); err != nil {
		t.Fatal(err)
	}
	if got, _ := p.homeFeed("ann"); len(got) != 0 {
		t.Fatalf("post visible before catch-up: %v", ids(got))
	}
	if c := mustCatchUp(); c.Rows != 2 {
		t.Fatalf("post by bob wrote %d view rows, want 2 (ann, cat)", c.Rows)
	}
	if _, err := db.CreatePost("s1", "star", "big news"); err != nil {
		t.Fatal(err)
	}
	if c := mustCatchUp(); c.Rows != 1 {
		t.Fatalf("celebrity post wrote %d view rows, want 1 (its outbox)", c.Rows)
	}
	for _, viewer := range []string{"ann", "cat"} {
		got, _ := p.homeFeed(viewer)
		want, _ := db.HomeFeedJoin(viewer, PageSize)
		if !slices.Equal(got, want) {
			t.Fatalf("%s: view %v, join %v", viewer, ids(got), ids(want))
		}
	}
	if got, _ := p.homeFeed("ann"); !slices.Equal(ids(got), []string{"s1", "b1"}) {
		t.Fatalf("ann's feed = %v, want [s1 b1]", ids(got))
	}
	if err := db.UpdateUser("bob", func(u *mv.User) { u.Name = "Robert" }); err != nil {
		t.Fatal(err)
	}
	if got, _ := p.homeFeed("cat"); got[0].AuthorName != "bob" {
		t.Fatalf("rename visible before catch-up: %q", got[0].AuthorName)
	}
	if c := mustCatchUp(); c.Rows != 2 {
		t.Fatalf("rename rewrote %d rows, want 2", c.Rows)
	}
	if got, _ := p.homeFeed("cat"); got[0].AuthorName != "Robert" {
		t.Fatalf("after catch-up author = %q, want Robert", got[0].AuthorName)
	}
}

// Feed reads may run concurrently with each other.
func TestConcurrentReads(t *testing.T) {
	db := mv.NewSocialDB()
	seed(db)
	p, err := newProjector(db, celebIDs())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.catchUp(nil); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 20 {
				if _, c := p.homeFeed(userID(g*20 + i)); c.Lookups+c.Rows > MaxFeedWork {
					t.Errorf("read cost %d", c.Lookups+c.Rows)
				}
			}
		}()
	}
	wg.Wait()
}
