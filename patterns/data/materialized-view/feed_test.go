package materializedview

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
)

func newFeedView(t *testing.T, opts ...FeedOption) *FeedView {
	t.Helper()
	v, err := NewFeedView(opts...)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// assertConsistent checks that every user's feed from the view equals the
// joined query over the normalized tables.
func assertConsistent(t *testing.T, db *SocialDB, v *FeedView, users []string) {
	t.Helper()
	for _, u := range users {
		for _, limit := range []int{3, -1} {
			want, _ := db.HomeFeedJoin(u, limit)
			got, _ := v.HomeFeed(u, limit)
			if !slices.Equal(got, want) && !(len(got) == 0 && len(want) == 0) {
				t.Fatalf("feed of %s (limit %d) diverged:\nview %v\njoin %v", u, limit, got, want)
			}
		}
	}
}

func TestFeedViewMatchesJoinAfterRandomChanges(t *testing.T) {
	for _, mode := range []LikeMode{EmbedLikes, ReferenceLikes} {
		t.Run(fmt.Sprint("mode ", mode), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(11, 3))
			db := NewSocialDB()
			v := newFeedView(t, WithLikeMode(mode))
			var users []string
			for i := range 12 {
				id := fmt.Sprint("u", i)
				users = append(users, id)
				_ = db.AddUser(User{ID: id, Name: "name-" + id, Avatar: id + ".png"})
			}
			var posts []string
			pick := func(s []string) string { return s[rng.IntN(len(s))] }
			for i := range 2500 {
				switch r := rng.IntN(20); {
				case r < 5 || len(posts) == 0:
					id := fmt.Sprint("p", i)
					if _, err := db.CreatePost(id, pick(users), "text "+id); err == nil {
						posts = append(posts, id)
					}
				case r < 9:
					_ = db.Follow(pick(users), pick(users)) // self-follows and repeats fail and emit nothing
				case r < 11:
					_ = db.Unfollow(pick(users), pick(users))
				case r < 15:
					_ = db.Like(pick(users), pick(posts))
				case r < 16:
					_ = db.Unlike(pick(users), pick(posts))
				case r < 18:
					u := pick(users)
					_ = db.UpdateUser(u, func(x *User) { x.Name = fmt.Sprintf("%s-v%d", u, i) })
				default:
					j := rng.IntN(len(posts))
					_ = db.DeletePost(posts[j])
					posts = slices.Delete(posts, j, j+1)
				}
				if i%53 == 0 {
					if _, err := v.CatchUp(db); err != nil {
						t.Fatal(err)
					}
					assertConsistent(t, db, v, users)
				}
			}
			if _, err := v.CatchUp(db); err != nil {
				t.Fatal(err)
			}
			assertConsistent(t, db, v, users)
		})
	}
}

func TestRenamePropagatesAndCostsPostsTimesFollowers(t *testing.T) {
	db := NewSocialDB()
	v := newFeedView(t)
	_ = db.AddUser(User{ID: "star", Name: "Star", Avatar: "a.png"})
	const fans, posts = 7, 4
	for i := range fans {
		f := fmt.Sprint("fan", i)
		_ = db.AddUser(User{ID: f, Name: f})
		_ = db.Follow(f, "star")
	}
	for i := range posts {
		_, _ = db.CreatePost(fmt.Sprint("p", i), "star", "hi")
	}
	if _, err := v.CatchUp(db); err != nil {
		t.Fatal(err)
	}
	if v.Rows() != fans*posts {
		t.Fatalf("rows=%d", v.Rows())
	}

	_ = db.UpdateUser("star", func(u *User) { u.Name, u.Avatar = "Superstar", "b.png" })
	cost, err := v.CatchUp(db)
	if err != nil {
		t.Fatal(err)
	}
	if cost.Rows != fans*posts {
		t.Fatalf("rename rewrote %d rows, want posts*followers = %d", cost.Rows, fans*posts)
	}
	for i := range fans {
		feed, _ := v.HomeFeed(fmt.Sprint("fan", i), -1)
		for _, it := range feed {
			if it.AuthorName != "Superstar" || it.AuthorAvatar != "b.png" {
				t.Fatalf("stale author copy: %+v", it)
			}
		}
	}
	// The normalized write was one row; the read is where the join pays.
	_, joinCost := db.HomeFeedJoin("fan0", -1)
	_, viewCost := v.HomeFeed("fan0", -1)
	if want := (Cost{Lookups: 1 + 1 + 2*posts, Rows: 1 + posts + posts}); joinCost != want {
		t.Fatalf("join cost %+v want %+v", joinCost, want)
	}
	if want := (Cost{Lookups: 1, Rows: posts}); viewCost != want {
		t.Fatalf("view cost %+v want %+v", viewCost, want)
	}
}

func TestLikeModesTradeWriteForReadCost(t *testing.T) {
	setup := func(mode LikeMode) (*SocialDB, *FeedView) {
		db := NewSocialDB()
		v := newFeedView(t, WithLikeMode(mode))
		_ = db.AddUser(User{ID: "star"})
		for i := range 5 {
			_ = db.AddUser(User{ID: fmt.Sprint("f", i)})
			_ = db.Follow(fmt.Sprint("f", i), "star")
		}
		_, _ = db.CreatePost("p", "star", "")
		_, _ = db.CreatePost("q", "star", "")
		_, _ = v.CatchUp(db)
		return db, v
	}
	tests := []struct {
		mode      LikeMode
		likeWrite Cost
		read      Cost
	}{
		{EmbedLikes, Cost{Lookups: 5, Rows: 5}, Cost{Lookups: 1, Rows: 2}},
		{ReferenceLikes, Cost{Lookups: 1, Rows: 1}, Cost{Lookups: 3, Rows: 4}},
	}
	for _, tt := range tests {
		db, v := setup(tt.mode)
		_ = db.Like("f0", "p")
		w, _ := v.CatchUp(db)
		feed, r := v.HomeFeed("f1", -1)
		if w != tt.likeWrite || r != tt.read {
			t.Errorf("mode %d: like write %+v read %+v", tt.mode, w, r)
		}
		if feed[1].PostID != "p" || feed[1].Likes != 1 {
			t.Errorf("mode %d: feed %+v", tt.mode, feed)
		}
	}
}

func TestFeedValidationIdempotencyAndGaps(t *testing.T) {
	if _, err := NewFeedView(WithLikeMode(LikeMode(9))); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad mode: %v", err)
	}
	db := NewSocialDB()
	_ = db.AddUser(User{ID: "a"})
	_ = db.AddUser(User{ID: "b"})
	errs := []struct {
		err, want error
	}{
		{db.AddUser(User{ID: "a"}), ErrExists},
		{db.AddUser(User{}), ErrInvalid},
		{db.Follow("a", "a"), ErrInvalid},
		{db.Follow("a", "zz"), ErrNotFound},
		{db.Unfollow("a", "b"), ErrNotFound},
		{db.Like("a", "nope"), ErrNotFound},
		{db.Unlike("a", "nope"), ErrNotFound},
		{db.DeletePost("nope"), ErrNotFound},
		{db.UpdateUser("zz", nil), ErrNotFound},
	}
	for i, e := range errs {
		if !errors.Is(e.err, e.want) {
			t.Errorf("case %d: got %v want %v", i, e.err, e.want)
		}
	}
	if _, err := db.CreatePost("p", "zz", ""); !errors.Is(err, ErrNotFound) {
		t.Errorf("post by missing author: %v", err)
	}
	_ = db.Follow("a", "b")
	_, _ = db.CreatePost("p", "b", "")
	if n := len(db.ChangesSince(0)); n != 4 {
		t.Fatalf("failed mutations emitted changes: %d", n)
	}

	v := newFeedView(t)
	ch := db.ChangesSince(0)
	if _, err := v.Apply(ch[1]); !errors.Is(err, ErrGap) {
		t.Fatalf("want gap, got %v", err)
	}
	_, _ = v.CatchUp(db)
	if _, err := v.Apply(ch[3]); err != nil || v.Rows() != 1 { // redelivered PostCreated
		t.Fatalf("duplicate applied: err=%v rows=%d", err, v.Rows())
	}
	if _, err := v.Apply(SocialChange{LSN: v.LSN() + 1, Op: SocialOp(99)}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown op: %v", err)
	}
}

func TestFeedConcurrentWritesReadsAndCatchUp(t *testing.T) {
	db := NewSocialDB()
	v := newFeedView(t)
	for i := range 4 {
		_ = db.AddUser(User{ID: fmt.Sprint("u", i), Name: "n"})
	}
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			me := fmt.Sprint("u", w)
			_ = db.Follow(me, fmt.Sprint("u", (w+1)%4))
			for i := range 100 {
				id := fmt.Sprintf("%s-%d", me, i)
				_, _ = db.CreatePost(id, me, "")
				_ = db.Like(fmt.Sprint("u", (w+2)%4), id)
				if i%10 == 0 {
					_ = db.UpdateUser(me, func(u *User) { u.Name = id })
					if _, err := v.CatchUp(db); err != nil {
						t.Error(err)
						return
					}
					_, _ = v.HomeFeed(me, 5)
				}
			}
		}()
	}
	wg.Wait()
	if _, err := v.CatchUp(db); err != nil {
		t.Fatal(err)
	}
	assertConsistent(t, db, v, []string{"u0", "u1", "u2", "u3"})
}
