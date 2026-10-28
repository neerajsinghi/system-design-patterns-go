// Command example keeps a "revenue per customer" dashboard up to date from an
// orders table's change stream, and compares the work done with a full
// recomputation. It then builds a denormalized home-feed view and compares
// rows touched per read and per write against a normalized JOIN.
package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"slices"

	"github.com/neerajsinghi/system-design-patterns-go/patterns/data/materialized-view"
)

func dollars(c int64) string { return fmt.Sprintf("$%d.%02d", c/100, c%100) }

func printView(v *materializedview.View) {
	for _, r := range v.All() {
		fmt.Printf("    %-8s orders=%-5d revenue=%s\n", r.Customer, r.Orders, dollars(r.Revenue))
	}
}

func main() {
	orders := materializedview.NewTable()
	view := materializedview.NewView()
	must := func(err error) {
		if err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("Bulk load: 10,000 historical orders across 3 customers")
	customers := []string{"acme", "globex", "initech"}
	for i := range 10_000 {
		must(orders.Insert(materializedview.Order{
			ID: fmt.Sprintf("h%d", i), Customer: customers[i%3], Status: "shipped", Amount: int64(100 + i%900),
		}))
	}
	n, err := view.CatchUp(orders)
	must(err)
	fmt.Printf("  view applied %d changes (LSN %d)\n", n, view.LSN())
	printView(view)

	fmt.Println("\nLive traffic, as change events:")
	must(orders.Insert(materializedview.Order{ID: "n1", Customer: "acme", Status: "placed", Amount: 50_000}))
	fmt.Println("  INSERT n1 acme $500.00")
	must(orders.Update("h0", func(o *materializedview.Order) { o.Status = "cancelled" }))
	fmt.Println("  UPDATE h0 status=cancelled (retract its revenue)")
	must(orders.Update("n1", func(o *materializedview.Order) { o.Customer = "globex" }))
	fmt.Println("  UPDATE n1 customer acme -> globex (move between groups)")
	must(orders.Delete("h1"))
	fmt.Println("  DELETE h1")

	n, err = view.CatchUp(orders)
	must(err)
	fmt.Printf("\n  view applied only %d deltas (LSN %d)\n", n, view.LSN())
	printView(view)

	full := materializedview.Recompute(orders.Rows())
	match := len(full) == len(view.All())
	for i, r := range view.All() {
		match = match && r == full[i]
	}
	fmt.Printf("\nA full recompute scans %d rows to get the same answer: match=%v\n", len(orders.Rows()), match)

	fmt.Println("\nThe change stream redelivers an event (at-least-once delivery):")
	last := orders.ChangesSince(view.LSN() - 1)[0]
	must(view.Apply(last))
	fmt.Printf("  re-applied LSN %d: ignored, view still at LSN %d\n", last.LSN, view.LSN())

	must(orders.Insert(materializedview.Order{ID: "n2", Customer: "acme", Status: "placed", Amount: 1}))
	must(orders.Insert(materializedview.Order{ID: "n3", Customer: "acme", Status: "placed", Amount: 1}))
	skipped := orders.ChangesSince(view.LSN() + 1)[0]
	fmt.Printf("  a consumer that skips an event and delivers LSN %d gets: %v\n", skipped.LSN, view.Apply(skipped))

	feed()
}

func feed() {
	must := func(err error) {
		if err != nil {
			log.Fatal(err)
		}
	}
	const fans, celebPosts, authors, authorPosts, limit = 1000, 30, 20, 10, 20
	fmt.Println("\n--- Denormalized feed rows: why no JOIN on the read path ---")
	fmt.Printf("1. Load: @chef (%d posts) followed by %d fans; %d regular authors (%d posts each); each fan follows @chef + 5 authors and likes 3 of @chef's posts\n",
		celebPosts, fans, authors, authorPosts)
	rng := rand.New(rand.NewPCG(42, 1))
	db := materializedview.NewSocialDB()
	must(db.AddUser(materializedview.User{ID: "chef", Name: "Chef Ana", Avatar: "ana.png"}))
	for a := range authors {
		id := fmt.Sprint("author", a)
		must(db.AddUser(materializedview.User{ID: id, Name: id, Avatar: id + ".png"}))
	}
	for f := range fans {
		id := fmt.Sprint("fan", f)
		must(db.AddUser(materializedview.User{ID: id, Name: id}))
		must(db.Follow(id, "chef"))
		for _, a := range rng.Perm(authors)[:5] {
			must(db.Follow(id, fmt.Sprint("author", a)))
		}
	}
	for i := range celebPosts {
		_, err := db.CreatePost(fmt.Sprint("chef-", i), "chef", "recipe")
		must(err)
		for a := range authors {
			if i < authorPosts {
				_, err := db.CreatePost(fmt.Sprintf("author%d-%d", a, i), fmt.Sprint("author", a), "post")
				must(err)
			}
		}
	}
	for f := range fans {
		for _, p := range rng.Perm(celebPosts)[:3] {
			must(db.Like(fmt.Sprint("fan", f), fmt.Sprint("chef-", p)))
		}
	}

	embed, err := materializedview.NewFeedView()
	must(err)
	ref, err := materializedview.NewFeedView(materializedview.WithLikeMode(materializedview.ReferenceLikes))
	must(err)
	load, err := embed.CatchUp(db)
	must(err)
	refLoad, err := ref.CatchUp(db)
	must(err)
	fmt.Printf("   view stores %d feed rows (one per viewer x post)\n", embed.Rows())
	fmt.Printf("   building it wrote %d rows with like counts embedded (each of the %d likes rewrote %d copies), %d with them referenced\n",
		load.Rows, fans*3, fans, refLoad.Rows)

	fmt.Printf("\n2. Read fan0's home feed (newest %d):\n", limit)
	joined, jc := db.HomeFeedJoin("fan0", limit)
	fromView, vc := embed.HomeFeed("fan0", limit)
	_, rc := ref.HomeFeed("fan0", limit)
	fmt.Printf("   JOIN follows+posts+users+likes:  %4d lookups, %4d rows read\n", jc.Lookups, jc.Rows)
	fmt.Printf("   view, everything embedded:       %4d lookups, %4d rows read\n", vc.Lookups, vc.Rows)
	fmt.Printf("   view, like count referenced:     %4d lookups, %4d rows read\n", rc.Lookups, rc.Rows)
	fmt.Printf("   same %d items: %v; top item %s by %q with %d likes\n", len(fromView), slices.Equal(joined, fromView),
		fromView[0].PostID, fromView[0].AuthorName, fromView[0].Likes)

	fmt.Println("\n3. Writes (normalized tables write 1 row for each of these):")
	_, err = db.CreatePost("chef-new", "chef", "new recipe")
	must(err)
	c, _ := embed.CatchUp(db)
	_, _ = ref.CatchUp(db)
	fmt.Printf("   @chef posts:          view inserts %5d rows (one per follower)\n", c.Rows)
	must(db.UpdateUser("chef", func(u *materializedview.User) { u.Name = "Chef Ana Lopez" }))
	c, _ = embed.CatchUp(db)
	_, _ = ref.CatchUp(db)
	fmt.Printf("   @chef renames:        view rewrites %5d rows (%d posts x %d followers)\n", c.Rows, celebPosts+1, fans)
	must(db.Like("fan1", "chef-new"))
	c, _ = embed.CatchUp(db)
	r, _ := ref.CatchUp(db)
	fmt.Printf("   fan1 likes chef-new:  embedded view rewrites %5d rows, referenced view writes %d\n", c.Rows, r.Rows)
	must(db.Follow("fan1", "author0"))
	c, _ = embed.CatchUp(db)
	_, _ = ref.CatchUp(db)
	fmt.Printf("   fan1 follows author0: view backfills %5d rows\n", c.Rows)

	fmt.Println("\n4. Consistency: every fan's feed from the view vs the JOIN, after all of the above")
	same := 0
	for f := range fans {
		id := fmt.Sprint("fan", f)
		j, _ := db.HomeFeedJoin(id, limit)
		e, _ := embed.HomeFeed(id, limit)
		x, _ := ref.HomeFeed(id, limit)
		if slices.Equal(j, e) && slices.Equal(j, x) {
			same++
		}
	}
	top, _ := embed.HomeFeed("fan0", 1)
	fmt.Printf("   %d/%d feeds identical; fan0 now sees %q by %q\n", same, fans, top[0].PostID, top[0].AuthorName)
	fmt.Printf("   total view maintenance so far: %d rows written for %d source changes\n", embed.WriteCost().Rows, embed.LSN())
}
