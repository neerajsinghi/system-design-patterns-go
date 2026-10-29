// Command example builds a 100,000-player leaderboard, applies 200,000 score
// updates, and answers "top 10", "my rank" and "players around me" from a
// skip list with span counts, counting the nodes each rank lookup visits next
// to a linear scan. It then runs a trending-hashtag stream through a
// 50-counter Space-Saving summary and compares its top 10 with exact counts.
package main

import (
	"fmt"
	"log"
	"math/rand/v2"

	leaderboard "github.com/neerajsinghi/system-design-patterns-go/patterns/data/leaderboard-top-k"
)

const (
	players = 100_000
	updates = 200_000
	me      = "player-042424"

	events   = 300_000
	hashtags = 20_000
	counters = 50
)

func main() {
	fmt.Println("1) Load 100,000 players with seeded scores, then apply 200,000 score updates")
	board, err := leaderboard.NewSortedSet(leaderboard.WithSeed(7, 11))
	if err != nil {
		log.Fatal(err)
	}
	table := make(map[string]int64, players) // what a SQL table would hold
	r := rand.New(rand.NewPCG(1, 2))
	names := make([]string, players)
	for i := range players {
		name := fmt.Sprintf("player-%06d", i)
		names[i] = name
		score := r.Int64N(10_000)
		board.Add(name, score)
		table[name] = score
	}
	for range updates {
		name := names[r.IntN(players)]
		delta := 1 + r.Int64N(500)
		if _, err := board.IncrBy(name, delta); err != nil {
			log.Fatal(err)
		}
		table[name] += delta
	}
	fmt.Printf("   %d members on the board\n\n", board.Len())

	fmt.Println("2) Top 10 (ZREVRANGE board 0 9 WITHSCORES)")
	for _, e := range board.Top(10) {
		fmt.Printf("   #%-3d %s  %6d\n", e.Rank+1, e.Member, e.Score)
	}

	fmt.Printf("\n3) Rank of %s, counted in nodes visited\n", me)
	rank, steps, _ := board.RankSteps(me)
	myScore, _ := board.Score(me)
	scan := 0
	for name, sc := range table { // SELECT COUNT(*) WHERE (score, name) ranks ahead
		if sc > myScore || (sc == myScore && name < me) {
			scan++
		}
	}
	fmt.Printf("   skip list (ZREVRANK): rank #%d (score %d) after visiting %d nodes\n", rank+1, myScore, steps)
	fmt.Printf("   COUNT(*) scan:        rank #%d after visiting %d rows\n", scan+1, len(table))
	fmt.Printf("   walking the sorted list from the top: %d nodes\n", rank+1)

	total, worst := 0, 0
	const lookups = 1000
	for range lookups {
		_, s, _ := board.RankSteps(names[r.IntN(players)])
		total += s
		worst = max(worst, s)
	}
	fmt.Printf("   over %d random players: skip list avg %.1f nodes (max %d) vs %d rows per scan\n\n",
		lookups, float64(total)/lookups, worst, players)

	fmt.Printf("4) Players around %s (3 above, 3 below)\n", me)
	around, _ := board.Around(me, 3)
	for _, e := range around {
		marker := " "
		if e.Member == me {
			marker = ">"
		}
		fmt.Printf("  %s#%-6d %s  %6d\n", marker, e.Rank+1, e.Member, e.Score)
	}
	top, _ := board.Around(board.Top(1)[0].Member, 3)
	fmt.Printf("   around the leader the window is cut short: %d entries, ranks #%d..#%d\n\n",
		len(top), top[0].Rank+1, top[len(top)-1].Rank+1)

	fmt.Printf("5) Trending hashtags: %d events over %d distinct tags (Zipf s=1.3), Space-Saving with k=%d\n",
		events, hashtags, counters)
	res := trend(1.3, counters)[0]
	fmt.Println("   rank  true top 10           Space-Saving top 10 (count, error)")
	for i := range res.trueTop {
		e := res.estTop[i]
		fmt.Printf("   %2d    %s %7d     %s %7d  err %d\n", i+1, res.trueTop[i].Item, res.trueTop[i].Count, e.Item, e.Count, e.Error)
	}
	res.summary()
	fmt.Printf("   memory: %d counters instead of %d exact counts\n\n", counters, res.distinct)

	fmt.Println("6) A flatter tail (Zipf s=1.1), same seed: the 10th tag sinks into the noise at k=50")
	for _, r := range trend(1.1, counters, 4*counters) {
		fmt.Printf("   k=%d:\n", r.k)
		r.summary()
	}
}

type trendResult struct {
	trueTop  []leaderboard.ItemCount
	estTop   []leaderboard.Estimate
	minCount int64
	n        int64
	k        int
	distinct int
}

// trend feeds one seeded hashtag stream (Zipf skew s) to exact counts and to
// a Space-Saving summary for each k.
func trend(s float64, ks ...int) []trendResult {
	summaries := make([]*leaderboard.SpaceSaving, len(ks))
	for i, k := range ks {
		ss, err := leaderboard.NewSpaceSaving(k)
		if err != nil {
			log.Fatal(err)
		}
		summaries[i] = ss
	}
	tags := make([]string, hashtags)
	for i := range tags {
		tags[i] = fmt.Sprintf("#tag%05d", i)
	}
	counts := make([]int64, hashtags) // exact counts, indexed by tag
	z := rand.NewZipf(rand.New(rand.NewPCG(3, 4)), s, 1, hashtags-1)
	for range events {
		id := z.Uint64()
		counts[id]++
		for _, ss := range summaries {
			ss.Add(tags[id], 1)
		}
	}
	exact := make(map[string]int64, hashtags)
	for id, c := range counts {
		if c > 0 {
			exact[tags[id]] = c
		}
	}
	trueTop, _ := leaderboard.TopK(exact, 10) // exact counts + a size-10 heap
	out := make([]trendResult, len(ks))
	for i, ss := range summaries {
		out[i] = trendResult{trueTop: trueTop, estTop: ss.Top(10), minCount: ss.MinCount(), n: ss.N(), k: ks[i], distinct: len(exact)}
	}
	return out
}

func (r trendResult) summary() {
	inTrue := map[string]bool{}
	for _, t := range r.trueTop {
		inTrue[t.Item] = true
	}
	overlap := 0
	var maxErr int64
	for _, e := range r.estTop {
		if inTrue[e.Item] {
			overlap++
		}
		maxErr = max(maxErr, e.Error)
	}
	tenth := r.trueTop[len(r.trueTop)-1].Count
	verdict := "above it, so every true top-10 tag is guaranteed to be monitored"
	if tenth <= r.minCount {
		verdict = "below it, so the tail can push it out"
	}
	fmt.Printf("     overlap with the true top 10: %d/10; largest error in the reported top 10: %d\n", overlap, maxErr)
	fmt.Printf("     error bound (smallest counter) %d, worst case N/k = %d; true #10 has %d mentions, %s\n",
		r.minCount, r.n/int64(r.k), tenth, verdict)
}
