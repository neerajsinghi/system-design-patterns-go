// Command example indexes 20,000 synthetic 32-dimensional "embeddings" and
// compares exact brute-force search with an IVF index. It measures recall@10
// against the exact answer and the number of vectors each query scans as
// nprobe grows, prices the k-means training, then adds a batch of new-topic
// vectors the centroids never saw and shows recall falling until Retrain
// re-clusters the data.
//
// Work is reported as "vectors scanned" (a deterministic count) rather than
// wall time, so the output is identical on every machine and every run.
package main

import (
	"fmt"
	"log"
	"math/rand/v2"

	vectorannindex "github.com/neerajsinghi/system-design-patterns-go/patterns/ai/vector-ann-index"
)

const (
	dim      = 32
	n        = 20_000
	clusters = 40    // "topics" in the original corpus
	spread   = 1.0   // per-coordinate noise around each topic centre
	nlist    = 100   // near sqrt(n), a common starting point
	sample   = 4_000 // 40 training vectors per centroid
	queries  = 100
	k        = 10
	operate  = 8 // nprobe chosen as the operating point after the sweep

	driftN        = 8_000
	driftClusters = 16
)

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// truths computes the exact top-k for every query with the flat index. It is
// computed once per corpus state and reused across the nprobe sweep.
func truths(flat *vectorannindex.Flat, qs [][]float32) [][]vectorannindex.Result {
	out := make([][]vectorannindex.Result, len(qs))
	for i, q := range qs {
		r, err := flat.Search(q, k)
		must(err)
		out[i] = r
	}
	return out
}

// measure runs qs against ivf at the given nprobe, returning mean recall@k
// against truth and mean vectors scanned per query.
func measure(ivf *vectorannindex.IVF, qs [][]float32, truth [][]vectorannindex.Result, nprobe int) (float64, float64) {
	var recall, scanned float64
	for i, q := range qs {
		got, st, err := ivf.Search(q, k, nprobe)
		must(err)
		recall += vectorannindex.Recall(got, truth[i])
		scanned += float64(st.VectorsScanned)
	}
	return recall / float64(len(qs)), scanned / float64(len(qs))
}

// spreadOf returns the mean number of distinct inverted lists that each
// query's exact top-k is filed in. The more lists the true neighbours are
// scattered over, the more of them a fixed nprobe will miss.
func spreadOf(ivf *vectorannindex.IVF, truth [][]vectorannindex.Result) float64 {
	var total int
	for _, t := range truth {
		lists := map[int]bool{}
		for _, r := range t {
			l, _ := ivf.ListOf(r.ID)
			lists[l] = true
		}
		total += len(lists)
	}
	return float64(total) / float64(len(truth))
}

func maxOf(xs []int) int {
	m := 0
	for _, x := range xs {
		m = max(m, x)
	}
	return m
}

func nonEmpty(xs []int) int {
	c := 0
	for _, x := range xs {
		if x > 0 {
			c++
		}
	}
	return c
}

var oneSweep = []int{1, 2, 4, 8, 16, 32, 64, nlist}

func main() {
	rng := rand.New(rand.NewPCG(7, 2026))
	data := vectorannindex.Clustered(rng, n, dim, clusters, spread)

	flat, err := vectorannindex.NewFlat(dim, vectorannindex.Cosine)
	must(err)
	ivf, err := vectorannindex.NewIVF(dim, nlist, vectorannindex.Cosine,
		vectorannindex.WithSeed(11, 22), vectorannindex.WithMaxIterations(15))
	must(err)

	// 1. Train the coarse quantiser on a random sample, then add everything.
	perm := rng.Perm(n)
	train := make([][]float32, sample)
	for i := range train {
		train[i] = data[perm[i]]
	}
	ts, err := ivf.Train(train)
	must(err)
	for id, v := range data {
		must(flat.Add(id, v))
		must(ivf.Add(id, v))
	}
	sizes := ivf.ListSizes()
	fmt.Printf("1. Indexed %d vectors (d=%d, cosine, %d synthetic topics) in a flat index and an IVF index with nlist=%d.\n",
		n, dim, clusters, nlist)
	fmt.Printf("   k-means on a %d-vector sample: %d iterations (converged: %v), %d vector-centroid distances.\n",
		ts.Samples, ts.Iterations, ts.Converged, ts.DistanceComputations)
	fmt.Printf("   Lists: %d non-empty, average %d vectors, largest %d.\n\n", nonEmpty(sizes), n/nlist, maxOf(sizes))

	// 2. Queries near the data; flat gives the exact top-10.
	qs := make([][]float32, queries)
	for i := range qs {
		qs[i] = vectorannindex.Jitter(rng, data[rng.IntN(n)], spread)
	}
	fmt.Printf("2. %d queries, each a stored vector plus noise. Flat search scans all %d vectors per query (%d total).\n\n",
		queries, n, queries*n)

	// 3. The recall / work dial.
	truth := truths(flat, qs)
	fmt.Println("3. IVF recall@10 vs flat, as nprobe grows:")
	fmt.Println("   nprobe   recall@10   vectors scanned/query   % of flat")
	var opRecall, opScanned float64
	for _, np := range oneSweep {
		r, s := measure(ivf, qs, truth, np)
		if np == operate {
			opRecall, opScanned = r, s
		}
		fmt.Printf("   %6d   %9.3f   %21.0f   %8.1f%%\n", np, r, s, 100*s/n)
	}
	fmt.Printf("   nprobe=%d is the operating point used below: recall %.3f while scanning %.1f%% of the corpus\n",
		operate, opRecall, 100*opScanned/n)
	fmt.Printf("   (plus %d centroid comparisons per query). nprobe=%d probes every list and is exact.\n\n", nlist, nlist)

	// 4. Cost of training.
	perQuery := opScanned + nlist
	fmt.Println("4. Training is not free:")
	fmt.Printf("   training did %d distance computations, the work of ~%.0f flat queries or ~%.0f IVF queries at nprobe=%d.\n",
		ts.DistanceComputations, float64(ts.DistanceComputations)/n, float64(ts.DistanceComputations)/perQuery, operate)
	fmt.Printf("   Each IVF query at nprobe=%d saves ~%.0f distance computations vs flat, so training pays for itself after ~%.0f queries.\n\n",
		operate, n-perQuery, float64(ts.DistanceComputations)/(n-perQuery))

	// 5. Drift: new topics the centroids never saw.
	drift := vectorannindex.Clustered(rand.New(rand.NewPCG(99, 1)), driftN, dim, driftClusters, spread)
	for i, v := range drift {
		must(flat.Add(n+i, v))
		must(ivf.Add(n+i, v))
	}
	newQs := make([][]float32, queries)
	for i := range newQs {
		newQs[i] = vectorannindex.Jitter(rng, drift[rng.IntN(driftN)], spread)
	}
	// Ground truth is recomputed: the corpus changed, so the exact top-10
	// of an old-topic query can now include new vectors.
	truth = truths(flat, qs)
	newTruth := truths(flat, newQs)
	intruders := 0
	for _, t := range truth {
		for _, r := range t {
			if r.ID >= n {
				intruders++
			}
		}
	}
	oldR, oldS := measure(ivf, qs, truth, operate)
	newR, newS := measure(ivf, newQs, newTruth, operate)
	sizes = ivf.ListSizes()
	fmt.Printf("5. Added %d vectors from %d new topics without retraining; each was filed under the nearest old centroid.\n",
		driftN, driftClusters)
	fmt.Printf("   Largest list is now %d vectors (average %d).\n", maxOf(sizes), (n+driftN)/nlist)
	fmt.Printf("   %d of the %d exact top-10 slots for old-topic queries now hold new vectors.\n", intruders, queries*k)
	fmt.Printf("   nprobe=%d, queries on old topics: recall %.3f, %.0f scanned/query\n", operate, oldR, oldS)
	fmt.Printf("   nprobe=%d, queries on new topics: recall %.3f, %.0f scanned/query\n", operate, newR, newS)
	fmt.Printf("   A new-topic query's true top-10 is scattered over %.1f lists on average (old-topic queries: %.1f):\n",
		spreadOf(ivf, newTruth), spreadOf(ivf, truth))
	fmt.Printf("   the old centroids cut the new topics along boundaries that mean nothing for them, so %d probes miss neighbours.\n", operate)
	fmt.Println("   Work per query grew only because the lists grew with the corpus; the real damage is recall.")
	fmt.Println()

	// 6. Retrain on a sample of everything stored.
	rs, err := ivf.Retrain(sample)
	must(err)
	oldR2, oldS2 := measure(ivf, qs, truth, operate)
	newR2, newS2 := measure(ivf, newQs, newTruth, operate)
	sizes = ivf.ListSizes()
	fmt.Printf("6. Retrain: k-means on a %d-vector sample of the %d stored, %d iterations, %d distance computations, %d vectors re-filed.\n",
		rs.Samples, ivf.Len(), rs.Iterations, rs.DistanceComputations, rs.Reassigned)
	fmt.Printf("   Largest list is now %d vectors.\n", maxOf(sizes))
	fmt.Printf("   nprobe=%d, queries on old topics: recall %.3f, %.0f scanned/query\n", operate, oldR2, oldS2)
	fmt.Printf("   nprobe=%d, queries on new topics: recall %.3f, %.0f scanned/query\n", operate, newR2, newS2)
	fmt.Printf("   A new-topic query's true top-10 now sits in %.1f lists on average; recall on new topics went %.3f -> %.3f.\n\n",
		spreadOf(ivf, newTruth), newR, newR2)

	st := ivf.Stats()
	flatWork := uint64(len(oneSweep)*queries*n + 4*queries*(n+driftN))
	fmt.Printf("IVF answered %d queries scanning %d vectors in total; flat would have scanned %d (IVF did %.1f%%).\n",
		st.Queries, st.VectorsScanned, flatWork, 100*float64(st.VectorsScanned)/float64(flatWork))
}
