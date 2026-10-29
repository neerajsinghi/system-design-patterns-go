package vectorannindex

import (
	"errors"
	"math"
	"math/rand/v2"
	"reflect"
	"sort"
	"sync"
	"testing"
)

const (
	tDim      = 16
	tN        = 3000
	tClusters = 12
	tNList    = 32
)

func fixture(t *testing.T, metric Metric) (*Flat, *IVF, [][]float32, [][]float32) {
	t.Helper()
	rng := rand.New(rand.NewPCG(3, 4))
	data := Clustered(rng, tN, tDim, tClusters, 0.8)
	flat, err := NewFlat(tDim, metric)
	if err != nil {
		t.Fatal(err)
	}
	ivf, err := NewIVF(tDim, tNList, metric, WithSeed(5, 6))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ivf.Train(data[:1000]); err != nil {
		t.Fatal(err)
	}
	for id, v := range data {
		if err := flat.Add(id, v); err != nil {
			t.Fatal(err)
		}
		if err := ivf.Add(id, v); err != nil {
			t.Fatal(err)
		}
	}
	qs := make([][]float32, 60)
	for i := range qs {
		qs[i] = Jitter(rng, data[rng.IntN(tN)], 0.8)
	}
	return flat, ivf, data, qs
}

func TestFlatMatchesFullSort(t *testing.T) {
	for _, m := range []Metric{Cosine, L2} {
		t.Run(m.String(), func(t *testing.T) {
			flat, _, data, qs := fixture(t, m)
			for _, q := range qs[:10] {
				pq, _ := prepare(m, tDim, q)
				all := make([]Result, len(data))
				for id, v := range data {
					pv, _ := prepare(m, tDim, v)
					all[id] = Result{ID: id, Score: score(m, pq, pv)}
				}
				sort.Slice(all, func(i, j int) bool { return better(all[i], all[j]) })
				got, err := flat.Search(q, 10)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, all[:10]) {
					t.Fatalf("flat top-10 = %v, want %v", got, all[:10])
				}
			}
		})
	}
}

func TestFlatSmallExamples(t *testing.T) {
	f, _ := NewFlat(2, L2)
	for id, v := range [][]float32{{0, 0}, {1, 0}, {0, 2}, {5, 5}} {
		if err := f.Add(id, v); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := f.Search([]float32{0.9, 0}, 2)
	if got[0].ID != 1 || got[1].ID != 0 {
		t.Fatalf("got %v, want ids 1 then 0", got)
	}
	if math.Abs(got[0].Score-(-0.01)) > 1e-6 {
		t.Fatalf("L2 score = %v, want -0.01 (negated squared distance)", got[0].Score)
	}
	all, _ := f.Search([]float32{0, 0}, 10)
	if len(all) != 4 {
		t.Fatalf("k > N returned %d results, want 4", len(all))
	}

	c, _ := NewFlat(2, Cosine)
	_ = c.Add(1, []float32{10, 0}) // same direction as the query, far away
	_ = c.Add(2, []float32{1, 1})
	got, _ = c.Search([]float32{1, 0}, 1)
	if got[0].ID != 1 || math.Abs(got[0].Score-1) > 1e-6 {
		t.Fatalf("cosine ignores length: got %v, want id 1 with score 1", got)
	}
}

func TestIVFFullProbeEqualsFlat(t *testing.T) {
	for _, m := range []Metric{Cosine, L2} {
		t.Run(m.String(), func(t *testing.T) {
			flat, ivf, _, qs := fixture(t, m)
			for _, q := range qs {
				want, _ := flat.Search(q, 10)
				got, st, err := ivf.Search(q, 10, tNList)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("nprobe=nlist: got %v, want %v", got, want)
				}
				if st.VectorsScanned != tN || st.ListsProbed != tNList {
					t.Fatalf("stats %+v, want all %d vectors in %d lists", st, tN, tNList)
				}
			}
			// nprobe beyond nlist is clamped, not an error.
			if _, st, err := ivf.Search(qs[0], 10, 10*tNList); err != nil || st.ListsProbed != tNList {
				t.Fatalf("nprobe > nlist: %+v, %v", st, err)
			}
		})
	}
}

func TestRecallAndWorkRiseWithNProbe(t *testing.T) {
	flat, ivf, _, qs := fixture(t, Cosine)
	truth := make([][]Result, len(qs))
	for i, q := range qs {
		truth[i], _ = flat.Search(q, 10)
	}
	prevRecall, prevScanned := -1.0, -1
	for _, np := range []int{1, 2, 4, 8, 16, 32} {
		var recall float64
		scanned := 0
		for i, q := range qs {
			got, st, err := ivf.Search(q, 10, np)
			if err != nil {
				t.Fatal(err)
			}
			recall += Recall(got, truth[i])
			scanned += st.VectorsScanned
		}
		recall /= float64(len(qs))
		t.Logf("nprobe=%2d recall=%.3f scanned/query=%d", np, recall, scanned/len(qs))
		// Probing more lists is a superset of the previous probe, so per
		// query recall can only stay the same or improve.
		if recall < prevRecall {
			t.Fatalf("recall fell from %.3f to %.3f at nprobe=%d", prevRecall, recall, np)
		}
		if scanned <= prevScanned {
			t.Fatalf("scanned did not grow at nprobe=%d", np)
		}
		prevRecall, prevScanned = recall, scanned
	}
	if prevRecall != 1 {
		t.Fatalf("full probe recall = %v, want 1", prevRecall)
	}
	// The fixture must actually exercise the trade-off.
	r1 := 0.0
	for i, q := range qs {
		got, _, _ := ivf.Search(q, 10, 1)
		r1 += Recall(got, truth[i])
	}
	if r1/float64(len(qs)) > 0.95 {
		t.Fatalf("nprobe=1 recall %.3f: fixture too easy to show the dial", r1/float64(len(qs)))
	}
}

func TestKMeansDeterministic(t *testing.T) {
	data := Clustered(rand.New(rand.NewPCG(8, 9)), 2000, tDim, 10, 0.5)
	train := func(s1, s2 uint64) ([][]float32, TrainStats) {
		ix, _ := NewIVF(tDim, 20, L2, WithSeed(s1, s2))
		st, err := ix.Train(data)
		if err != nil {
			t.Fatal(err)
		}
		return ix.centroids, st
	}
	a, sa := train(1, 1)
	b, sb := train(1, 1)
	if !reflect.DeepEqual(a, b) || sa != sb {
		t.Fatal("same seed and data gave different centroids or stats")
	}
	c, _ := train(2, 7)
	if reflect.DeepEqual(a, c) {
		t.Fatal("different seeds gave identical centroids; seed is not used")
	}
	if sa.Iterations == 0 || sa.DistanceComputations == 0 || sa.Samples != 2000 {
		t.Fatalf("stats look wrong: %+v", sa)
	}
}

func TestKMeansConvergesOrStopsAtCap(t *testing.T) {
	data := Clustered(rand.New(rand.NewPCG(8, 9)), 2000, tDim, 10, 0.5)
	ix, _ := NewIVF(tDim, 20, L2, WithMaxIterations(2))
	st, _ := ix.Train(data)
	if st.Iterations != 2 || st.Converged {
		t.Fatalf("cap of 2: %+v", st)
	}
	ix, _ = NewIVF(tDim, 10, L2, WithMaxIterations(100))
	st, _ = ix.Train(data)
	if !st.Converged || st.Iterations >= 100 {
		t.Fatalf("well-separated blobs should converge: %+v", st)
	}
}

func TestAddAfterTrainLandsInNearestList(t *testing.T) {
	_, ivf, _, _ := fixture(t, L2)
	rng := rand.New(rand.NewPCG(10, 11))
	for i := range 200 {
		v := make([]float32, tDim)
		for j := range v {
			v[j] = float32(rng.NormFloat64() * 2)
		}
		id := 100_000 + i
		if err := ivf.Add(id, v); err != nil {
			t.Fatal(err)
		}
		// Brute-force the nearest centroid independently.
		best, bestD := -1, math.Inf(1)
		for c, cv := range ivf.centroids {
			var d float64
			for j := range v {
				x := float64(v[j]) - float64(cv[j])
				d += x * x
			}
			if d < bestD {
				best, bestD = c, d
			}
		}
		got, ok := ivf.ListOf(id)
		if !ok || got != best {
			t.Fatalf("id %d in list %d, nearest centroid is %d", id, got, best)
		}
		if nl, _ := ivf.NearestList(v); nl != best {
			t.Fatalf("NearestList = %d, want %d", nl, best)
		}
	}
	total := 0
	for _, s := range ivf.ListSizes() {
		total += s
	}
	if total != tN+200 || ivf.Len() != tN+200 {
		t.Fatalf("list sizes sum to %d, Len %d, want %d", total, ivf.Len(), tN+200)
	}
}

func TestRetrainReassignsEverything(t *testing.T) {
	_, ivf, _, _ := fixture(t, Cosine)
	before := ivf.ListSizes()
	st, err := ivf.Retrain(0)
	if err != nil {
		t.Fatal(err)
	}
	if st.Samples != tN || st.Reassigned != tN || ivf.Len() != tN {
		t.Fatalf("retrain stats %+v, Len %d", st, ivf.Len())
	}
	st, _ = ivf.Retrain(500)
	if st.Samples != 500 || st.Reassigned != tN {
		t.Fatalf("sampled retrain stats %+v", st)
	}
	total := 0
	for _, s := range ivf.ListSizes() {
		total += s
	}
	if total != tN || len(before) != tNList {
		t.Fatalf("lists hold %d vectors after retrain, want %d", total, tN)
	}
	// Every vector must be in its nearest list under the new centroids.
	for id := range 50 {
		l, _ := ivf.ListOf(id)
		var v []float32
		for i, x := range ivf.lists[l].ids {
			if x == id {
				v = ivf.lists[l].vecs[i]
			}
		}
		if v == nil || nearest(Cosine, ivf.centroids, v) != l {
			t.Fatalf("id %d is not in its nearest list after retrain", id)
		}
	}
}

func TestDriftHurtsRecallAndRetrainFixesIt(t *testing.T) {
	flat, ivf, _, _ := fixture(t, Cosine)
	rng := rand.New(rand.NewPCG(20, 21))
	drift := Clustered(rand.New(rand.NewPCG(77, 78)), 1500, tDim, 6, 0.8)
	for i, v := range drift {
		_ = flat.Add(tN+i, v)
		_ = ivf.Add(tN+i, v)
	}
	qs := make([][]float32, 60)
	for i := range qs {
		qs[i] = Jitter(rng, drift[rng.IntN(len(drift))], 0.8)
	}
	recall := func() float64 {
		var r float64
		for _, q := range qs {
			truth, _ := flat.Search(q, 10)
			got, _, _ := ivf.Search(q, 10, 4)
			r += Recall(got, truth)
		}
		return r / float64(len(qs))
	}
	before := recall()
	if _, err := ivf.Retrain(0); err != nil {
		t.Fatal(err)
	}
	after := recall()
	t.Logf("new-topic recall@10 at nprobe=4: %.3f before retrain, %.3f after", before, after)
	if after <= before {
		t.Fatalf("retrain did not improve recall on drifted data: %.3f -> %.3f", before, after)
	}
}

func TestValidation(t *testing.T) {
	if _, err := NewFlat(0, Cosine); err == nil {
		t.Error("NewFlat dim 0: want error")
	}
	if _, err := NewFlat(4, Metric(9)); err == nil {
		t.Error("NewFlat bad metric: want error")
	}
	for _, tc := range []struct {
		dim, nlist int
		m          Metric
		opts       []Option
	}{
		{0, 4, L2, nil},
		{4, 0, L2, nil},
		{4, 4, Metric(-1), nil},
		{4, 4, L2, []Option{WithMaxIterations(0)}},
	} {
		if _, err := NewIVF(tc.dim, tc.nlist, tc.m, tc.opts...); err == nil {
			t.Errorf("NewIVF(%d, %d, %v): want error", tc.dim, tc.nlist, tc.m)
		}
	}

	f, _ := NewFlat(3, Cosine)
	vecErrs := []struct {
		v    []float32
		want error
	}{
		{[]float32{1, 2}, ErrDimension},
		{[]float32{0, 0, 0}, ErrZeroVector},
		{[]float32{1, float32(math.NaN()), 0}, ErrNotFinite},
		{[]float32{1, float32(math.Inf(1)), 0}, ErrNotFinite},
	}
	for _, tc := range vecErrs {
		if err := f.Add(1, tc.v); !errors.Is(err, tc.want) {
			t.Errorf("Add(%v) = %v, want %v", tc.v, err, tc.want)
		}
	}
	_ = f.Add(1, []float32{1, 0, 0})
	if err := f.Add(1, []float32{0, 1, 0}); !errors.Is(err, ErrDuplicateID) {
		t.Errorf("duplicate id: %v", err)
	}
	if _, err := f.Search([]float32{1, 0, 0}, 0); !errors.Is(err, ErrBadK) {
		t.Errorf("k=0: %v", err)
	}
	if _, err := f.Search([]float32{1, 0}, 1); !errors.Is(err, ErrDimension) {
		t.Errorf("short query: %v", err)
	}
	l2, _ := NewFlat(2, L2)
	if err := l2.Add(1, []float32{0, 0}); err != nil {
		t.Errorf("zero vector is fine under L2: %v", err)
	}

	ix, _ := NewIVF(2, 4, L2)
	if err := ix.Add(1, []float32{1, 1}); !errors.Is(err, ErrNotTrained) {
		t.Errorf("Add before Train: %v", err)
	}
	if _, _, err := ix.Search([]float32{1, 1}, 1, 1); !errors.Is(err, ErrNotTrained) {
		t.Errorf("Search before Train: %v", err)
	}
	if _, err := ix.NearestList([]float32{1, 1}); !errors.Is(err, ErrNotTrained) {
		t.Errorf("NearestList before Train: %v", err)
	}
	if _, err := ix.Train([][]float32{{1, 1}, {2, 2}}); !errors.Is(err, ErrTooFewToTrain) {
		t.Errorf("2 samples for 4 lists: %v", err)
	}
	if _, err := ix.Train([][]float32{{1, 1}, {2}, {3, 3}, {4, 4}}); !errors.Is(err, ErrDimension) {
		t.Errorf("bad sample dimension: %v", err)
	}
	if ix.Trained() {
		t.Error("failed Train must leave the index untrained")
	}
	if _, err := ix.Retrain(-1); err == nil {
		t.Error("Retrain(-1): want error")
	}
	if _, err := ix.Train([][]float32{{0, 0}, {1, 0}, {0, 1}, {1, 1}, {5, 5}}); err != nil {
		t.Fatal(err)
	}
	_ = ix.Add(1, []float32{1, 1})
	if err := ix.Add(1, []float32{2, 2}); !errors.Is(err, ErrDuplicateID) {
		t.Errorf("IVF duplicate id: %v", err)
	}
	if _, _, err := ix.Search([]float32{1, 1}, 1, 0); !errors.Is(err, ErrBadNProbe) {
		t.Errorf("nprobe=0: %v", err)
	}
	if _, _, err := ix.Search([]float32{1, 1}, 0, 1); !errors.Is(err, ErrBadK) {
		t.Errorf("k=0: %v", err)
	}
}

func TestTrainIdenticalPoints(t *testing.T) {
	// k-means++ with all distances zero must not loop or panic.
	ix, _ := NewIVF(2, 3, L2)
	same := [][]float32{{1, 1}, {1, 1}, {1, 1}, {1, 1}}
	if _, err := ix.Train(same); err != nil {
		t.Fatal(err)
	}
	if err := ix.Add(1, []float32{1, 1}); err != nil {
		t.Fatal(err)
	}
	got, _, _ := ix.Search([]float32{1, 1}, 1, 3)
	if len(got) != 1 || got[0].ID != 1 {
		t.Fatalf("got %v", got)
	}
}

func TestRecall(t *testing.T) {
	truth := []Result{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}}
	if r := Recall([]Result{{ID: 2}, {ID: 4}, {ID: 9}}, truth); r != 0.5 {
		t.Fatalf("recall = %v, want 0.5", r)
	}
	if r := Recall(nil, nil); r != 1 {
		t.Fatalf("empty truth recall = %v, want 1", r)
	}
}

func TestConcurrentQueries(t *testing.T) {
	flat, ivf, data, qs := fixture(t, Cosine)
	want := make([][]Result, len(qs))
	for i, q := range qs {
		want[i], _, _ = ivf.Search(q, 10, 4)
	}
	const workers = 8
	before := ivf.Stats()

	// Phase 1: readers only. Every concurrent answer must match the serial one.
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i, q := range qs {
				got, _, err := ivf.Search(q, 10, 4)
				if err != nil || !reflect.DeepEqual(got, want[i]) {
					t.Errorf("worker %d query %d: concurrent result differs", w, i)
					return
				}
			}
		}()
	}
	wg.Wait()
	if got := ivf.Stats().Queries - before.Queries; got != workers*uint64(len(qs)) {
		t.Fatalf("queries counted %d, want %d", got, workers*len(qs))
	}

	// Phase 2: readers while a writer adds and then retrains. Answers may
	// change as the data does, so only check they stay well formed.
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, q := range qs {
				got, _, err := ivf.Search(q, 10, 4)
				if err != nil || len(got) != 10 {
					t.Errorf("search during writes: %d results, %v", len(got), err)
					return
				}
				if _, err := flat.Search(q, 10); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 100 {
			_ = ivf.Add(tN+i, data[i])
			_ = flat.Add(tN+i, data[i])
		}
		if _, err := ivf.Retrain(500); err != nil {
			t.Error(err)
		}
	}()
	wg.Wait()
	if ivf.Len() != tN+100 || flat.Len() != tN+100 {
		t.Fatalf("Len = %d / %d, want %d", ivf.Len(), flat.Len(), tN+100)
	}
}
