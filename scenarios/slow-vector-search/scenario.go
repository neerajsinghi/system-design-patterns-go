// Package slowvectorsearch simulates the "semantic search gets slow, then
// gets wrong" incident. A product search launches with flat (exact) search
// over product embeddings. It is fast at launch, but every query scores every
// product, so latency grows in step with the catalogue and the search page
// starts timing out. The team then builds an IVF index once and moves on.
// Months later, new product categories are added: their vectors are filed
// under centroids that were learned before those categories existed, and
// searches for them quietly return the wrong products.
//
// Broken serves flat search until it breaches the SLO, then switches to an
// IVF index trained once, with nprobe tuned once, and never touched again.
// Fixed uses an IVF index from launch, re-tunes nprobe at every growth stage
// to the lowest value that meets a recall target on held-out queries, and
// retrains when a drift signal fires: the corpus has doubled since the last
// training, or newly added vectors sit much further from their list's centre
// than the training data did.
//
// Latency is modelled, not measured: every vector scanned and every centroid
// compared costs ModelDim multiply-adds at NsPerMultiply. That keeps the
// output identical on every machine while the simulated corpus stays small.
package slowvectorsearch

import (
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"time"

	vectorannindex "github.com/neerajsinghi/system-design-patterns-go/patterns/ai/vector-ann-index"
)

const (
	// Dim is the length of the simulated embeddings.
	Dim = 32
	// NList is the number of IVF inverted lists in both designs.
	NList = 128
	// K is how many results a search returns.
	K = 10
	// Spread is the per-coordinate noise around each category's centre.
	Spread = 0.6
	// QueryNoise is the per-coordinate noise added to a stored vector to
	// make a query: a search phrased close to an existing product.
	QueryNoise = 0.3

	// ProductsPerVector is how many catalogue products one simulated vector
	// stands for. Corpus sizes and scan counts are multiplied by it.
	ProductsPerVector = 400
	// ModelDim is the embedding length the cost model prices (a typical
	// sentence-embedding size), not the simulated Dim.
	ModelDim = 768
	// NsPerMultiply is the modelled cost of one multiply-add in a distance
	// computation (SIMD float32 on one core).
	NsPerMultiply = 0.5
	// SLO is the p99 search latency the search page can afford.
	SLO = 100 * time.Millisecond

	// RecallTarget is the recall@10 every query group must reach.
	RecallTarget = 0.95
	// TuneTarget is the recall nprobe is tuned to on held-out tuning
	// queries: a margin above RecallTarget, since the tuning queries are not
	// the ones served.
	TuneTarget = 0.97

	// GrowthTrigger retrains when the corpus has grown by this fraction
	// since the last training (1.0 = doubled).
	GrowthTrigger = 1.0
	// DriftTrigger retrains when newly added vectors are this many times
	// further, on average, from the centre of the list they were filed in
	// than the vectors the lists were trained on.
	DriftTrigger = 1.3
	// TrainSample caps how many stored vectors k-means runs on.
	TrainSample = 1280
	// MaxIterations caps Lloyd iterations per training run.
	MaxIterations = 8

	// LaunchTopics, WaveATopics and WaveBTopics are the numbers of product
	// categories at launch and in the two later category launches.
	LaunchTopics = 40
	WaveATopics  = 12
	WaveBTopics  = 12

	// EvalQueries and TuneQueries are per query group (launch topics, new
	// topics) per stage.
	EvalQueries = 60
	TuneQueries = 40

	// BrokenIVFAfter is the stage (0-based) after which Broken builds its
	// one-off IVF index.
	BrokenIVFAfter = 2
)

// Stage is the catalogue at one growth stage: how many simulated vectors of
// each category group are stored.
type Stage struct {
	Launch, WaveA, WaveB int
}

// Total is the number of simulated vectors stored.
func (s Stage) Total() int { return s.Launch + s.WaveA + s.WaveB }

// Products is the catalogue size the stage stands for.
func (s Stage) Products() int { return s.Total() * ProductsPerVector }

// Stages is the catalogue's growth: first more of the launch categories,
// then a first wave of new categories, then a second.
var Stages = []Stage{
	{500, 0, 0},
	{1250, 0, 0},
	{2500, 0, 0},
	{2500, 1250, 0},
	{2500, 2500, 0},
	{3000, 2500, 750},
}

// ModelledLatency prices one query: each vector scanned stands for
// ProductsPerVector products, and every product and centroid costs ModelDim
// multiply-adds. Centroid comparisons are not scaled (see README).
func ModelledLatency(vectorsScanned, centroidsCompared int) time.Duration {
	mults := float64(vectorsScanned*ProductsPerVector+centroidsCompared) * ModelDim
	return time.Duration(math.Round(mults * NsPerMultiply))
}

// StageResult is what one growth stage measured.
type StageResult struct {
	Products int
	// Index describes what served the stage's queries.
	Index string
	// NProbe is the IVF nprobe used (0 for flat).
	NProbe int
	// ScannedPerQuery is the mean number of simulated vectors scored per
	// query, and ScannedFraction that as a share of the corpus.
	ScannedPerQuery float64
	ScannedFraction float64
	// P99 is the modelled 99th-percentile latency over the stage's queries.
	P99 time.Duration
	// RecallLaunch and RecallNew are mean recall@10 against exact search,
	// for queries on launch categories and on categories added later.
	RecallLaunch, RecallNew float64
	HasNew                  bool
	// Drift is the drift ratio the fixed design measured (0 if not measured).
	Drift float64
	// Retrained says why the index was retrained at this stage ("" if not).
	Retrained string
	// Retrains is the cumulative number of retrains (initial training
	// excluded).
	Retrains int
}

// Holds reports whether the stage met the invariant.
func (s StageResult) Holds() bool {
	return s.P99 <= SLO && s.RecallLaunch >= RecallTarget && (!s.HasNew || s.RecallNew >= RecallTarget)
}

// Report is what a run measured.
type Report struct {
	Name   string
	Stages []StageResult
	// Trainings counts every k-means run, including the first.
	Trainings int
	// TrainDistanceComputations totals vector-to-centroid distances over all
	// trainings (seeding, Lloyd iterations and re-filing).
	TrainDistanceComputations int64
	// CentroidBytes and CorpusBytes are the simulated index's float32
	// centroid table and stored vectors at the last stage.
	CentroidBytes, CorpusBytes int
	// RescueNProbe is, for the broken design only, the nprobe the stale
	// index would need at the last stage to reach TuneTarget without
	// retraining, with the p99 and new-category recall it would give.
	RescueNProbe    int
	RescueP99       time.Duration
	RescueRecallNew float64
}

// Violations counts stages that broke the SLO and stages that missed the
// recall target.
func (r Report) Violations() (slo, recall int) {
	for _, s := range r.Stages {
		if s.P99 > SLO {
			slo++
		}
		if s.RecallLaunch < RecallTarget || (s.HasNew && s.RecallNew < RecallTarget) {
			recall++
		}
	}
	return slo, recall
}

// Print writes the per-stage table and a summary.
func (r Report) Print(w io.Writer) {
	fmt.Fprintf(w, "%s\n", r.Name)
	fmt.Fprintf(w, "Cost model: (vectors scanned x %d products + centroids compared) x %d dims x %.1f ns. SLO: p99 <= %v, recall@%d >= %.2f.\n\n",
		ProductsPerVector, ModelDim, NsPerMultiply, SLO, K, RecallTarget)
	fmt.Fprintln(w, "  stage  products  index           scanned/query    p99 latency  recall launch  recall new  retrains")
	for i, s := range r.Stages {
		newR := "      -"
		if s.HasNew {
			newR = fmt.Sprintf("%7.3f", s.RecallNew)
		}
		mark := ""
		if !s.Holds() {
			mark = "  <- violates"
		}
		fmt.Fprintf(w, "  %5d  %8s  %-14s  %6.0f (%5.1f%%)  %11s  %13.3f  %10s  %8d%s\n",
			i+1, products(s.Products), s.Index, s.ScannedPerQuery, 100*s.ScannedFraction,
			s.P99.Round(100*time.Microsecond), s.RecallLaunch, newR, s.Retrains, mark)
	}
	var notes []string
	for i, s := range r.Stages {
		if s.Retrained != "" {
			notes = append(notes, fmt.Sprintf("  stage %d: retrained (%s)", i+1, s.Retrained))
		} else if s.Drift > 0 {
			notes = append(notes, fmt.Sprintf("  stage %d: no retrain (drift ratio %.2f)", i+1, s.Drift))
		}
	}
	if len(notes) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, strings.Join(notes, "\n"))
	}
	slo, recall := r.Violations()
	fmt.Fprintf(w, "\n  stages over the latency SLO: %d of %d; stages under the recall target: %d of %d\n",
		slo, len(r.Stages), recall, len(r.Stages))
	fmt.Fprintf(w, "  k-means trainings: %d, %d distance computations in total\n", r.Trainings, r.TrainDistanceComputations)
	if r.CentroidBytes > 0 {
		fmt.Fprintf(w, "  centroid table: %d bytes, %.1f%% of the %d bytes of stored vectors\n",
			r.CentroidBytes, 100*float64(r.CentroidBytes)/float64(r.CorpusBytes), r.CorpusBytes)
	}
	if r.RescueNProbe > 0 {
		fmt.Fprintf(w, "  raising nprobe instead of retraining: stage %d needs nprobe=%d for recall new %.3f, at p99 %v\n",
			len(r.Stages), r.RescueNProbe, r.RescueRecallNew, r.RescueP99.Round(100*time.Microsecond))
	}
}

func products(n int) string {
	if n >= 1_000_000 {
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	}
	return fmt.Sprintf("%dk", n/1000)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// world is the data both designs share: the vector pools, the growing exact
// index (ground truth, and the broken design's search), and the queries.
type world struct {
	launch, waveA, waveB [][]float32
	flat                 *vectorannindex.Flat
	stored               map[int][]float32 // id -> unit vector
	order                []int             // ids in the order they were added
	stage                int
}

func newWorld() *world {
	last := Stages[len(Stages)-1]
	f, err := vectorannindex.NewFlat(Dim, vectorannindex.Cosine)
	must(err)
	return &world{
		launch: vectorannindex.Clustered(rand.New(rand.NewPCG(1, 1)), last.Launch, Dim, LaunchTopics, Spread),
		waveA:  vectorannindex.Clustered(rand.New(rand.NewPCG(2, 2)), last.WaveA, Dim, WaveATopics, Spread),
		waveB:  vectorannindex.Clustered(rand.New(rand.NewPCG(3, 3)), last.WaveB, Dim, WaveBTopics, Spread),
		flat:   f,
		stored: map[int][]float32{},
	}
}

// IDs: launch vectors 0.., wave A from 1e6, wave B from 2e6.
const waveAID, waveBID = 1_000_000, 2_000_000

type item struct {
	id int
	v  []float32
}

// grow adds the stage's new vectors to the flat index and returns them.
func (w *world) grow(i int) []item {
	var prev Stage
	if i > 0 {
		prev = Stages[i-1]
	}
	cur := Stages[i]
	var added []item
	for j := prev.Launch; j < cur.Launch; j++ {
		added = append(added, item{j, w.launch[j]})
	}
	for j := prev.WaveA; j < cur.WaveA; j++ {
		added = append(added, item{waveAID + j, w.waveA[j]})
	}
	for j := prev.WaveB; j < cur.WaveB; j++ {
		added = append(added, item{waveBID + j, w.waveB[j]})
	}
	for _, it := range added {
		must(w.flat.Add(it.id, it.v))
		w.stored[it.id] = unit(it.v)
		w.order = append(w.order, it.id)
	}
	w.stage = i
	return added
}

// querySet is a group of queries and their exact top-K.
type querySet struct {
	qs    [][]float32
	truth [][]vectorannindex.Result
}

// stageQueries are one stage's query sets: evaluation queries (served and
// reported) and separate tuning queries (used to choose nprobe), each for
// the launch categories and the categories added later.
type stageQueries struct {
	evalLaunch, evalNew, tuneLaunch, tuneNew querySet
}

var (
	sharedOnce sync.Once
	sharedQs   []stageQueries
)

// queries returns the current stage's query sets. They depend only on the
// seeded data, so they are computed once (with exact answers from a flat
// index) and shared by both designs: identical traffic, and the expensive
// ground truth is not paid twice.
func (w *world) queries() stageQueries {
	sharedOnce.Do(func() {
		tw := newWorld()
		for i := range Stages {
			tw.grow(i)
			sharedQs = append(sharedQs, stageQueries{
				evalLaunch: tw.draw(false, EvalQueries, purposeEval),
				evalNew:    tw.draw(true, EvalQueries, purposeEval),
				tuneLaunch: tw.draw(false, TuneQueries, purposeTune),
				tuneNew:    tw.draw(true, TuneQueries, purposeTune),
			})
		}
	})
	return sharedQs[w.stage]
}

// draw draws n queries near stored vectors of the launch categories
// (newTopics false) or of the categories added later, with exact answers.
func (w *world) draw(newTopics bool, n int, purpose uint64) querySet {
	cur := Stages[w.stage]
	var pool [][]float32
	if newTopics {
		pool = append(append(pool, w.waveA[:cur.WaveA]...), w.waveB[:cur.WaveB]...)
	} else {
		pool = w.launch[:cur.Launch]
	}
	if len(pool) == 0 {
		return querySet{}
	}
	kind := uint64(0)
	if newTopics {
		kind = 1
	}
	rng := rand.New(rand.NewPCG(uint64(w.stage)*100+kind*10+purpose, 2026))
	var s querySet
	for range n {
		q := vectorannindex.Jitter(rng, pool[rng.IntN(len(pool))], QueryNoise)
		t, err := w.flat.Search(q, K)
		must(err)
		s.qs = append(s.qs, q)
		s.truth = append(s.truth, t)
	}
	return s
}

const (
	purposeEval = 1
	purposeTune = 2
)

// served is the outcome of running a query set against one design.
type served struct {
	recall    float64
	latencies []time.Duration
	scanned   int
}

func serveFlat(w *world, s querySet) served {
	out := served{recall: 1}
	for range s.qs {
		out.latencies = append(out.latencies, ModelledLatency(w.flat.Len(), 0))
		out.scanned += w.flat.Len()
	}
	return out
}

func serveIVF(ivf *vectorannindex.IVF, nprobe int, s querySet) served {
	var out served
	for i, q := range s.qs {
		got, st, err := ivf.Search(q, K, nprobe)
		must(err)
		out.recall += vectorannindex.Recall(got, s.truth[i])
		out.latencies = append(out.latencies, ModelledLatency(st.VectorsScanned, st.CentroidsCompared))
		out.scanned += st.VectorsScanned
	}
	if len(s.qs) > 0 {
		out.recall /= float64(len(s.qs))
	}
	return out
}

// p99 is the nearest-rank 99th percentile.
func p99(ls []time.Duration) time.Duration {
	s := append([]time.Duration(nil), ls...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[int(math.Ceil(0.99*float64(len(s))))-1]
}

func result(w *world, index string, nprobe int, launch, fresh served, hasNew bool) StageResult {
	lat := append(append([]time.Duration(nil), launch.latencies...), fresh.latencies...)
	n := len(lat)
	r := StageResult{
		Products:        Stages[w.stage].Products(),
		Index:           index,
		NProbe:          nprobe,
		ScannedPerQuery: float64(launch.scanned+fresh.scanned) / float64(n),
		P99:             p99(lat),
		RecallLaunch:    launch.recall,
		RecallNew:       fresh.recall,
		HasNew:          hasNew,
	}
	r.ScannedFraction = r.ScannedPerQuery / float64(Stages[w.stage].Total())
	return r
}

// nprobeCandidates are the values tuning tries, cheapest first.
var nprobeCandidates = []int{1, 2, 3, 4, 5, 6, 8, 10, 12, 16, 20, 24, 32, 48, NList}

// tuneNProbe returns the smallest candidate nprobe at which every non-empty
// tuning set reaches TuneTarget.
func tuneNProbe(ivf *vectorannindex.IVF, sets ...querySet) int {
	for _, np := range nprobeCandidates {
		ok := true
		for _, s := range sets {
			if len(s.qs) > 0 && serveIVF(ivf, np, s).recall < TuneTarget {
				ok = false
				break
			}
		}
		if ok {
			return np
		}
	}
	return NList
}

func newIVF() *vectorannindex.IVF {
	ivf, err := vectorannindex.NewIVF(Dim, NList, vectorannindex.Cosine,
		vectorannindex.WithSeed(5, 6), vectorannindex.WithMaxIterations(MaxIterations))
	must(err)
	return ivf
}

// trainOn trains an empty IVF on a seeded random sample of the stored
// vectors and adds them all. (A strided sample would alias with the
// round-robin category layout of the synthetic data and skip whole
// categories.)
func trainOn(w *world, ivf *vectorannindex.IVF) vectorannindex.TrainStats {
	ids := w.order
	n := min(TrainSample, len(ids))
	perm := rand.New(rand.NewPCG(7, uint64(len(ids)))).Perm(len(ids))
	sample := make([][]float32, n)
	for i := range sample {
		sample[i] = w.stored[ids[perm[i]]]
	}
	ts, err := ivf.Train(sample)
	must(err)
	for _, id := range ids {
		must(ivf.Add(id, w.stored[id]))
	}
	ts.DistanceComputations += int64(len(ids)) * NList // filing every vector
	return ts
}

func unit(v []float32) []float32 {
	var s float64
	for _, x := range v {
		s += float64(x) * float64(x)
	}
	n := math.Sqrt(s)
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = float32(float64(x) / n)
	}
	return out
}

func cosDist(a, b []float32) float64 {
	var d float64
	for i := range a {
		d += float64(a[i]) * float64(b[i])
	}
	return 1 - d
}

// driftMonitor is the fixed design's retrain trigger. It is built from the
// index's public API only: after each training it records the mean (unit)
// vector of each list's members (ListOf) and how far, on average, members
// sit from it. New vectors are checked against the centre of the list they
// were filed in (NearestList).
type driftMonitor struct {
	centres  [][]float32
	baseline float64
	trainedN int
}

func (d *driftMonitor) snapshot(w *world, ivf *vectorannindex.IVF) {
	sums := make([][]float64, NList)
	for i := range sums {
		sums[i] = make([]float64, Dim)
	}
	for _, id := range w.order {
		v := w.stored[id]
		l, ok := ivf.ListOf(id)
		if !ok {
			panic("slowvectorsearch: stored vector missing from index")
		}
		for j, x := range v {
			sums[l][j] += float64(x)
		}
	}
	d.centres = make([][]float32, NList)
	for l, s := range sums {
		c := make([]float32, Dim)
		zero := true
		for j, x := range s {
			c[j] = float32(x)
			zero = zero && x == 0
		}
		if !zero {
			c = unit(c)
		}
		d.centres[l] = c
	}
	var total float64
	for _, id := range w.order {
		l, _ := ivf.ListOf(id)
		total += cosDist(w.stored[id], d.centres[l])
	}
	d.baseline = total / float64(len(w.stored))
	d.trainedN = len(w.stored)
}

// ratio is the mean distance of added vectors to the centre of the list they
// were filed in, over the training baseline.
func (d *driftMonitor) ratio(ivf *vectorannindex.IVF, added []item) float64 {
	var total float64
	for _, it := range added {
		l, err := ivf.NearestList(it.v)
		must(err)
		total += cosDist(unit(it.v), d.centres[l])
	}
	return total / float64(len(added)) / d.baseline
}

// Broken: flat search until it breaches the SLO, then an IVF index trained
// once on the catalogue of that day, with nprobe tuned once, never revisited.
func Broken() Report {
	r := Report{Name: "BROKEN: flat search, then an IVF index trained once and never retrained"}
	w := newWorld()
	var ivf *vectorannindex.IVF
	var nprobe int
	for i := range Stages {
		added := w.grow(i)
		q := w.queries()
		launch, fresh := q.evalLaunch, q.evalNew
		hasNew := len(fresh.qs) > 0
		var sr StageResult
		if ivf == nil {
			sr = result(w, "flat", 0, serveFlat(w, launch), serveFlat(w, fresh), hasNew)
		} else {
			for _, it := range added {
				must(ivf.Add(it.id, it.v))
			}
			sr = result(w, fmt.Sprintf("ivf nprobe=%d", nprobe), nprobe, serveIVF(ivf, nprobe, launch), serveIVF(ivf, nprobe, fresh), hasNew)
		}
		r.Stages = append(r.Stages, sr)
		if i == BrokenIVFAfter {
			// The team ships an IVF index after the search page starts timing out.
			ivf = newIVF()
			ts := trainOn(w, ivf)
			r.Trainings++
			r.TrainDistanceComputations += ts.DistanceComputations
			q := w.queries()
			nprobe = tuneNProbe(ivf, q.tuneLaunch, q.tuneNew)
		}
	}
	if ivf != nil {
		r.CentroidBytes = NList * Dim * 4
		r.CorpusBytes = ivf.Len() * Dim * 4
		// What it would take to win recall back without retraining: re-tune
		// nprobe on the stale centroids at the last stage.
		q := w.queries()
		r.RescueNProbe = tuneNProbe(ivf, q.tuneLaunch, q.tuneNew)
		rs := result(w, "", r.RescueNProbe, serveIVF(ivf, r.RescueNProbe, q.evalLaunch), serveIVF(ivf, r.RescueNProbe, q.evalNew), true)
		r.RescueP99, r.RescueRecallNew = rs.P99, rs.RecallNew
	}
	return r
}

// Fixed: an IVF index from launch; at every stage the drift monitor decides
// whether to retrain, and nprobe is re-tuned to the lowest value that meets
// the recall target on held-out tuning queries.
func Fixed() Report {
	r := Report{Name: "FIXED: IVF with nprobe tuned to a recall target + drift-triggered retraining"}
	w := newWorld()
	ivf := newIVF()
	var mon driftMonitor
	retrains := 0
	for i := range Stages {
		added := w.grow(i)
		var reason string
		var drift float64
		if i == 0 {
			ts := trainOn(w, ivf)
			r.Trainings++
			r.TrainDistanceComputations += ts.DistanceComputations
		} else {
			for _, it := range added {
				must(ivf.Add(it.id, it.v))
			}
			drift = mon.ratio(ivf, added)
			growth := float64(ivf.Len())/float64(mon.trainedN) - 1
			switch {
			case growth >= GrowthTrigger:
				reason = fmt.Sprintf("corpus grew %.0f%% since training", 100*growth)
			case drift >= DriftTrigger:
				reason = fmt.Sprintf("drift ratio %.2f", drift)
			}
			if reason != "" {
				ts, err := ivf.Retrain(TrainSample)
				must(err)
				retrains++
				r.Trainings++
				r.TrainDistanceComputations += ts.DistanceComputations
			}
		}
		if i == 0 || reason != "" {
			mon.snapshot(w, ivf)
		}
		q := w.queries()
		nprobe := tuneNProbe(ivf, q.tuneLaunch, q.tuneNew)
		sr := result(w, fmt.Sprintf("ivf nprobe=%d", nprobe), nprobe, serveIVF(ivf, nprobe, q.evalLaunch), serveIVF(ivf, nprobe, q.evalNew), len(q.evalNew.qs) > 0)
		sr.Drift, sr.Retrained, sr.Retrains = drift, reason, retrains
		r.Stages = append(r.Stages, sr)
	}
	r.CentroidBytes = NList * Dim * 4
	r.CorpusBytes = ivf.Len() * Dim * 4
	return r
}
