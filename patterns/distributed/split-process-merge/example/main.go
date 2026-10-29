// Command example encodes a simulated two-hour upload by splitting it into
// 4-second segments, fanning the segments out to a worker pool and merging
// them in order. It prints how wall time scales with workers and where it
// flattens, what speculative backups and a streaming merge do to the tail,
// what a worker crash and a coordinator crash cost with and without a
// checkpoint, and finally runs a small job for real with goroutines.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	spm "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/split-process-merge"
)

func must[T any](v T, err error) T {
	if err != nil {
		log.Fatal(err)
	}
	return v
}

func d(t time.Duration) string { return t.Round(time.Second).String() }

func countDone(done []bool) int {
	n := 0
	for _, x := range done {
		if x {
			n++
		}
	}
	return n
}

func main() {
	// 1) Split.
	chunks := must(spm.SplitFixed(120*time.Minute, 4*time.Second))
	fmt.Printf("1) Split a 120-minute upload into %d fixed 4s chunks.\n", len(chunks))
	kf := []time.Duration{0, 2 * time.Second, 4500 * time.Millisecond, 5 * time.Second, 9 * time.Second,
		11 * time.Second, 12 * time.Second, 13 * time.Second, 17 * time.Second, 21 * time.Second}
	kchunks := must(spm.SplitAtKeyframes(24*time.Second, kf, 4*time.Second))
	var parts []string
	for _, c := range kchunks {
		parts = append(parts, fmt.Sprintf("[%v-%v)", c.Start, c.End))
	}
	fmt.Printf("   Splitting the first 24s at its keyframes instead (target 4s) gives %d uneven chunks:\n   %s\n",
		len(kchunks), strings.Join(parts, " "))
	fmt.Println("   Each one starts on a keyframe, so it decodes without its neighbour.")
	fmt.Println()

	base := spm.SimConfig{
		Chunks:             chunks,
		CostPerMediaSecond: 3, // encode every resolution: 3s of work per second of video
		ChunkOverhead:      time.Second,
		Jitter:             0.25,
		StragglerRate:      0.005, // 1 attempt in 200 runs 20x slower
		StragglerFactor:    20,
		MergePerChunk:      100 * time.Millisecond,
		DetectAfter:        30 * time.Second,
		Seed:               42,
	}
	sim := func(mod func(*spm.SimConfig)) spm.SimReport {
		c := base
		mod(&c)
		return must(spm.Simulate(c))
	}
	serial := spm.SerialTime(base)

	// 2) Scale out.
	fmt.Println("2) Scale out (no speculation, merge after the last chunk). Serial = one encoder, whole file.")
	fmt.Printf("   %-8s %10s %8s %11s %8s %10s\n", "workers", "wall time", "speedup", "processing", "merge", "efficiency")
	fmt.Printf("   %-8s %10s %7.1fx %11s %8s %10s\n", "serial", d(serial), 1.0, d(serial), "-", "-")
	for _, n := range []int{1, 4, 16, 64, 256} {
		r := sim(func(c *spm.SimConfig) { c.Workers = n })
		sp := float64(serial) / float64(r.Makespan)
		fmt.Printf("   %-8d %10s %7.1fx %11s %8s %9.0f%%\n", n, d(r.Makespan), sp, d(r.ProcessingEnd), d(r.MergeTail), 100*sp/float64(n))
	}
	fmt.Println("   N=1 is slower than serial: per-chunk overhead plus stragglers. The curve flattens past 64")
	fmt.Println("   workers because two things don't parallelise: the 3-minute serial merge, and the slowest")
	fmt.Println("   chunk (a 20x straggler of a 13s chunk takes ~4 minutes however many workers sit idle).")
	fmt.Println()

	// 3) Trim the tail.
	fmt.Println("3) Trim the tail: speculative backups (first result wins) and a streaming merge.")
	fmt.Printf("   %-8s %-18s %10s %11s %8s %5s %12s\n", "workers", "variant", "wall time", "processing", "backups", "wins", "worker time")
	for _, n := range []int{64, 256} {
		for _, v := range []struct {
			name            string
			spec, streaming bool
		}{{"baseline", false, false}, {"+ speculation", true, false}, {"+ streaming merge", false, true}, {"+ both", true, true}} {
			r := sim(func(c *spm.SimConfig) { c.Workers, c.Speculate, c.StreamingMerge = n, v.spec, v.streaming })
			fmt.Printf("   %-8d %-18s %10s %11s %8d %5d %12s\n", n, v.name, d(r.Makespan), d(r.ProcessingEnd),
				r.Stats.Backups, r.Stats.BackupWins, d(r.Busy))
		}
	}
	fmt.Println("   Backups start only once the queue is empty, on the longest-running chunks. Most lose (their")
	fmt.Println("   original was a normal chunk); the wins that matter are on the 20x stragglers. Worker time even")
	fmt.Println("   drops: a straggler stops burning its worker the moment its backup wins and it is cancelled.")
	fmt.Println("   A streaming merge appends chunk k as soon as chunks 0..k are done, so little merge work is")
	fmt.Println("   left at the end; the floor becomes the 3-minute merge itself.")
	fmt.Println()

	// 4) Worker crash.
	fmt.Println("4) Workers crash mid-job (16 workers, a crash is noticed when its 30s lease expires).")
	ok16 := sim(func(c *spm.SimConfig) { c.Workers = 16 })
	fmt.Printf("   %-36s %10s %6s %7s %12s\n", "case", "wall time", "lost", "reruns", "wasted work")
	fmt.Printf("   %-36s %10s %6d %7d %12s\n", "no crash", d(ok16.Makespan), 0, 0, d(ok16.Wasted))
	for _, k := range []int{1, 4} {
		var crashes []spm.WorkerCrash
		for w := range k {
			crashes = append(crashes, spm.WorkerCrash{Worker: w, At: 10*time.Minute + time.Duration(w)*time.Minute})
		}
		r := sim(func(c *spm.SimConfig) { c.Workers, c.Crashes = 16, crashes })
		fmt.Printf("   %-36s %10s %6d %7d %12s\n", fmt.Sprintf("%d worker(s) die from 10 min on", k), d(r.Makespan), r.Stats.Lost, r.Stats.Reruns, d(r.Wasted))
	}
	fmt.Printf("   %-36s %10s %6s %7s %12s\n", "unsplit job: its one encoder dies", d(10*time.Minute+serial), "all", "-", d(10*time.Minute))
	fmt.Println("   Only the chunk each dead worker held is redone; the smaller pool then takes a little longer.")
	fmt.Println()

	// 5) Coordinator crash and resume.
	fmt.Println("5) The whole job crashes at 20 min (16 workers) and is restarted.")
	first := sim(func(c *spm.SimConfig) { c.Workers, c.StopAt = 16, 20*time.Minute })
	doneBefore := countDone(first.Done)
	resumed := sim(func(c *spm.SimConfig) { c.Workers, c.Resume = 16, first.Done })
	fmt.Printf("   at the crash: %d/%d chunks checkpointed, %d in flight (their partial work is lost)\n", doneBefore, len(chunks), first.InFlight)
	fmt.Printf("   %-24s %15s %14s %16s\n", "restart strategy", "chunks encoded", "chunks redone", "total wall time")
	fmt.Printf("   %-24s %15d %14d %16s\n", "resume from checkpoint", resumed.Stats.Attempts, first.InFlight, d(20*time.Minute+resumed.Makespan))
	fmt.Printf("   %-24s %15d %14d %16s\n", "restart from zero", ok16.Stats.Attempts, doneBefore+first.InFlight, d(20*time.Minute+ok16.Makespan))
	fmt.Println()

	// 6) Deadline.
	dl := sim(func(c *spm.SimConfig) { c.Workers, c.StopAt = 4, 15*time.Minute })
	fmt.Printf("6) A 15-minute job deadline with only 4 workers: stopped=%v, %d/%d chunks checkpointed,\n",
		dl.Stopped, countDone(dl.Done), len(chunks))
	fmt.Printf("   %d in-flight attempts cancelled. A later run resumes from the checkpoint.\n", dl.InFlight)
	fmt.Println()

	// 7) For real.
	fmt.Println("7) Run for real with goroutines: 120 chunks, 8 workers, chunk 100 fails twice, cancel once 50 are merged.")
	small := chunks[:120]
	cp := spm.NewMemoryCheckpoint[string]()
	var calls atomic.Int64
	var mu sync.Mutex
	failures := 0
	process := func(ctx context.Context, a spm.Attempt) (string, error) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		calls.Add(1)
		if a.Chunk.Index == 100 {
			mu.Lock()
			defer mu.Unlock()
			if failures < 2 {
				failures++
				return "", errors.New("encoder crashed")
			}
		}
		return fmt.Sprintf("seg%03d", a.Chunk.Index), nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	merged := 0
	_, err := spm.Run(ctx, small, process, spm.Config[string]{Workers: 8, Checkpoint: cp,
		OnMerge: func(spm.Chunk, string) error {
			if merged++; merged == 50 {
				cancel()
			}
			return nil
		}})
	firstCalls := calls.Load()
	fmt.Printf("   first run: %v\n", err)
	fmt.Printf("     %d process calls; the checkpoint holds %d chunks (counts vary between runs: real goroutines)\n", firstCalls, cp.Len())
	var out []string
	rep, err := spm.Run(context.Background(), small, process, spm.Config[string]{Workers: 8, Checkpoint: cp,
		OnMerge: func(_ spm.Chunk, r string) error { out = append(out, r); return nil }})
	if err != nil {
		log.Fatal(err)
	}
	missing := len(small) - rep.Stats.Resumed
	second := calls.Load() - firstCalls
	fmt.Printf("   resume: %d chunks skipped (checkpointed), %d process calls = %d missing chunks + %d failed attempts\n",
		rep.Stats.Resumed, second, missing, rep.Stats.Failures)
	fmt.Printf("     merged %d chunks in index order: %s ... %s\n", len(out), strings.Join(out[:3], " "), strings.Join(out[len(out)-2:], " "))
	if int(second) != missing+rep.Stats.Failures {
		log.Fatal("resume redid chunks that were already checkpointed")
	}
}
