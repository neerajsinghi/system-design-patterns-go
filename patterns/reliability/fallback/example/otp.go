package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/fallback"
)

// Simulated traffic and outages for step 2. Everything runs on a fake clock.
const (
	otpRequests  = 1200                   // one OTP request ...
	otpInterval  = 500 * time.Millisecond // ... every 500ms: 10 simulated minutes
	invalidEvery = 40                     // every 40th number is malformed

	smsATimeout  = 3 * time.Second
	smsBTimeout  = 3 * time.Second
	voiceTimeout = 5 * time.Second
	otpDeadline  = 10 * time.Second // the user is staring at "Sending code..."
)

var (
	smsADown = [2]time.Duration{1 * time.Minute, 7 * time.Minute} // primary outage
	smsBDown = [2]time.Duration{3 * time.Minute, 5 * time.Minute} // secondary fails too

	errInvalidNumber = errors.New("invalid phone number")
)

// simProvider is a fake SMS/voice provider on the shared simulated clock.
type simProvider struct {
	name    string
	latency time.Duration // when healthy
	timeout time.Duration // a down provider hangs until this fires
	cents   float64       // price per delivered message, in cents
	down    [2]time.Duration
}

type otpReq struct {
	to    string
	valid bool
}

type otpStats struct {
	valid, delivered, failed     int
	invalid, invalidReachedVoice int
	failoverAfterPermanent       int // attempts made after a non-retryable error: must be 0
	servedBy                     map[string]int
	wastedWait                   time.Duration // time spent on attempts that failed
	totalLatency, maxLatency     time.Duration
	cents                        float64
}

func runOTP(label string, providers []simProvider, opt fallback.ChainOptions) otpStats {
	start := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	now := start
	clock := func() time.Time { return now }
	opt.Now = clock

	price := map[string]float64{}
	var chainProviders []fallback.Provider[otpReq, string]
	for _, p := range providers {
		price[p.name] = p.cents
		chainProviders = append(chainProviders, fallback.Provider[otpReq, string]{
			Name:    p.name,
			Timeout: p.timeout,
			Call: func(_ context.Context, r otpReq) (string, error) {
				at := now.Sub(start)
				if at >= p.down[0] && at < p.down[1] {
					now = now.Add(p.timeout) // hangs until the per-provider timeout
					return "", context.DeadlineExceeded
				}
				if !r.valid {
					now = now.Add(150 * time.Millisecond) // rejected at validation
					return "", fallback.Permanent(errInvalidNumber)
				}
				now = now.Add(p.latency)
				return "msg-id", nil
			},
		})
	}
	chain, err := fallback.NewChain(chainProviders, opt)
	if err != nil {
		panic(err)
	}

	st := otpStats{servedBy: map[string]int{}}
	for i := range otpRequests {
		arrival := start.Add(time.Duration(i) * otpInterval)
		now = arrival
		req := otpReq{to: fmt.Sprintf("+1555%07d", i), valid: i%invalidEvery != invalidEvery-1}
		res, err := chain.Do(context.Background(), req)
		lat := now.Sub(arrival)
		st.totalLatency += lat
		st.maxLatency = max(st.maxLatency, lat)
		sawPermanent := false
		for _, a := range res.Attempts {
			if sawPermanent {
				st.failoverAfterPermanent++
			}
			if fallback.IsPermanent(a.Err) {
				sawPermanent = true
			} else if a.Err != nil {
				st.wastedWait += a.Took
			}
			if !req.valid && !a.Skipped && a.Provider == "voice" {
				st.invalidReachedVoice++
			}
		}
		switch {
		case !req.valid:
			st.invalid++
		case err == nil:
			st.valid++
			st.delivered++
			st.servedBy[res.Provider]++
			st.cents += price[res.Provider]
		default:
			st.valid++
			st.failed++
		}
	}

	valid := st.valid
	fmt.Printf("%-24s delivered %4d/%d valid (%5.1f%% failed)  served a/b/voice %4d/%3d/%3d  avg %5v  max %5v  waited on dead providers %6v  cost $%.2f\n",
		label, st.delivered, valid, 100*float64(st.failed)/float64(valid),
		st.servedBy["sms-a"], st.servedBy["sms-b"], st.servedBy["voice"],
		(st.totalLatency / otpRequests).Round(time.Millisecond), st.maxLatency,
		st.wastedWait.Round(time.Second), st.cents/100)
	return st
}

func otpFailover() {
	smsA := simProvider{name: "sms-a", latency: 200 * time.Millisecond, timeout: smsATimeout, cents: 0.5, down: smsADown}
	smsB := simProvider{name: "sms-b", latency: 350 * time.Millisecond, timeout: smsBTimeout, cents: 0.7, down: smsBDown}
	voice := simProvider{name: "voice", latency: 1500 * time.Millisecond, timeout: voiceTimeout, cents: 3.0}

	fmt.Printf("%d OTPs, one every %v; sms-a down %v-%v, sms-b down %v-%v; every %dth number is invalid.\n",
		otpRequests, otpInterval, smsADown[0], smsADown[1], smsBDown[0], smsBDown[1], invalidEvery)
	fmt.Printf("Timeouts: sms-a %v, sms-b %v, voice %v; overall deadline %v. Price per OTP: 0.5c / 0.7c / 3.0c.\n\n",
		smsATimeout, smsBTimeout, voiceTimeout, otpDeadline)

	only := runOTP("1. primary only", []simProvider{smsA}, fallback.ChainOptions{Deadline: otpDeadline, FailureThreshold: -1})
	noCool := runOTP("2. chain, no cooldown", []simProvider{smsA, smsB, voice}, fallback.ChainOptions{Deadline: otpDeadline, FailureThreshold: -1})
	cool := runOTP("3. chain + 30s cooldown", []simProvider{smsA, smsB, voice}, fallback.ChainOptions{Deadline: otpDeadline, FailureThreshold: 3, Cooldown: 30 * time.Second})

	fmt.Println()
	fmt.Printf("Primary only: every OTP in sms-a's 6-minute outage fails (%d of them) and each one waits the full %v.\n", only.failed, smsATimeout)
	fmt.Printf("The chain delivers all of them: sms-b while it is up, voice while both SMS providers are down.\n")
	fmt.Printf("Without cooldown every request still pays sms-a's %v timeout first (plus sms-b's while it is down): avg %v, max %v.\n",
		smsATimeout, (noCool.totalLatency / otpRequests).Round(time.Millisecond), noCool.maxLatency)
	fmt.Printf("With cooldown sms-a is skipped after 3 failures and probed once per 30s: avg %v, wasted wait %v instead of %v.\n",
		(cool.totalLatency / otpRequests).Round(time.Millisecond), cool.wastedWait.Round(time.Second), noCool.wastedWait.Round(time.Second))
	fmt.Printf("The cooldown's price: a recovered provider is only noticed at its next probe, so up to 30s of traffic\n")
	fmt.Printf("stays on the pricier fallbacks after each recovery (voice %d vs %d, cost $%.2f vs $%.2f).\n",
		cool.servedBy["voice"], noCool.servedBy["voice"], cool.cents/100, noCool.cents/100)
	fmt.Printf("Invalid numbers: %d; attempts made after a provider said \"invalid number\": %d (non-retryable errors stop the chain).\n",
		cool.invalid, cool.failoverAfterPermanent)
	fmt.Printf("  %d of them still reached voice: they arrived while both SMS providers were down, so nobody had rejected them yet.\n",
		cool.invalidReachedVoice)
	fmt.Printf("Cost of staying up: $%.2f vs $%.2f if sms-a had never failed, because sms-b and voice are pricier.\n",
		cool.cents/100, float64(cool.delivered)*smsA.cents/100)
}
