// Package duplicatechat simulates the "chat app shows the same message twice"
// incident. Alice is on a flaky mobile network. She sends a message, the
// server stores it, and the server's ack is lost on the way back. Her app
// can't tell "the server never got it" from "the reply got lost", so it
// sends again, and Bob sees the message twice, or three times. The second
// leg has the same problem: the server pushes the message to Bob's phone,
// Bob's ack is lost, and the server pushes it again.
//
// Broken: the client sends the text with no ID of its own, the server gives
// every attempt a fresh server ID, and both the server and Bob's phone
// append whatever arrives. Both sides retry three times, a second apart, and
// then give up, so a message sent while a phone is in a dead zone is lost.
//
// Fixed: the client gives each message a Snowflake ID once, before the first
// attempt, and keeps it on every retransmission. The server checks that ID
// against an inbox (a dedup store with a retention window) before it stores
// and fans out the message, and acks duplicates without storing them again.
// Bob's phone keeps its own inbox of IDs it has shown, so a repeated push is
// dropped. Both legs retry with capped exponential backoff and jitter, which
// spreads attempts past a dead zone instead of burning them inside it. Bob's
// conversation is sorted by Snowflake ID, so a late retransmission appears
// in the place it was sent, not where it arrived.
package duplicatechat

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"strconv"
	"time"

	snowflakeid "github.com/neerajsinghi/system-design-patterns-go/patterns/data/snowflake-id"
	idempotentconsumer "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/idempotent-consumer"
	retrybackoff "github.com/neerajsinghi/system-design-patterns-go/patterns/reliability/retry-backoff"
)

const (
	Messages    = 300                    // messages Alice sends to Bob
	MinGap      = 400 * time.Millisecond // shortest pause between two of Alice's messages
	MaxGap      = 1600 * time.Millisecond
	Latency     = 60 * time.Millisecond // one-way network latency on either phone's link
	AckTimeout  = time.Second           // how long a sender waits for an ack before it retries
	RequestLoss = 0.05                  // chance an attempt never reaches the other side
	AckLoss     = 0.15                  // chance it arrives but the ack is lost
	DeadZones   = 4                     // per phone: lifts, tunnels, a train between stations
	MinDead     = 4 * time.Second
	MaxDead     = 15 * time.Second

	BrokenAttempts = 3 // broken: three tries, one ack timeout apart, then give up

	FixedAttempts = 10 // fixed: ten tries with capped exponential backoff
	BaseDelay     = 500 * time.Millisecond
	MaxDelay      = 16 * time.Second

	// DedupWindow is how long the server remembers a message ID. It must be
	// longer than the longest time a client can keep retransmitting.
	DedupWindow = 2 * time.Minute

	aliceNode = 1 // Snowflake node ID of Alice's phone
	seed      = 2026
)

// start is when the simulation begins (simulated time).
var start = time.Date(2026, time.October, 28, 9, 0, 0, 0, time.UTC)

var errNoAck = errors.New("duplicatechat: no ack before timeout")

// Report is what a run measured.
type Report struct {
	Name string
	Sent int // messages Alice sent
	// Shown is how many bubbles Bob's conversation ends up with.
	Shown int
	// Duplicates is how many of those bubbles repeat a message already shown.
	Duplicates int
	// MessagesDuplicated is how many distinct messages Bob saw more than once.
	MessagesDuplicated int
	// MaxCopies is the most copies of a single message on Bob's screen.
	MaxCopies int
	// Lost is how many sent messages never appear on Bob's screen;
	// LostBeforeServer is how many of those never reached the server.
	Lost             int
	LostBeforeServer int
	// OutOfOrder counts adjacent pairs of first appearances in Bob's view
	// where the later bubble was sent earlier.
	OutOfOrder int

	ClientAttempts    int // send attempts by Alice's phone, including retries
	ClientRetransmits int // attempts after the first
	ClientGaveUp      int // messages whose sender stopped retrying without an ack
	ServerReceived    int // attempts that reached the server
	ServerStored      int // copies the server stored and fanned out
	ServerDedupHits   int // retransmissions the server recognised and only re-acked
	PushRetries       int // server-to-phone push attempts after the first
	DeviceDedupHits   int // pushes Bob's phone recognised and dropped
	// PeakServerInbox is the most IDs the server's dedup store held at once.
	PeakServerInbox int
}

// Print writes a summary of the run.
func (r Report) Print(w io.Writer) {
	fmt.Fprintf(w, "%s\n", r.Name)
	fmt.Fprintf(w, "Alice sends %d messages to Bob; both phones lose %.0f%% of requests and %.0f%% of acks, and each passes through %d dead zones of %v-%v\n\n",
		r.Sent, RequestLoss*100, AckLoss*100, DeadZones, MinDead, MaxDead)
	fmt.Fprintln(w, "  Alice's phone -> server")
	fmt.Fprintf(w, "    send attempts:                    %d (%d retransmits)\n", r.ClientAttempts, r.ClientRetransmits)
	fmt.Fprintf(w, "    gave up without an ack:           %d\n", r.ClientGaveUp)
	fmt.Fprintf(w, "    attempts that reached the server: %d\n", r.ServerReceived)
	fmt.Fprintf(w, "    copies stored and fanned out:     %d\n", r.ServerStored)
	fmt.Fprintf(w, "    retransmits caught by dedup:      %d (peak dedup store: %d IDs, window %v)\n", r.ServerDedupHits, r.PeakServerInbox, DedupWindow)
	fmt.Fprintln(w, "  server -> Bob's phone")
	fmt.Fprintf(w, "    push retries:                     %d\n", r.PushRetries)
	fmt.Fprintf(w, "    repeat pushes dropped on device:  %d\n", r.DeviceDedupHits)
	fmt.Fprintln(w, "  Bob's conversation")
	fmt.Fprintf(w, "    bubbles shown:                    %d for %d messages sent\n", r.Shown, r.Sent)
	if r.Duplicates > 0 {
		fmt.Fprintf(w, "    duplicate bubbles:                %d (%d messages shown more than once, up to %d×)\n", r.Duplicates, r.MessagesDuplicated, r.MaxCopies)
	} else {
		fmt.Fprintln(w, "    duplicate bubbles:                0")
	}
	fmt.Fprintf(w, "    lost messages:                    %d (%d never reached the server, %d lost on the push)\n", r.Lost, r.LostBeforeServer, r.Lost-r.LostBeforeServer)
	fmt.Fprintf(w, "    shown out of send order:          %d\n", r.OutOfOrder)
	if r.Duplicates == 0 && r.Lost == 0 {
		fmt.Fprintln(w, "\n  every message appears exactly once, in the order it was sent")
		fmt.Fprintf(w, "  longest possible retransmit span %v < dedup window %v\n", worstCaseRetrySpan(), DedupWindow)
	}
}

// ---------------------------------------------------------------------------
// The network: identical for both designs.

type window struct{ from, to time.Time }

// network holds each phone's dead zones and the seeded loss draws. An
// attempt's fate depends only on when it is made and on (leg, message, copy,
// attempt), so both designs face exactly the same faults for any attempt
// they both make.
type network struct {
	dead [2][]window // per leg: 0 is Alice's link, 1 is Bob's
}

func newNetwork() *network {
	rng := rand.New(rand.NewPCG(seed, 1))
	span := time.Duration(Messages) * (MinGap + MaxGap) / 2
	n := &network{}
	for leg := range n.dead {
		for range DeadZones {
			at := start.Add(time.Duration(rng.Int64N(int64(span))))
			d := MinDead + time.Duration(rng.Int64N(int64(MaxDead-MinDead)))
			n.dead[leg] = append(n.dead[leg], window{at, at.Add(d)})
		}
	}
	return n
}

func (n *network) inDeadZone(leg int, t time.Time) bool {
	for _, w := range n.dead[leg] {
		if !t.Before(w.from) && t.Before(w.to) {
			return true
		}
	}
	return false
}

// attempt reports whether an attempt made at t reaches the other side and
// whether its ack makes it back.
func (n *network) attempt(leg, msg, copyNo, attempt int, t time.Time) (arrived, acked bool) {
	r := rand.New(rand.NewPCG(seed, uint64(leg)<<56|uint64(msg)<<32|uint64(copyNo)<<16|uint64(attempt))).Float64()
	if n.inDeadZone(leg, t) || r < RequestLoss {
		return false, false
	}
	if n.inDeadZone(leg, t.Add(2*Latency)) || r < RequestLoss+AckLoss {
		return true, false
	}
	return true, true
}

// ---------------------------------------------------------------------------
// Senders: how each design retries one message on one leg.

// arrival is one attempt reaching the far side.
type arrival struct {
	at     time.Time
	msg    idempotentconsumer.Message
	copyNo int // which stored copy of the message this push carries (broken stores one per arrival)
}

// sender retries one transfer, starting at t, and returns the attempts that
// reached the far side and whether it ended with an ack.
type sender func(leg int, m idempotentconsumer.Message, copyNo int, t time.Time) (arrivals []arrival, acked bool, attempts int)

func (n *network) brokenSender(leg int, m idempotentconsumer.Message, copyNo int, t time.Time) ([]arrival, bool, int) {
	var out []arrival
	for a := 1; a <= BrokenAttempts; a++ {
		arrived, acked := n.attempt(leg, int(m.Value), copyNo, a, t)
		if arrived {
			out = append(out, arrival{t.Add(Latency), m, copyNo})
		}
		if acked {
			return out, true, a
		}
		t = t.Add(AckTimeout)
	}
	return out, false, BrokenAttempts
}

// fixedPolicy is the retry policy for both legs in the fixed design. Its
// Sleep advances the attempt's simulated clock instead of sleeping.
func fixedPolicy(rng *rand.Rand, sleep func(time.Duration)) retrybackoff.Policy {
	return retrybackoff.Policy{
		MaxAttempts: FixedAttempts,
		BaseDelay:   BaseDelay,
		MaxDelay:    MaxDelay,
		Jitter:      retrybackoff.EqualJitter,
		Rand:        rng.Float64,
		Sleep: func(_ context.Context, d time.Duration) error {
			sleep(d)
			return nil
		},
	}
}

// worstCaseRetrySpan is the longest a fixed-design sender keeps
// retransmitting one message: an ack timeout per attempt plus the largest
// possible backoff before each retry.
func worstCaseRetrySpan() time.Duration {
	p := retrybackoff.Policy{MaxAttempts: FixedAttempts, BaseDelay: BaseDelay, MaxDelay: MaxDelay, Jitter: retrybackoff.NoJitter}
	span := time.Duration(0)
	for retry := range FixedAttempts - 1 {
		span += AckTimeout + p.Backoff(retry)
	}
	return span
}

func (n *network) fixedSender(rng *rand.Rand) sender {
	return func(leg int, m idempotentconsumer.Message, copyNo int, t time.Time) ([]arrival, bool, int) {
		var out []arrival
		attempts := 0
		p := fixedPolicy(rng, func(d time.Duration) { t = t.Add(d) })
		err := p.Do(context.Background(), func(context.Context) error {
			attempts++
			arrived, acked := n.attempt(leg, int(m.Value), copyNo, attempts, t)
			if arrived {
				out = append(out, arrival{t.Add(Latency), m, copyNo})
			}
			if acked {
				return nil
			}
			t = t.Add(AckTimeout)
			return errNoAck
		})
		return out, err == nil, attempts
	}
}

// ---------------------------------------------------------------------------
// The run.

// design is everything that differs between Broken and Fixed.
type design struct {
	name   string
	send   sender
	newID  func(t time.Time) string // client-side ID; "" means none
	server func(a arrival) (stored *idempotentconsumer.Message, dedupHit bool, inbox int)
	device func(a arrival) (show bool)
	sortBy func(view []idempotentconsumer.Message) // nil keeps arrival order
}

func byTime(as []arrival) {
	slices.SortStableFunc(as, func(a, b arrival) int { return a.at.Compare(b.at) })
}

func run(net *network, d design) Report {
	r := Report{Name: d.name, Sent: Messages}
	rng := rand.New(rand.NewPCG(seed, 2))

	// Alice's phone sends each message; collect what reaches the server.
	var toServer []arrival
	t := start
	for i := range Messages {
		t = t.Add(MinGap + time.Duration(rng.Int64N(int64(MaxGap-MinGap))))
		m := idempotentconsumer.Message{ID: d.newID(t), Key: "alice->bob", Value: int64(i)}
		got, acked, attempts := d.send(0, m, 0, t)
		toServer = append(toServer, got...)
		r.ClientAttempts += attempts
		r.ClientRetransmits += attempts - 1
		if !acked {
			r.ClientGaveUp++
		}
	}

	// The server handles arrivals in time order and pushes what it stores.
	byTime(toServer)
	reached := make(map[int64]bool)
	for _, a := range toServer {
		reached[a.msg.Value] = true
	}
	r.LostBeforeServer = Messages - len(reached)
	var toDevice []arrival
	copiesOf := make(map[int64]int)
	for _, a := range toServer {
		r.ServerReceived++
		stored, hit, inbox := d.server(a)
		r.PeakServerInbox = max(r.PeakServerInbox, inbox)
		if hit {
			r.ServerDedupHits++
		}
		if stored == nil {
			continue
		}
		r.ServerStored++
		copiesOf[stored.Value]++
		got, _, attempts := d.send(1, *stored, copiesOf[stored.Value], a.at)
		toDevice = append(toDevice, got...)
		r.PushRetries += attempts - 1
	}

	// Bob's phone handles pushes in time order.
	byTime(toDevice)
	var view []idempotentconsumer.Message
	for _, a := range toDevice {
		if d.device(a) {
			view = append(view, a.msg)
		} else {
			r.DeviceDedupHits++
		}
	}
	if d.sortBy != nil {
		d.sortBy(view)
	}
	r.measureView(view)
	return r
}

func (r *Report) measureView(view []idempotentconsumer.Message) {
	r.Shown = len(view)
	copies := make(map[int64]int)
	last := int64(-1)
	for _, m := range view {
		copies[m.Value]++
		if copies[m.Value] > 1 {
			r.Duplicates++
			continue
		}
		if m.Value < last {
			r.OutOfOrder++
		}
		last = m.Value
	}
	for _, c := range copies {
		if c > 1 {
			r.MessagesDuplicated++
		}
		r.MaxCopies = max(r.MaxCopies, c)
	}
	r.Lost = r.Sent - len(copies)
}

// clock is the simulated time the stores see.
type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }

// newServerInbox returns the fixed server's ingest step: dedup on the
// message ID inside the same transaction that stores the message.
func newServerInbox() func(a arrival) (*idempotentconsumer.Message, bool, int) {
	c := &clock{now: start}
	inbox, err := idempotentconsumer.NewStore(idempotentconsumer.WithClock(c.Now), idempotentconsumer.WithRetention(DedupWindow))
	if err != nil {
		panic(err)
	}
	return func(a arrival) (*idempotentconsumer.Message, bool, int) {
		c.now = a.at
		applied, err := inbox.Apply(a.msg, true, func(tx *idempotentconsumer.Tx, m idempotentconsumer.Message) error {
			tx.Add(m.Key, 1) // the conversation's message count, committed with the inbox row
			return nil
		})
		if err != nil {
			panic(err)
		}
		if !applied {
			return nil, true, inbox.InboxSize()
		}
		m := a.msg
		return &m, false, inbox.InboxSize()
	}
}

// Broken: no client ID, a new server ID per arrival, blind appends on the
// server and the phone, three quick retries then give up, arrival order.
func Broken() Report {
	net := newNetwork()
	serverSeq := 0
	return run(net, design{
		name:  "BROKEN: no client message ID, server ID per attempt, blind appends, 3 retries 1s apart",
		send:  net.brokenSender,
		newID: func(time.Time) string { return "" },
		server: func(a arrival) (*idempotentconsumer.Message, bool, int) {
			serverSeq++
			m := a.msg
			m.ID = "srv-" + strconv.Itoa(serverSeq) // every attempt is a new message
			return &m, false, 0
		},
		device: func(arrival) bool { return true },
	})
}

// Fixed: a client-generated Snowflake ID kept across retransmissions, an
// inbox on the server before fan-out and another on Bob's phone, capped
// exponential backoff with jitter on both legs, and the view sorted by ID.
func Fixed() Report {
	net := newNetwork()
	c := &clock{now: start}
	ids, err := snowflakeid.New(aliceNode, snowflakeid.WithClock(c.Now))
	if err != nil {
		panic(err)
	}
	phone, err := idempotentconsumer.NewStore(idempotentconsumer.WithClock(c.Now)) // keeps every ID it has shown
	if err != nil {
		panic(err)
	}
	return run(net, design{
		name: "FIXED: client Snowflake ID + server inbox before fan-out + device dedup + backoff, sorted by ID",
		send: net.fixedSender(rand.New(rand.NewPCG(seed, 3))),
		newID: func(t time.Time) string {
			c.now = t
			id, err := ids.Next()
			if err != nil {
				panic(err)
			}
			return id.String()
		},
		server: newServerInbox(),
		device: func(a arrival) bool {
			c.now = a.at
			applied, err := phone.Apply(a.msg, true, func(*idempotentconsumer.Tx, idempotentconsumer.Message) error { return nil })
			if err != nil {
				panic(err)
			}
			return applied
		},
		sortBy: func(view []idempotentconsumer.Message) {
			slices.SortFunc(view, func(a, b idempotentconsumer.Message) int {
				x, _ := snowflakeid.Parse(a.ID)
				y, _ := snowflakeid.Parse(b.ID)
				return cmp.Compare(x, y)
			})
		},
	})
}
