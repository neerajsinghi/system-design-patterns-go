// Package pubsub implements an in-process publish/subscribe broker with
// topics, fan-out to every subscriber of a topic, and an explicit policy for
// what happens when a subscriber cannot keep up.
//
// Each subscriber owns a bounded buffer. Publish never blocks on a slow
// subscriber: when a buffer is full the subscriber's Overflow policy decides
// whether the new message is dropped, the oldest buffered message is dropped,
// or the subscriber is disconnected. One slow consumer therefore cannot stall
// the publisher or the other subscribers.
package pubsub

import (
	"errors"
	"sync"
)

// Overflow selects how a full subscriber buffer is handled.
type Overflow int

const (
	// DropNewest discards the message being published (keep what is queued).
	DropNewest Overflow = iota
	// DropOldest discards the oldest queued message to make room (keep the latest).
	DropOldest
	// Disconnect evicts the subscriber and closes its channel.
	Disconnect
)

// String returns the policy name.
func (o Overflow) String() string {
	switch o {
	case DropNewest:
		return "drop-newest"
	case DropOldest:
		return "drop-oldest"
	case Disconnect:
		return "disconnect"
	default:
		return "unknown"
	}
}

// Errors returned by the broker.
var (
	ErrClosed        = errors.New("pubsub: broker closed")
	ErrEmptyTopic    = errors.New("pubsub: topic must not be empty")
	ErrBadBuffer     = errors.New("pubsub: buffer must be >= 1")
	ErrUnknownPolicy = errors.New("pubsub: unknown overflow policy")
)

// Message is a single published event. Seq is a per-topic sequence number,
// which lets a subscriber detect gaps caused by dropping.
type Message struct {
	Topic   string
	Seq     uint64
	Payload any
}

// PublishStats reports what happened to one published message.
type PublishStats struct {
	Delivered int // subscribers that had the message queued
	Dropped   int // subscribers that lost a message (new or old) to overflow
	Evicted   int // subscribers disconnected by this publish
}

// Subscription is one subscriber's view of a topic.
type Subscription struct {
	broker *Broker
	topic  string
	policy Overflow
	ch     chan Message

	// guarded by broker.mu
	dropped uint64
	evicted bool
	closed  bool
}

// C returns the channel messages are delivered on. It is closed when the
// subscription is cancelled, evicted, or the broker is closed.
func (s *Subscription) C() <-chan Message { return s.ch }

// Dropped returns how many messages this subscriber has lost to overflow.
func (s *Subscription) Dropped() uint64 {
	s.broker.mu.Lock()
	defer s.broker.mu.Unlock()
	return s.dropped
}

// Evicted reports whether the subscriber was disconnected for being slow.
func (s *Subscription) Evicted() bool {
	s.broker.mu.Lock()
	defer s.broker.mu.Unlock()
	return s.evicted
}

// Cancel unsubscribes and closes C. It is safe to call more than once.
func (s *Subscription) Cancel() {
	s.broker.mu.Lock()
	defer s.broker.mu.Unlock()
	s.broker.removeLocked(s)
}

// Broker routes published messages to topic subscribers.
type Broker struct {
	mu     sync.Mutex
	topics map[string]map[*Subscription]struct{}
	seq    map[string]uint64
	closed bool
}

// NewBroker returns an empty broker.
func NewBroker() *Broker {
	return &Broker{
		topics: make(map[string]map[*Subscription]struct{}),
		seq:    make(map[string]uint64),
	}
}

// Subscribe registers a subscriber on topic with a buffer of the given size.
// Only messages published after Subscribe returns are delivered.
func (b *Broker) Subscribe(topic string, buffer int, policy Overflow) (*Subscription, error) {
	if topic == "" {
		return nil, ErrEmptyTopic
	}
	if buffer < 1 {
		return nil, ErrBadBuffer
	}
	if policy < DropNewest || policy > Disconnect {
		return nil, ErrUnknownPolicy
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	s := &Subscription{broker: b, topic: topic, policy: policy, ch: make(chan Message, buffer)}
	subs := b.topics[topic]
	if subs == nil {
		subs = make(map[*Subscription]struct{})
		b.topics[topic] = subs
	}
	subs[s] = struct{}{}
	return s, nil
}

// Publish fans payload out to every current subscriber of topic. It never
// blocks: all sends are non-blocking and happen while holding the broker
// lock, which is also what makes closing subscriber channels safe.
func (b *Broker) Publish(topic string, payload any) (PublishStats, error) {
	if topic == "" {
		return PublishStats{}, ErrEmptyTopic
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return PublishStats{}, ErrClosed
	}
	b.seq[topic]++
	msg := Message{Topic: topic, Seq: b.seq[topic], Payload: payload}

	var st PublishStats
	for s := range b.topics[topic] {
		select {
		case s.ch <- msg:
			st.Delivered++
			continue
		default:
		}
		// Buffer is full: apply the subscriber's policy.
		switch s.policy {
		case DropNewest:
			s.dropped++
			st.Dropped++
		case DropOldest:
			// We are the only sender (we hold b.mu), so after removing one
			// element there is guaranteed room. The consumer may also have
			// drained one concurrently, in which case nothing is discarded.
			select {
			case <-s.ch:
				s.dropped++
				st.Dropped++
			default:
			}
			s.ch <- msg
			st.Delivered++
		case Disconnect:
			s.evicted = true
			b.removeLocked(s)
			st.Evicted++
		}
	}
	return st, nil
}

// Subscribers returns the number of active subscribers on topic.
func (b *Broker) Subscribers(topic string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.topics[topic])
}

// Close cancels every subscription and rejects further operations.
func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for _, subs := range b.topics {
		for s := range subs {
			b.removeLocked(s)
		}
	}
}

func (b *Broker) removeLocked(s *Subscription) {
	if s.closed {
		return
	}
	s.closed = true
	if subs := b.topics[s.topic]; subs != nil {
		delete(subs, s)
		if len(subs) == 0 {
			delete(b.topics, s.topic)
		}
	}
	close(s.ch)
}
