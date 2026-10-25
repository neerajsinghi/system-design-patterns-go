// Package transactionaloutbox shows how to publish an event reliably after a
// database write without a distributed transaction.
//
// The business row and an "outbox" row describing the event are written in
// the same local database transaction, so either both exist or neither does.
// A separate relay polls the outbox, publishes unpublished rows to the
// message broker, and only then marks them published. If the relay crashes
// between publishing and marking, the row is published again on the next
// pass: delivery is at-least-once, and consumers deduplicate by message ID.
//
// The database here is an in-memory stand-in with all-or-nothing
// transactions; in production this is your SQL database and the relay is a
// poller or a change-data-capture pipeline.
package transactionaloutbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Order is the business entity being written.
type Order struct {
	ID     string
	Amount int
}

// OutboxRecord is one pending event, stored in the same database as orders.
type OutboxRecord struct {
	ID        int64 // monotonically increasing, used for ordering and dedup
	Topic     string
	Key       string
	Payload   string
	Published bool
}

// DB is an in-memory database with an orders table and an outbox table.
type DB struct {
	mu     sync.Mutex
	orders map[string]Order
	outbox []OutboxRecord
	nextID int64
}

// NewDB returns an empty database.
func NewDB() *DB { return &DB{orders: make(map[string]Order)} }

// Tx buffers writes until the transaction commits.
type Tx struct {
	db     *DB
	orders []Order
	events []OutboxRecord
}

// GetOrder reads a committed order (no read-your-writes, kept simple).
func (tx *Tx) GetOrder(id string) (Order, bool) {
	o, ok := tx.db.orders[id] // caller holds db.mu via WithTx
	return o, ok
}

// PutOrder stages an order write.
func (tx *Tx) PutOrder(o Order) { tx.orders = append(tx.orders, o) }

// AddOutbox stages an event in the outbox table.
func (tx *Tx) AddOutbox(topic, key, payload string) {
	tx.events = append(tx.events, OutboxRecord{Topic: topic, Key: key, Payload: payload})
}

// WithTx runs fn in a transaction. If fn returns an error nothing is written.
// Transactions are serialised by a single lock, which is enough to make them
// atomic and isolated in this simulation.
func (db *DB) WithTx(fn func(tx *Tx) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	tx := &Tx{db: db}
	if err := fn(tx); err != nil {
		return err // rollback: staged writes are discarded
	}
	for _, o := range tx.orders {
		db.orders[o.ID] = o
	}
	for _, e := range tx.events {
		db.nextID++
		e.ID = db.nextID
		db.outbox = append(db.outbox, e)
	}
	return nil
}

// Order returns a committed order.
func (db *DB) Order(id string) (Order, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	o, ok := db.orders[id]
	return o, ok
}

// Pending returns up to limit unpublished outbox rows in ID order.
func (db *DB) Pending(limit int) []OutboxRecord {
	db.mu.Lock()
	defer db.mu.Unlock()
	var out []OutboxRecord
	for _, r := range db.outbox {
		if !r.Published {
			out = append(out, r)
			if len(out) == limit {
				break
			}
		}
	}
	return out
}

// MarkPublished flags an outbox row as delivered. Real systems often delete
// the row or archive it instead.
func (db *DB) MarkPublished(id int64) {
	db.mu.Lock()
	defer db.mu.Unlock()
	for i := range db.outbox {
		if db.outbox[i].ID == id {
			db.outbox[i].Published = true
			return
		}
	}
}

// ErrDuplicateOrder is returned when an order ID already exists.
var ErrDuplicateOrder = errors.New("transactionaloutbox: order already exists")

// PlaceOrder writes the order and its OrderPlaced event atomically.
func PlaceOrder(db *DB, o Order) error {
	if o.ID == "" || o.Amount <= 0 {
		return fmt.Errorf("transactionaloutbox: invalid order %+v", o)
	}
	return db.WithTx(func(tx *Tx) error {
		if _, exists := tx.GetOrder(o.ID); exists {
			return ErrDuplicateOrder
		}
		tx.PutOrder(o)
		tx.AddOutbox("order.placed", o.ID, fmt.Sprintf(`{"id":%q,"amount":%d}`, o.ID, o.Amount))
		return nil
	})
}

// Message is what the relay hands to the broker.
type Message struct {
	ID      int64 // the outbox row ID, used by consumers to deduplicate
	Topic   string
	Key     string
	Payload string
}

// Publisher is the message broker client.
type Publisher interface {
	Publish(ctx context.Context, m Message) error
}

// Relay moves outbox rows to the broker.
type Relay struct {
	DB        *DB
	Publisher Publisher
	BatchSize int // defaults to 100
	// AfterPublish is a test hook called after a successful publish and
	// before the row is marked. Returning an error simulates the relay
	// crashing at the worst possible moment.
	AfterPublish func(Message) error
}

// RelayOnce publishes one batch of pending rows in order. It stops at the
// first failure so later events are not published ahead of earlier ones.
// It returns how many rows were published and marked.
func (r *Relay) RelayOnce(ctx context.Context) (int, error) {
	batch := r.BatchSize
	if batch <= 0 {
		batch = 100
	}
	n := 0
	for _, rec := range r.DB.Pending(batch) {
		if err := ctx.Err(); err != nil {
			return n, err
		}
		m := Message{ID: rec.ID, Topic: rec.Topic, Key: rec.Key, Payload: rec.Payload}
		if err := r.Publisher.Publish(ctx, m); err != nil {
			return n, fmt.Errorf("publish outbox %d: %w", rec.ID, err)
		}
		if r.AfterPublish != nil {
			if err := r.AfterPublish(m); err != nil {
				return n, err
			}
		}
		r.DB.MarkPublished(rec.ID)
		n++
	}
	return n, nil
}

// Run calls RelayOnce every interval until ctx is cancelled. Errors are
// passed to onErr (if non-nil) and retried on the next tick.
func (r *Relay) Run(ctx context.Context, interval time.Duration, onErr func(error)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if _, err := r.RelayOnce(ctx); err != nil && onErr != nil && ctx.Err() == nil {
			onErr(err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// DedupConsumer is an idempotent consumer: it remembers processed message IDs
// and ignores redeliveries. In production the seen-set lives in the
// consumer's own database, updated in the same transaction as its side effect.
type DedupConsumer struct {
	mu        sync.Mutex
	seen      map[int64]bool
	Processed []Message // first delivery of each message, in order
	Dupes     int
}

// Publish implements Publisher so the consumer can stand in for a broker.
func (c *DedupConsumer) Publish(_ context.Context, m Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.seen == nil {
		c.seen = make(map[int64]bool)
	}
	if c.seen[m.ID] {
		c.Dupes++
		return nil
	}
	c.seen[m.ID] = true
	c.Processed = append(c.Processed, m)
	return nil
}

// Snapshot returns a copy of the processed messages and the duplicate count.
func (c *DedupConsumer) Snapshot() ([]Message, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Message(nil), c.Processed...), c.Dupes
}
