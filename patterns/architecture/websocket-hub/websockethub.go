// Package websockethub implements a WebSocket connection hub for real-time,
// two-way delivery (chat, live location) to many users spread over several
// server nodes, with a hand-rolled RFC 6455 implementation from the standard
// library only.
//
// The problems it addresses:
//
//   - Each connection is a long-lived TCP socket that lives on exactly one
//     node, but a message for a user can originate on any node, and a user
//     may be connected from several devices at once.
//   - One slow client (a phone on a bad link) must not stall delivery to
//     everyone else.
//   - Dead connections are silent: a NAT box drops its mapping or a phone
//     goes to sleep, and the server's socket stays "open" until it tries to
//     write. They must be detected and cleaned up.
//
// The approach:
//
//   - Upgrade and Dial do the opening handshake; Conn does framing,
//     masking, fragmentation, ping/pong and the closing handshake.
//   - Each connection gets a bounded send queue and its own writer
//     goroutine. Delivery only enqueues, so it never blocks on a socket. When
//     a queue is full the SlowPolicy either drops the oldest queued message
//     or disconnects the client.
//   - Heartbeat pings every connection and disconnects those that have sent
//     nothing (no pong, no message) for longer than the pong timeout. The
//     clock is injectable, so tests drive it deterministically.
//   - Nodes join a Cluster: a pub/sub bus (patterns/distributed/pub-sub) plus
//     a Registry mapping each user to the nodes holding their connections.
//     SendToUser looks the user up and publishes to those nodes' topics, and
//     only the owning node writes to the socket.
package websockethub

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	pubsub "github.com/neerajsinghi/system-design-patterns-go/patterns/distributed/pub-sub"
)

// SlowPolicy decides what happens when a connection's send queue is full.
type SlowPolicy int

const (
	// DropOldest discards the oldest queued message to make room. Good for
	// live location or prices, where only the latest value matters.
	DropOldest SlowPolicy = iota
	// Disconnect closes the connection. Good for chat, where silently losing
	// messages is worse than forcing a reconnect and a history fetch.
	Disconnect
)

// String returns the policy name.
func (p SlowPolicy) String() string {
	switch p {
	case DropOldest:
		return "drop-oldest"
	case Disconnect:
		return "disconnect"
	default:
		return "unknown"
	}
}

// Errors returned by the hub.
var (
	ErrHubClosed   = errors.New("websockethub: hub closed")
	ErrUserOffline = errors.New("websockethub: user has no connections on any node")
)

// Registry maps each user to the nodes holding their connections, counting
// connections per node so multi-device users are handled. It is the
// "connection registry" that Redis or DynamoDB plays in production.
type Registry struct {
	mu    sync.Mutex
	users map[string]map[string]int // user -> node -> connections
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{users: map[string]map[string]int{}} }

// Add records one more connection for user on node.
func (r *Registry) Add(user, node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	nodes := r.users[user]
	if nodes == nil {
		nodes = map[string]int{}
		r.users[user] = nodes
	}
	nodes[node]++
}

// Remove records one connection fewer for user on node, deleting entries
// that reach zero.
func (r *Registry) Remove(user, node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	nodes := r.users[user]
	if nodes[node] <= 1 {
		delete(nodes, node)
	} else {
		nodes[node]--
	}
	if len(nodes) == 0 {
		delete(r.users, user)
	}
}

// Nodes returns the nodes user is connected to, sorted.
func (r *Registry) Nodes(user string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.users[user]))
	for n := range r.users[user] {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Users returns how many users have at least one connection.
func (r *Registry) Users() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.users)
}

// Cluster is the shared plumbing between nodes: the message bus and the
// connection registry.
type Cluster struct {
	bus      *pubsub.Broker
	registry *Registry

	mu    sync.Mutex
	nodes map[string]bool
}

// NewCluster returns a cluster with an in-process bus and an empty registry.
func NewCluster() *Cluster {
	return &Cluster{bus: pubsub.NewBroker(), registry: NewRegistry(), nodes: map[string]bool{}}
}

// Registry returns the cluster's connection registry.
func (c *Cluster) Registry() *Registry { return c.registry }

// Close shuts the bus down. Close every hub first.
func (c *Cluster) Close() { c.bus.Close() }

const broadcastTopic = "broadcast"

func nodeTopic(node string) string { return "node/" + node }

// envelope is what travels on the bus between nodes.
type envelope struct {
	Origin string
	User   string // empty for a broadcast
	Data   []byte
}

// Stats counts what a hub has done. Every field is cumulative except
// Connections.
type Stats struct {
	Connections int    // currently registered on this node
	Accepted    uint64 // connections registered
	Received    uint64 // messages read from clients
	Enqueued    uint64 // messages queued to local connections
	Written     uint64 // messages written to sockets
	Forwarded   uint64 // messages published to other nodes
	FromBus     uint64 // messages received from other nodes
	Dropped     uint64 // messages discarded by DropOldest
	EvictedSlow uint64 // connections closed because their queue was full
	EvictedDead uint64 // connections closed by the heartbeat
	Pings       uint64 // pings requested
	Pongs       uint64 // pongs received
	BusDropped  uint64 // bus messages this node lost because its bus buffer was full
}

type counters struct {
	accepted, received, enqueued, written, forwarded, fromBus atomic.Uint64
	dropped, evictedSlow, evictedDead, pings, pongs           atomic.Uint64
}

type config struct {
	queueSize    int
	policy       SlowPolicy
	pingInterval time.Duration
	pongTimeout  time.Duration
	writeTimeout time.Duration
	maxMessage   int64
	busBuffer    int
	now          func() time.Time
	cluster      *Cluster
	auth         func(*http.Request) (string, error)
	onMessage    func(h *Hub, user string, op Opcode, data []byte)
}

// Option configures a Hub.
type Option func(*config) error

// WithQueueSize sets each connection's send-queue capacity (default 64).
func WithQueueSize(n int) Option {
	return func(c *config) error {
		if n < 1 {
			return errors.New("websockethub: queue size must be >= 1")
		}
		c.queueSize = n
		return nil
	}
}

// WithSlowPolicy sets what happens when a send queue is full (default
// DropOldest).
func WithSlowPolicy(p SlowPolicy) Option {
	return func(c *config) error {
		if p != DropOldest && p != Disconnect {
			return errors.New("websockethub: unknown slow policy")
		}
		c.policy = p
		return nil
	}
}

// WithHeartbeat sets the ping interval used by Run and the silence after
// which Heartbeat disconnects a connection (defaults 25s and 60s). The
// timeout must be longer than the interval, or a healthy client could be
// cut off before it was even pinged.
func WithHeartbeat(interval, timeout time.Duration) Option {
	return func(c *config) error {
		if interval <= 0 || timeout <= interval {
			return errors.New("websockethub: need 0 < ping interval < pong timeout")
		}
		c.pingInterval, c.pongTimeout = interval, timeout
		return nil
	}
}

// WithWriteTimeout bounds each socket write (default 10s). A write that
// times out closes the connection, which also catches a client whose TCP
// window stays shut.
func WithWriteTimeout(d time.Duration) Option {
	return func(c *config) error {
		if d <= 0 {
			return errors.New("websockethub: write timeout must be > 0")
		}
		c.writeTimeout = d
		return nil
	}
}

// WithMaxMessageSize bounds inbound frames and messages (default 1 MiB).
func WithMaxMessageSize(n int64) Option {
	return func(c *config) error {
		if n < 1 {
			return errors.New("websockethub: max message size must be >= 1")
		}
		c.maxMessage = n
		return nil
	}
}

// WithBusBuffer sets this node's inbound bus buffer (default 4096).
func WithBusBuffer(n int) Option {
	return func(c *config) error {
		if n < 1 {
			return errors.New("websockethub: bus buffer must be >= 1")
		}
		c.busBuffer = n
		return nil
	}
}

// WithClock sets the clock Heartbeat uses to judge silence.
func WithClock(now func() time.Time) Option {
	return func(c *config) error {
		if now == nil {
			return errors.New("websockethub: clock must not be nil")
		}
		c.now = now
		return nil
	}
}

// WithCluster joins the hub to a cluster shared with other nodes. Without
// it the hub is a cluster of one.
func WithCluster(cl *Cluster) Option {
	return func(c *config) error {
		if cl == nil {
			return errors.New("websockethub: cluster must not be nil")
		}
		c.cluster = cl
		return nil
	}
}

// WithAuth sets how ServeHTTP identifies the user. The default reads the
// "user" query parameter, a stand-in for validating a session token.
func WithAuth(f func(*http.Request) (string, error)) Option {
	return func(c *config) error {
		if f == nil {
			return errors.New("websockethub: auth must not be nil")
		}
		c.auth = f
		return nil
	}
}

// WithOnMessage sets the handler for messages clients send. It runs on the
// connection's read goroutine, so it should not block for long.
func WithOnMessage(f func(h *Hub, user string, op Opcode, data []byte)) Option {
	return func(c *config) error {
		c.onMessage = f
		return nil
	}
}

func queryUser(r *http.Request) (string, error) {
	u := r.URL.Query().Get("user")
	if u == "" {
		return "", errors.New("missing user")
	}
	return u, nil
}

// Hub is one node: it owns the connections that landed on it and delivers
// messages to them.
type Hub struct {
	node string
	cfg  config
	cl   *Cluster
	st   counters

	nodeSub, bcastSub *pubsub.Subscription

	mu     sync.Mutex
	users  map[string]map[*client]struct{}
	conns  map[*client]struct{}
	nextID uint64
	closed bool

	wg sync.WaitGroup
}

// New starts a hub for node. Node names must be unique within a cluster.
func New(node string, opts ...Option) (*Hub, error) {
	if node == "" {
		return nil, errors.New("websockethub: node name must not be empty")
	}
	cfg := config{
		queueSize:    64,
		policy:       DropOldest,
		pingInterval: 25 * time.Second,
		pongTimeout:  60 * time.Second,
		writeTimeout: 10 * time.Second,
		maxMessage:   DefaultMaxMessageSize,
		busBuffer:    4096,
		now:          time.Now,
		auth:         queryUser,
	}
	for _, o := range opts {
		if err := o(&cfg); err != nil {
			return nil, err
		}
	}
	if cfg.cluster == nil {
		cfg.cluster = NewCluster()
	}
	cl := cfg.cluster
	cl.mu.Lock()
	if cl.nodes[node] {
		cl.mu.Unlock()
		return nil, fmt.Errorf("websockethub: node %q already in cluster", node)
	}
	cl.nodes[node] = true
	cl.mu.Unlock()

	h := &Hub{node: node, cfg: cfg, cl: cl, users: map[string]map[*client]struct{}{}, conns: map[*client]struct{}{}}
	var err error
	if h.nodeSub, err = cl.bus.Subscribe(nodeTopic(node), cfg.busBuffer, pubsub.DropNewest); err != nil {
		return nil, err
	}
	if h.bcastSub, err = cl.bus.Subscribe(broadcastTopic, cfg.busBuffer, pubsub.DropNewest); err != nil {
		h.nodeSub.Cancel()
		return nil, err
	}
	h.wg.Add(2)
	go h.consume(h.nodeSub)
	go h.consume(h.bcastSub)
	return h, nil
}

// Node returns the hub's node name.
func (h *Hub) Node() string { return h.node }

// consume delivers messages other nodes published for this node.
func (h *Hub) consume(sub *pubsub.Subscription) {
	defer h.wg.Done()
	for m := range sub.C() {
		env := m.Payload.(envelope)
		if env.Origin == h.node {
			continue // our own broadcast, already delivered locally
		}
		h.st.fromBus.Add(1)
		if env.User == "" {
			h.deliver(h.snapshotAll(), env.Data)
		} else {
			h.deliver(h.snapshotUser(env.User), env.Data)
		}
	}
}

// ServeHTTP authenticates the request, upgrades it and registers the
// connection.
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	user, err := h.cfg.auth(r)
	if err != nil {
		http.Error(w, "unauthorized: "+err.Error(), http.StatusUnauthorized)
		return
	}
	c, err := Upgrade(w, r)
	if err != nil {
		return // Upgrade already answered
	}
	if _, err := h.Register(user, c); err != nil {
		_ = c.CloseHandshake(CloseGoingAway, "server shutting down")
	}
}

// Register adopts an upgraded connection for user, starts its reader and
// writer goroutines, and records it in the registry. It returns a
// connection ID ("node/n"), like the connection IDs API Gateway hands out.
func (h *Hub) Register(user string, conn *Conn) (string, error) {
	if user == "" {
		return "", errors.New("websockethub: user must not be empty")
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return "", ErrHubClosed
	}
	h.nextID++
	c := &client{
		hub:  h,
		id:   h.node + "/" + strconv.FormatUint(h.nextID, 10),
		user: user,
		conn: conn,
		wake: make(chan struct{}, 1),
		done: make(chan struct{}),
	}
	c.lastSeen.Store(h.cfg.now().UnixNano())
	set := h.users[user]
	if set == nil {
		set = map[*client]struct{}{}
		h.users[user] = set
	}
	set[c] = struct{}{}
	h.conns[c] = struct{}{}
	h.cl.registry.Add(user, h.node)
	h.wg.Add(2)
	h.mu.Unlock()

	h.st.accepted.Add(1)
	conn.SetMaxMessageSize(h.cfg.maxMessage)
	conn.SetPongHandler(func([]byte) {
		h.st.pongs.Add(1)
		c.lastSeen.Store(h.cfg.now().UnixNano())
	})
	go c.readLoop()
	go c.writeLoop()
	return c.id, nil
}

func (h *Hub) unregister(c *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.conns[c]; !ok {
		return
	}
	delete(h.conns, c)
	if set := h.users[c.user]; set != nil {
		delete(set, c)
		if len(set) == 0 {
			delete(h.users, c.user)
		}
	}
	h.cl.registry.Remove(c.user, h.node)
}

func (h *Hub) snapshotUser(user string) []*client {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]*client, 0, len(h.users[user]))
	for c := range h.users[user] {
		out = append(out, c)
	}
	return out
}

func (h *Hub) snapshotAll() []*client {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]*client, 0, len(h.conns))
	for c := range h.conns {
		out = append(out, c)
	}
	return out
}

// deliver enqueues data on each client. It never touches a socket, so it
// can't block on a slow one. It runs outside h.mu because eviction
// unregisters.
func (h *Hub) deliver(cs []*client, data []byte) int {
	n := 0
	for _, c := range cs {
		if c.enqueue(data) {
			n++
		}
	}
	return n
}

// SendToUser delivers data as a text message to every connection of user,
// on whichever nodes hold them. Local connections are queued directly;
// remote nodes get it through the bus and write it themselves. It returns
// the number of nodes the message was routed to, or ErrUserOffline.
func (h *Hub) SendToUser(user string, data []byte) (int, error) {
	if h.isClosed() {
		return 0, ErrHubClosed
	}
	nodes := h.cl.registry.Nodes(user)
	if len(nodes) == 0 {
		return 0, ErrUserOffline
	}
	for _, n := range nodes {
		if n == h.node {
			h.deliver(h.snapshotUser(user), data)
			continue
		}
		if _, err := h.cl.bus.Publish(nodeTopic(n), envelope{Origin: h.node, User: user, Data: data}); err != nil {
			return 0, err
		}
		h.st.forwarded.Add(1)
	}
	return len(nodes), nil
}

// Broadcast delivers data to every connection on every node. It returns
// how many local connections it was queued for.
func (h *Hub) Broadcast(data []byte) (int, error) {
	if h.isClosed() {
		return 0, ErrHubClosed
	}
	n := h.deliver(h.snapshotAll(), data)
	if _, err := h.cl.bus.Publish(broadcastTopic, envelope{Origin: h.node, Data: data}); err != nil {
		return n, err
	}
	h.st.forwarded.Add(1)
	return n, nil
}

// Heartbeat is one sweep: every connection that has been silent longer than
// the pong timeout is closed, and every other one is sent a ping. Run calls
// it every ping interval; tests call it directly with a fake clock.
func (h *Hub) Heartbeat() (pinged, evicted int) {
	now := h.cfg.now().UnixNano()
	for _, c := range h.snapshotAll() {
		if time.Duration(now-c.lastSeen.Load()) > h.cfg.pongTimeout {
			if c.shutdown() {
				h.st.evictedDead.Add(1)
				evicted++
			}
			continue
		}
		c.requestPing()
		h.st.pings.Add(1)
		pinged++
	}
	return pinged, evicted
}

// Run calls Heartbeat every ping interval until ctx is done.
func (h *Hub) Run(ctx context.Context) {
	t := time.NewTicker(h.cfg.pingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h.Heartbeat()
		}
	}
}

// Connections returns how many connections this node holds.
func (h *Hub) Connections() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.conns)
}

// Stats returns a snapshot of the hub's counters.
func (h *Hub) Stats() Stats {
	return Stats{
		Connections: h.Connections(),
		Accepted:    h.st.accepted.Load(),
		Received:    h.st.received.Load(),
		Enqueued:    h.st.enqueued.Load(),
		Written:     h.st.written.Load(),
		Forwarded:   h.st.forwarded.Load(),
		FromBus:     h.st.fromBus.Load(),
		Dropped:     h.st.dropped.Load(),
		EvictedSlow: h.st.evictedSlow.Load(),
		EvictedDead: h.st.evictedDead.Load(),
		Pings:       h.st.pings.Load(),
		Pongs:       h.st.pongs.Load(),
		BusDropped:  h.nodeSub.Dropped() + h.bcastSub.Dropped(),
	}
}

func (h *Hub) isClosed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closed
}

// Close refuses new connections, sends every connection a 1001 close
// frame, leaves the cluster, and waits for all goroutines to finish.
func (h *Hub) Close() {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	h.mu.Unlock()
	for _, c := range h.snapshotAll() {
		_ = c.conn.CloseHandshake(CloseGoingAway, "server shutting down")
	}
	h.nodeSub.Cancel()
	h.bcastSub.Cancel()
	h.wg.Wait()
	h.cl.mu.Lock()
	delete(h.cl.nodes, h.node)
	h.cl.mu.Unlock()
}

// client is the hub's state for one connection.
type client struct {
	hub      *Hub
	id       string
	user     string
	conn     *Conn
	lastSeen atomic.Int64 // unix nanos on the hub's clock

	mu       sync.Mutex
	queue    [][]byte
	wantPing bool
	closed   bool

	wake chan struct{} // capacity 1: "there is work"
	done chan struct{}
}

func (c *client) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// enqueue adds data to the send queue, applying the slow policy when it is
// full. It reports whether data was queued.
func (c *client) enqueue(data []byte) bool {
	h := c.hub
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false
	}
	if len(c.queue) >= h.cfg.queueSize {
		if h.cfg.policy == Disconnect {
			c.mu.Unlock()
			if c.shutdown() {
				h.st.evictedSlow.Add(1)
			}
			return false
		}
		c.queue[0] = nil
		c.queue = c.queue[1:]
		h.st.dropped.Add(1)
	}
	c.queue = append(c.queue, data)
	c.mu.Unlock()
	h.st.enqueued.Add(1)
	c.signal()
	return true
}

func (c *client) requestPing() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.wantPing = true
	c.mu.Unlock()
	c.signal()
}

// shutdown closes the connection abruptly and unregisters it. Only the
// first call does anything; it reports whether this call was it.
func (c *client) shutdown() bool {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false
	}
	c.closed = true
	c.queue = nil
	c.mu.Unlock()
	close(c.done)
	_ = c.conn.Close() // unblocks a writer stuck on a full socket
	c.hub.unregister(c)
	return true
}

// writeLoop is the only goroutine that writes data frames to this socket.
// It pops one message at a time, so while it is blocked on a slow socket
// the queue keeps its full capacity and the slow policy can kick in.
func (c *client) writeLoop() {
	h := c.hub
	defer h.wg.Done()
	for {
		select {
		case <-c.wake:
		case <-c.done:
			return
		}
		for {
			c.mu.Lock()
			if c.closed {
				c.mu.Unlock()
				return
			}
			ping := c.wantPing
			c.wantPing = false
			var msg []byte
			has := false
			if !ping && len(c.queue) > 0 {
				msg, has = c.queue[0], true
				c.queue[0] = nil
				c.queue = c.queue[1:]
			}
			c.mu.Unlock()
			if !ping && !has {
				break
			}
			_ = c.conn.SetWriteDeadline(time.Now().Add(h.cfg.writeTimeout))
			var err error
			if ping {
				err = c.conn.WriteControl(OpPing, nil)
			} else {
				err = c.conn.WriteMessage(OpText, msg)
				if err == nil {
					h.st.written.Add(1)
				}
			}
			if err != nil {
				c.shutdown()
				return
			}
		}
	}
}

// readLoop reads client messages until the connection ends, then
// unregisters it. Any frame counts as a sign of life.
func (c *client) readLoop() {
	h := c.hub
	defer h.wg.Done()
	defer c.shutdown()
	for {
		op, data, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		c.lastSeen.Store(h.cfg.now().UnixNano())
		h.st.received.Add(1)
		if f := h.cfg.onMessage; f != nil {
			f(h, c.user, op, data)
		}
	}
}
