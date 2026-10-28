package pubsub

// Event versioning and upcasting.
//
// A topic outlives any single version of its producers. During a rollout some
// producers still emit v1 events while others emit v2, and a consumer that
// replays history will see events written years ago. Instead of teaching every
// consumer every old shape, each event travels in an Envelope that names its
// type and schema version, and the consumer runs it through a chain of
// upcasters (v1 -> v2 -> v3 ...) until it reaches the version the consumer
// code understands. Only the newest shape has to be handled by business logic.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// Errors returned by the versioning helpers.
var (
	ErrUnknownEventType = errors.New("pubsub: unknown event type")
	ErrUnknownVersion   = errors.New("pubsub: unknown event version")
	ErrMissingUpcaster  = errors.New("pubsub: no upcaster for version")
	ErrBadEnvelope      = errors.New("pubsub: malformed envelope")
	ErrBadRegistration  = errors.New("pubsub: invalid upcaster registration")
)

// Envelope is the wire format of a versioned event. Data is the payload in
// the shape of schema Version of event Type.
type Envelope struct {
	Type    string          `json:"type"`
	Version int             `json:"version"`
	Data    json.RawMessage `json:"data"`
}

// Encode marshals payload into an envelope of the given type and version.
// The result is what a producer publishes (as a []byte Payload).
func Encode(eventType string, version int, payload any) ([]byte, error) {
	if eventType == "" || version < 1 {
		return nil, fmt.Errorf("%w: type %q version %d", ErrBadEnvelope, eventType, version)
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(Envelope{Type: eventType, Version: version, Data: data})
}

// DecodeEnvelope parses the envelope without touching its payload.
func DecodeEnvelope(raw []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(raw, &e); err != nil {
		return Envelope{}, fmt.Errorf("%w: %v", ErrBadEnvelope, err)
	}
	if e.Type == "" || e.Version < 1 || len(e.Data) == 0 {
		return Envelope{}, fmt.Errorf("%w: type %q version %d", ErrBadEnvelope, e.Type, e.Version)
	}
	return e, nil
}

// Upcaster converts the fields of one event from version N to N+1, in place.
// Fields are kept as raw JSON so that anything the upcaster does not touch,
// including fields it has never heard of, passes through unchanged. An
// upcaster should be written so that it does not clobber a field the producer
// already set (a v2 producer that dual-writes a new field keeps its value).
type Upcaster func(fields map[string]json.RawMessage) error

// UpcastStats counts what a Registry has done.
type UpcastStats struct {
	Decoded  uint64 // events successfully brought to the current version
	Upcasted uint64 // of those, how many needed at least one upcast step
	Steps    uint64 // total upcaster invocations
	Failed   uint64 // events rejected (bad envelope, unknown type/version, missing upcaster)
}

type eventSchema struct {
	current   int
	upcasters map[int]Upcaster // from version -> upcaster to version+1
}

// Registry knows, per event type, the version the consumer code is written
// against and the upcasters that bring older versions up to it. It is safe
// for concurrent use.
type Registry struct {
	mu    sync.RWMutex
	types map[string]*eventSchema

	decoded, upcasted, steps, failed atomic.Uint64
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{types: make(map[string]*eventSchema)}
}

// Declare sets the current (newest understood) version of eventType.
// Calling it again with a higher version supports a consumer upgrade.
func (r *Registry) Declare(eventType string, current int) error {
	if eventType == "" || current < 1 {
		return fmt.Errorf("%w: type %q current %d", ErrBadRegistration, eventType, current)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.types[eventType]
	if s == nil {
		s = &eventSchema{upcasters: make(map[int]Upcaster)}
		r.types[eventType] = s
	}
	s.current = current
	return nil
}

// Register adds the upcaster from version from to from+1. The type must be
// declared, from+1 must not exceed the declared current version, and each
// step can only be registered once.
func (r *Registry) Register(eventType string, from int, up Upcaster) error {
	if up == nil || from < 1 {
		return fmt.Errorf("%w: %s v%d", ErrBadRegistration, eventType, from)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.types[eventType]
	if s == nil {
		return fmt.Errorf("%w: %q (declare it first)", ErrUnknownEventType, eventType)
	}
	if from >= s.current {
		return fmt.Errorf("%w: %s v%d->v%d is past current v%d", ErrBadRegistration, eventType, from, from+1, s.current)
	}
	if _, dup := s.upcasters[from]; dup {
		return fmt.Errorf("%w: %s v%d->v%d already registered", ErrBadRegistration, eventType, from, from+1)
	}
	s.upcasters[from] = up
	return nil
}

// Current returns the declared current version of eventType (0 if unknown).
func (r *Registry) Current(eventType string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if s := r.types[eventType]; s != nil {
		return s.current
	}
	return 0
}

// Upcast brings e to the current version of its type and returns the new
// envelope plus how many upcast steps ran. An event already at the current
// version is returned unchanged with 0 steps, so upcasting is idempotent.
// An event newer than the consumer understands is ErrUnknownVersion (an
// upcaster cannot invent the meaning of a future schema); a hole in the
// chain is ErrMissingUpcaster.
func (r *Registry) Upcast(e Envelope) (Envelope, int, error) {
	out, n, err := r.upcast(e)
	if err != nil {
		r.failed.Add(1)
		return Envelope{}, 0, err
	}
	r.decoded.Add(1)
	if n > 0 {
		r.upcasted.Add(1)
		r.steps.Add(uint64(n))
	}
	return out, n, nil
}

func (r *Registry) upcast(e Envelope) (Envelope, int, error) {
	r.mu.RLock()
	s := r.types[e.Type]
	var current int
	var chain []Upcaster
	if s != nil {
		current = s.current
		for v := e.Version; v >= 1 && v < current; v++ {
			up, ok := s.upcasters[v]
			if !ok {
				r.mu.RUnlock()
				return Envelope{}, 0, fmt.Errorf("%w: %s v%d->v%d", ErrMissingUpcaster, e.Type, v, v+1)
			}
			chain = append(chain, up)
		}
	}
	r.mu.RUnlock()

	switch {
	case s == nil:
		return Envelope{}, 0, fmt.Errorf("%w: %q", ErrUnknownEventType, e.Type)
	case e.Version < 1 || e.Version > current:
		return Envelope{}, 0, fmt.Errorf("%w: %s v%d (consumer understands up to v%d)", ErrUnknownVersion, e.Type, e.Version, current)
	case len(chain) == 0:
		return e, 0, nil // already current: nothing to do
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(e.Data, &fields); err != nil {
		return Envelope{}, 0, fmt.Errorf("%w: data is not an object: %v", ErrBadEnvelope, err)
	}
	if fields == nil {
		fields = make(map[string]json.RawMessage)
	}
	for i, up := range chain {
		if err := up(fields); err != nil {
			return Envelope{}, 0, fmt.Errorf("pubsub: upcast %s v%d->v%d: %w", e.Type, e.Version+i, e.Version+i+1, err)
		}
	}
	data, err := json.Marshal(fields)
	if err != nil {
		return Envelope{}, 0, err
	}
	return Envelope{Type: e.Type, Version: current, Data: data}, len(chain), nil
}

// Decode parses a raw envelope and upcasts it to the current version.
func (r *Registry) Decode(raw []byte) (Envelope, int, error) {
	e, err := DecodeEnvelope(raw)
	if err != nil {
		r.failed.Add(1)
		return Envelope{}, 0, err
	}
	return r.Upcast(e)
}

// Stats returns a snapshot of the registry's counters.
func (r *Registry) Stats() UpcastStats {
	return UpcastStats{
		Decoded:  r.decoded.Load(),
		Upcasted: r.upcasted.Load(),
		Steps:    r.steps.Load(),
		Failed:   r.failed.Load(),
	}
}

// ReadTolerant unmarshals data into out following the tolerant-reader rule:
// unknown fields are ignored, and fields missing from data keep whatever
// value out already holds, so callers pre-fill defaults before calling it.
// This is plain encoding/json behaviour, named here to make it a decision.
func ReadTolerant(data []byte, out any) error {
	return json.Unmarshal(data, out)
}

// ReadStrict unmarshals data into out but rejects unknown fields. It is the
// brittle reader: the first producer that adds a field breaks it.
func ReadStrict(data []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(out)
}
