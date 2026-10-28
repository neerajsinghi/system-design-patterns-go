package sharding

import "errors"

// NumSlots is the number of hash slots, the same as Redis Cluster.
const NumSlots = 16384

// KeySlot returns the hash slot for key: CRC16 (XMODEM) of the key, modulo
// NumSlots. If the key contains a hash tag, a non-empty substring between the
// first '{' and the first '}' after it, only the tag is hashed. That lets
// related keys such as "{user:42}:profile" and "{user:42}:cart" share a slot,
// and so a shard, which multi-key operations need.
func KeySlot(key string) int {
	return int(crc16([]byte(hashTag(key))) % NumSlots)
}

func hashTag(key string) string {
	for i := 0; i < len(key); i++ {
		if key[i] != '{' {
			continue
		}
		for j := i + 1; j < len(key); j++ {
			if key[j] == '}' {
				if j > i+1 {
					return key[i+1 : j]
				}
				return key // "{}" is not a tag: hash the whole key
			}
		}
		return key
	}
	return key
}

// crc16 is CRC-16/XMODEM (polynomial 0x1021, initial value 0), as used by Redis Cluster.
func crc16(b []byte) uint16 {
	var crc uint16
	for _, c := range b {
		crc ^= uint16(c) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

// SlotRouter is hash-slot routing: keys hash to one of NumSlots fixed slots,
// and a table assigns each slot to a shard. Resharding moves whole slots
// between shards instead of changing the hash function, so adding a shard
// moves only the slots it takes over (about 1/(N+1) of the keys), not most
// of them as with hash % N.
type SlotRouter struct {
	owner []uint16 // slot -> shard
	n     int
}

// NewSlotRouter returns a router that splits the slots into n contiguous,
// near-equal ranges, one per shard.
func NewSlotRouter(n int) (SlotRouter, error) {
	if n <= 0 || n > NumSlots {
		return SlotRouter{}, errors.New("sharding: shard count must be in [1, 16384]")
	}
	owner := make([]uint16, NumSlots)
	for s := range owner {
		owner[s] = uint16(s * n / NumSlots)
	}
	return SlotRouter{owner: owner, n: n}, nil
}

// ShardFor implements Router.
func (r SlotRouter) ShardFor(key string) int {
	if r.n == 0 {
		return 0 // zero-value router; NewCluster rejects it
	}
	return int(r.owner[KeySlot(key)])
}

// ShardsForRange implements Router. Like any hash, slots destroy key order,
// so every shard may hold keys in the range.
func (r SlotRouter) ShardsForRange(_, _ string) []int {
	all := make([]int, r.n)
	for i := range all {
		all[i] = i
	}
	return all
}

// NumShards implements Router.
func (r SlotRouter) NumShards() int { return r.n }

// SlotsPerShard returns how many slots each shard owns.
func (r SlotRouter) SlotsPerShard() []int {
	counts := make([]int, r.n)
	for _, sh := range r.owner {
		counts[sh]++
	}
	return counts
}

// AddShard returns a new router with one more shard. Every existing shard
// hands its excess slots (its highest-numbered ones) to the new shard until
// each owns NumSlots/(N+1); the division's remainder ends up on the new
// shard. The receiver is unchanged.
func (r SlotRouter) AddShard() (SlotRouter, error) {
	if r.n == 0 || r.n >= NumSlots {
		return SlotRouter{}, errors.New("sharding: cannot add a shard to this router")
	}
	next := SlotRouter{owner: append([]uint16(nil), r.owner...), n: r.n + 1}
	target := NumSlots / next.n
	excess := r.SlotsPerShard()
	for i := range excess {
		excess[i] -= target
	}
	for s := NumSlots - 1; s >= 0; s-- {
		if sh := next.owner[s]; excess[sh] > 0 {
			next.owner[s] = uint16(r.n)
			excess[sh]--
		}
	}
	return next, nil
}
