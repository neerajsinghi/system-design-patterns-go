// Package geospatialindex implements a geohash-based proximity index for
// moving objects: "find the drivers within 2 km of me" without scanning every
// driver.
//
// A full scan computes a distance to every object for every query: O(N). With
// 100k drivers sending a new position every few seconds and thousands of
// searches per second, almost all of that work is spent on drivers that are
// tens of kilometres away.
//
// A geohash turns a (lat, lon) pair into a short string by repeatedly halving
// the world: one bit for "east or west half of the current longitude range",
// one for "north or south half of the latitude range", alternating, five bits
// per base32 character. Every character added shrinks the cell by a factor of
// 32, and points that share a prefix are in the same cell. That makes a cell
// a bucket: the index keeps, for every prefix length up to its storage
// precision, a map from cell to the set of IDs inside it.
//
// A query picks the finest cell size that is at least as tall and as wide as
// the search radius, takes the query point's cell plus its 8 neighbours (a
// point near the edge of its cell has matches in the cell next door), drops
// neighbours the circle can't reach, and then filters the candidates by exact
// haversine distance. Neighbours wrap across the antimeridian. Near a pole,
// where cells get very narrow and the circle may cross the pole, no cell size
// fits and the query falls back to a full scan, so results stay correct.
package geospatialindex

import (
	"errors"
	"math"
	"sort"
	"sync"
	"sync/atomic"
)

// MaxPrecision is the longest geohash supported (60 bits, about 3.7 cm by
// 1.9 cm per cell).
const MaxPrecision = 12

// EarthRadiusMeters is the mean Earth radius used by Distance.
const EarthRadiusMeters = 6371008.8

const base32 = "0123456789bcdefghjkmnpqrstuvwxyz"

var decodeTable = func() [256]int8 {
	var t [256]int8
	for i := range t {
		t[i] = -1
	}
	for i := range len(base32) {
		t[base32[i]] = int8(i)
	}
	return t
}()

var (
	errLat       = errors.New("geospatialindex: latitude must be in [-90, 90]")
	errLon       = errors.New("geospatialindex: longitude must be in [-180, 180]")
	errPrecision = errors.New("geospatialindex: precision must be in [1, 12]")
	errHash      = errors.New("geospatialindex: invalid geohash")
)

func validPoint(lat, lon float64) error {
	if math.IsNaN(lat) || lat < -90 || lat > 90 {
		return errLat
	}
	if math.IsNaN(lon) || lon < -180 || lon > 180 {
		return errLon
	}
	return nil
}

// Encode returns the geohash of (lat, lon) with the given number of
// characters. Bits alternate longitude, latitude, longitude, ... starting
// with longitude, and each group of 5 bits becomes one base32 character.
func Encode(lat, lon float64, precision int) (string, error) {
	if err := validPoint(lat, lon); err != nil {
		return "", err
	}
	if precision < 1 || precision > MaxPrecision {
		return "", errPrecision
	}
	latLo, latHi := -90.0, 90.0
	lonLo, lonHi := -180.0, 180.0
	buf := make([]byte, precision)
	isLon := true
	ch, bits := 0, 0
	for i := 0; i < precision; {
		if isLon {
			mid := (lonLo + lonHi) / 2
			if lon >= mid {
				ch = ch<<1 | 1
				lonLo = mid
			} else {
				ch <<= 1
				lonHi = mid
			}
		} else {
			mid := (latLo + latHi) / 2
			if lat >= mid {
				ch = ch<<1 | 1
				latLo = mid
			} else {
				ch <<= 1
				latHi = mid
			}
		}
		isLon = !isLon
		if bits++; bits == 5 {
			buf[i] = base32[ch]
			i++
			ch, bits = 0, 0
		}
	}
	return string(buf), nil
}

// Box is the area covered by a geohash cell.
type Box struct {
	MinLat, MinLon, MaxLat, MaxLon float64
}

// Center returns the middle of the box.
func (b Box) Center() (lat, lon float64) {
	return (b.MinLat + b.MaxLat) / 2, (b.MinLon + b.MaxLon) / 2
}

// Decode returns the bounding box of a geohash cell.
func Decode(hash string) (Box, error) {
	if len(hash) < 1 || len(hash) > MaxPrecision {
		return Box{}, errHash
	}
	b := Box{MinLat: -90, MaxLat: 90, MinLon: -180, MaxLon: 180}
	isLon := true
	for i := range len(hash) {
		v := decodeTable[hash[i]]
		if v < 0 {
			return Box{}, errHash
		}
		for bit := 4; bit >= 0; bit-- {
			one := v>>bit&1 == 1
			if isLon {
				mid := (b.MinLon + b.MaxLon) / 2
				if one {
					b.MinLon = mid
				} else {
					b.MaxLon = mid
				}
			} else {
				mid := (b.MinLat + b.MaxLat) / 2
				if one {
					b.MinLat = mid
				} else {
					b.MaxLat = mid
				}
			}
			isLon = !isLon
		}
	}
	return b, nil
}

// cellSize returns a cell's height and width in degrees at a precision.
func cellSize(precision int) (latDeg, lonDeg float64) {
	bits := 5 * precision
	lonBits := (bits + 1) / 2
	latBits := bits / 2
	return 180 / math.Exp2(float64(latBits)), 360 / math.Exp2(float64(lonBits))
}

// Indexes into the array returned by Neighbours.
const (
	North = iota
	NorthEast
	East
	SouthEast
	South
	SouthWest
	West
	NorthWest
)

// Neighbours returns the 8 cells of the same precision around hash, indexed
// by North, NorthEast, ... NorthWest. East and west wrap across the
// antimeridian (longitude 180 is next to -180). There is nothing north of a
// cell that touches the north pole (or south of one touching the south pole),
// so those entries are "". The cells across the pole (same latitude row,
// longitude + 180) are not returned; Index.Nearby handles polar queries with
// a full scan instead.
func Neighbours(hash string) ([8]string, error) {
	var out [8]string
	b, err := Decode(hash)
	if err != nil {
		return out, err
	}
	lat, lon := b.Center()
	h, w := b.MaxLat-b.MinLat, b.MaxLon-b.MinLon
	offsets := [8][2]float64{
		North: {1, 0}, NorthEast: {1, 1}, East: {0, 1}, SouthEast: {-1, 1},
		South: {-1, 0}, SouthWest: {-1, -1}, West: {0, -1}, NorthWest: {1, -1},
	}
	for i, o := range offsets {
		nlat := lat + o[0]*h
		if nlat > 90 || nlat < -90 {
			continue
		}
		out[i], _ = Encode(nlat, wrapLon(lon+o[1]*w), len(hash))
	}
	return out, nil
}

func wrapLon(lon float64) float64 {
	for lon > 180 {
		lon -= 360
	}
	for lon < -180 {
		lon += 360
	}
	return lon
}

func rad(d float64) float64 { return d * math.Pi / 180 }

// Distance returns the great-circle (haversine) distance in metres.
func Distance(lat1, lon1, lat2, lon2 float64) float64 {
	return distance(lat1, lon1, math.Cos(rad(lat1)), lat2, lon2, math.Cos(rad(lat2)))
}

// distance is haversine with the cosines of both latitudes precomputed: the
// index stores cos(lat) per point and computes the query's once.
func distance(lat1, lon1, cos1, lat2, lon2, cos2 float64) float64 {
	sLat := math.Sin(rad(lat2-lat1) / 2)
	sLon := math.Sin(rad(lon2-lon1) / 2)
	a := sLat*sLat + cos1*cos2*sLon*sLon
	return 2 * EarthRadiusMeters * math.Asin(math.Min(1, math.Sqrt(a)))
}

const metersPerDegLat = EarthRadiusMeters * math.Pi / 180

// Result is one match from a proximity query.
type Result struct {
	ID       string
	Lat, Lon float64
	Meters   float64
}

// QueryStats describes how a query was answered.
type QueryStats struct {
	// Precision is the geohash length of the cells searched (0 for a scan).
	Precision int
	// CellsSearched is how many cells were read (at most 9).
	CellsSearched int
	// Candidates is how many IDs had their exact distance computed.
	Candidates int
	// FullScan is true when no cell size fitted the radius (huge radius, or a
	// circle reaching a pole) or BruteForce was called.
	FullScan bool
}

// Stats counts index activity.
type Stats struct {
	IDs         int
	Updates     uint64
	CellChanges uint64 // updates that moved an ID to a different storage cell
	Queries     uint64 // Nearby calls (BruteForce is not counted)
	FullScans   uint64 // Nearby calls that had to scan everything
}

type position struct {
	lat, lon float64
	cosLat   float64
	hash     string // geohash at the storage precision
}

// Index is a concurrency-safe geohash index of moving IDs.
type Index struct {
	precision int

	mu     sync.RWMutex
	pos    map[string]position
	levels []map[string]map[string]struct{} // levels[p][prefix of length p] = IDs
	stats  Stats                            // Updates and CellChanges, guarded by mu

	queries, fullScans atomic.Uint64
}

// Option configures an Index.
type Option func(*Index) error

// WithPrecision sets the storage precision: the longest prefix kept, which
// is also the finest cell a query can use. Default 7 (about 153 m by 153 m).
func WithPrecision(p int) Option {
	return func(ix *Index) error {
		if p < 1 || p > MaxPrecision {
			return errPrecision
		}
		ix.precision = p
		return nil
	}
}

// New returns an empty Index.
func New(opts ...Option) (*Index, error) {
	ix := &Index{precision: 7, pos: make(map[string]position)}
	for _, o := range opts {
		if o == nil {
			return nil, errors.New("geospatialindex: nil option")
		}
		if err := o(ix); err != nil {
			return nil, err
		}
	}
	ix.levels = make([]map[string]map[string]struct{}, ix.precision+1)
	for p := 1; p <= ix.precision; p++ {
		ix.levels[p] = make(map[string]map[string]struct{})
	}
	return ix, nil
}

// Precision returns the storage precision.
func (ix *Index) Precision() int { return ix.precision }

// Update inserts id at (lat, lon), or moves it there. Only the cells whose
// prefix actually changed are touched, so a driver creeping along inside its
// cell costs one map write.
func (ix *Index) Update(id string, lat, lon float64) error {
	if id == "" {
		return errors.New("geospatialindex: empty id")
	}
	hash, err := Encode(lat, lon, ix.precision)
	if err != nil {
		return err
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.stats.Updates++
	old, existed := ix.pos[id]
	ix.pos[id] = position{lat: lat, lon: lon, cosLat: math.Cos(rad(lat)), hash: hash}
	if existed && old.hash == hash {
		return nil
	}
	if existed {
		ix.stats.CellChanges++
	}
	for p := 1; p <= ix.precision; p++ {
		cell := hash[:p]
		if existed && old.hash[:p] == cell {
			continue
		}
		if existed {
			ix.removeFrom(p, old.hash[:p], id)
		}
		set := ix.levels[p][cell]
		if set == nil {
			set = make(map[string]struct{})
			ix.levels[p][cell] = set
		}
		set[id] = struct{}{}
	}
	return nil
}

func (ix *Index) removeFrom(p int, cell, id string) {
	set := ix.levels[p][cell]
	delete(set, id)
	if len(set) == 0 {
		delete(ix.levels[p], cell)
	}
}

// Remove deletes id. It reports whether id was present.
func (ix *Index) Remove(id string) bool {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	old, ok := ix.pos[id]
	if !ok {
		return false
	}
	delete(ix.pos, id)
	for p := 1; p <= ix.precision; p++ {
		ix.removeFrom(p, old.hash[:p], id)
	}
	return true
}

// Len returns the number of IDs in the index.
func (ix *Index) Len() int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return len(ix.pos)
}

// Position returns id's last known position.
func (ix *Index) Position(id string) (lat, lon float64, ok bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	p, ok := ix.pos[id]
	return p.lat, p.lon, ok
}

// Cell returns id's geohash at the storage precision.
func (ix *Index) Cell(id string) (string, bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	p, ok := ix.pos[id]
	return p.hash, ok
}

// IDsInCell returns the IDs in a cell, sorted. The hash may be any length
// from 1 to the storage precision.
func (ix *Index) IDsInCell(hash string) ([]string, error) {
	if _, err := Decode(hash); err != nil {
		return nil, err
	}
	if len(hash) > ix.precision {
		return nil, errors.New("geospatialindex: hash longer than storage precision")
	}
	ix.mu.RLock()
	set := ix.levels[len(hash)][hash]
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	ix.mu.RUnlock()
	sort.Strings(ids)
	return ids, nil
}

// Stats returns a snapshot of the counters.
func (ix *Index) Stats() Stats {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	s := ix.stats
	s.IDs = len(ix.pos)
	s.Queries = ix.queries.Load()
	s.FullScans = ix.fullScans.Load()
	return s
}

func validQuery(lat, lon, radius float64, limit int) error {
	if err := validPoint(lat, lon); err != nil {
		return err
	}
	if math.IsNaN(radius) || math.IsInf(radius, 0) || radius <= 0 {
		return errors.New("geospatialindex: radius must be positive and finite")
	}
	if limit < 0 {
		return errors.New("geospatialindex: limit must be >= 0 (0 means no limit)")
	}
	return nil
}

// Nearby returns the IDs within radiusMeters of (lat, lon), closest first
// (ties broken by ID). limit 0 means all of them.
func (ix *Index) Nearby(lat, lon, radiusMeters float64, limit int) ([]Result, error) {
	r, _, err := ix.NearbyStats(lat, lon, radiusMeters, limit)
	return r, err
}

// queryPrecision returns the finest precision (<= max) whose cells are at
// least radius tall and radius wide everywhere the circle reaches, or 0. With
// such cells the circle around a point can't extend past the 3x3 block
// around the point's own cell.
func queryPrecision(lat, radius float64, max int) int {
	dLat := radius / metersPerDegLat
	edge := math.Abs(lat) + dLat // the circle's latitude closest to a pole
	if edge >= 90 {
		return 0 // the circle touches or crosses a pole
	}
	cosEdge := math.Cos(rad(edge))
	for p := max; p >= 1; p-- {
		hDeg, wDeg := cellSize(p)
		if hDeg*metersPerDegLat >= radius && wDeg*metersPerDegLat*cosEdge >= radius {
			return p
		}
	}
	return 0
}

// NearbyStats is Nearby plus a description of the work done.
func (ix *Index) NearbyStats(lat, lon, radiusMeters float64, limit int) ([]Result, QueryStats, error) {
	if err := validQuery(lat, lon, radiusMeters, limit); err != nil {
		return nil, QueryStats{}, err
	}
	p := queryPrecision(lat, radiusMeters, ix.precision)
	if p == 0 {
		return ix.scan(lat, lon, radiusMeters, limit, true)
	}

	// Bounding box of the circle, used to skip neighbours it can't reach.
	dLat := radiusMeters / metersPerDegLat
	dLon := dLat / math.Cos(rad(math.Abs(lat)+dLat))
	q := Box{MinLat: lat - dLat, MaxLat: lat + dLat, MinLon: lon - dLon, MaxLon: lon + dLon}

	own, _ := Encode(lat, lon, p)
	nb, _ := Neighbours(own)
	cells := make([]string, 0, 9)
	seen := map[string]bool{}
	for _, c := range append([]string{own}, nb[:]...) {
		if c == "" || seen[c] {
			continue
		}
		seen[c] = true
		if b, _ := Decode(c); c != own && !intersects(b, q) {
			continue
		}
		cells = append(cells, c)
	}

	st := QueryStats{Precision: p, CellsSearched: len(cells)}
	var out []Result
	qcos := math.Cos(rad(lat))
	ix.mu.RLock()
	for _, c := range cells {
		for id := range ix.levels[p][c] {
			st.Candidates++
			pp := ix.pos[id]
			if d := distance(lat, lon, qcos, pp.lat, pp.lon, pp.cosLat); d <= radiusMeters {
				out = append(out, Result{ID: id, Lat: pp.lat, Lon: pp.lon, Meters: d})
			}
		}
	}
	ix.mu.RUnlock()
	ix.queries.Add(1)
	return finish(out, limit), st, nil
}

// intersects reports whether cell b overlaps query box q, trying q shifted by
// ±360° so boxes that cross the antimeridian are handled.
func intersects(b, q Box) bool {
	if b.MaxLat < q.MinLat || b.MinLat > q.MaxLat {
		return false
	}
	for _, shift := range []float64{-360, 0, 360} {
		if b.MaxLon >= q.MinLon+shift && b.MinLon <= q.MaxLon+shift {
			return true
		}
	}
	return false
}

// BruteForce answers the same query by computing the distance to every ID.
// It is the O(N) baseline the index exists to avoid, and a correctness
// oracle for it.
func (ix *Index) BruteForce(lat, lon, radiusMeters float64, limit int) ([]Result, QueryStats, error) {
	if err := validQuery(lat, lon, radiusMeters, limit); err != nil {
		return nil, QueryStats{}, err
	}
	return ix.scan(lat, lon, radiusMeters, limit, false)
}

func (ix *Index) scan(lat, lon, radius float64, limit int, count bool) ([]Result, QueryStats, error) {
	var out []Result
	qcos := math.Cos(rad(lat))
	ix.mu.RLock()
	st := QueryStats{FullScan: true, Candidates: len(ix.pos)}
	for id, pp := range ix.pos {
		if d := distance(lat, lon, qcos, pp.lat, pp.lon, pp.cosLat); d <= radius {
			out = append(out, Result{ID: id, Lat: pp.lat, Lon: pp.lon, Meters: d})
		}
	}
	ix.mu.RUnlock()
	if count {
		ix.queries.Add(1)
		ix.fullScans.Add(1)
	}
	return finish(out, limit), st, nil
}

func finish(out []Result, limit int) []Result {
	sort.Slice(out, func(i, j int) bool {
		if out[i].Meters != out[j].Meters {
			return out[i].Meters < out[j].Meters
		}
		return out[i].ID < out[j].ID
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}
