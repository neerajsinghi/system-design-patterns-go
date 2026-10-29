package vectorannindex

import "math/rand/v2"

// Clustered returns n synthetic vectors of length dim drawn from a mixture of
// Gaussian blobs, a rough stand-in for embeddings, which form topical
// clusters. Cluster centres are standard normal; each point is its centre
// plus N(0, spread²) noise per coordinate. Points are spread round-robin over
// the clusters. The same rng state always gives the same vectors.
func Clustered(rng *rand.Rand, n, dim, clusters int, spread float64) [][]float32 {
	if n <= 0 || dim <= 0 || clusters <= 0 {
		return nil
	}
	centres := make([][]float64, clusters)
	for c := range centres {
		centres[c] = make([]float64, dim)
		for j := range centres[c] {
			centres[c][j] = rng.NormFloat64()
		}
	}
	out := make([][]float32, n)
	for i := range out {
		c := centres[i%clusters]
		v := make([]float32, dim)
		for j := range v {
			v[j] = float32(c[j] + spread*rng.NormFloat64())
		}
		out[i] = v
	}
	return out
}

// Jitter returns a copy of v with N(0, sigma²) noise added to each
// coordinate: a query "near" a stored vector.
func Jitter(rng *rand.Rand, v []float32, sigma float64) []float32 {
	out := make([]float32, len(v))
	for j, x := range v {
		out[j] = x + float32(sigma*rng.NormFloat64())
	}
	return out
}
