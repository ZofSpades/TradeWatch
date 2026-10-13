package sim

import (
	"hash/fnv"
	"math"
	"math/rand/v2"
)

// streamRNG returns an independent PCG stream for (seed, label). Agents get
// their own stream keyed by a stable label, so adding or removing an agent
// never changes the random draws of any other agent.
func streamRNG(seed uint64, label string) *rand.Rand {
	h := fnv.New64a()
	h.Write([]byte(label))
	return rand.New(rand.NewPCG(seed, h.Sum64()))
}

// expNs draws an exponentially distributed interval with the given mean, in
// nanoseconds, of at least 1ns.
func expNs(r *rand.Rand, mean int64) int64 {
	d := int64(r.ExpFloat64() * float64(mean))
	if d < 1 {
		d = 1
	}
	return d
}

// jitterNs returns mean scaled by a uniform factor in [1-f, 1+f].
func jitterNs(r *rand.Rand, mean int64, f float64) int64 {
	d := int64(float64(mean) * (1 + f*(2*r.Float64()-1)))
	if d < 1 {
		d = 1
	}
	return d
}

// lognormal draws from a log-normal distribution with the given median.
func lognormal(r *rand.Rand, median, sigma float64) float64 {
	return median * math.Exp(sigma*r.NormFloat64())
}

// roundQty rounds q to a positive multiple of step.
func roundQty(q float64, step int64) int64 {
	n := int64(math.Round(q/float64(step))) * step
	if n < step {
		n = step
	}
	return n
}

func floorTick(p float64, tick int64) int64 { return int64(math.Floor(p/float64(tick))) * tick }
func ceilTick(p float64, tick int64) int64  { return int64(math.Ceil(p/float64(tick))) * tick }
