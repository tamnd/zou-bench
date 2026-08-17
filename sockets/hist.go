package sockets

import "math"

// Hist is a fixed histogram of microsecond durations.
//
// A hundred thousand sockets at a thousand rows a second is a hundred
// million deliveries an hour, and a slice of samples that long is a
// generator that runs out of memory before the run runs out of time.
// The other drivers here keep every sample because a pgbench run has
// thousands of them, not tens of millions. This one keeps counts.
//
// The buckets are a hundred to the decade, so the width of a bucket is
// never more than one percent of what falls in it, and a percentile
// read off it is the bucket's upper bound rather than an interpolation.
// That is a ceiling, which is the only thing a histogram can honestly
// say: the true value is somewhere inside the bucket, and the bound is
// the most that may be claimed.
type Hist struct {
	buckets []uint64
	count   uint64
	sum     float64
	max     uint64
	min     uint64
	over    uint64
}

const (
	decades = 9 // 1us through 1000s
	// A decade is nine hundred buckets wide, each a hundredth of the
	// decade's own scale, so a bucket is never more than one percent of
	// the smallest value that can land in it.
	perDecade = 900
)

// NewHist makes an empty histogram.
func NewHist() *Hist {
	return &Hist{buckets: make([]uint64, decades*perDecade), min: math.MaxUint64}
}

// Add records one duration in microseconds. Zero and negative land in
// the first bucket, because a clock that ran backwards by a hair is a
// delivery that was very fast, not a delivery that did not happen.
func (h *Hist) Add(us int64) {
	if us < 0 {
		us = 0
	}
	v := uint64(us)
	h.count++
	h.sum += float64(v)
	if v > h.max {
		h.max = v
	}
	if v < h.min {
		h.min = v
	}
	i := index(v)
	if i < 0 {
		// Past a thousand seconds nothing is being measured any more,
		// but the count still has to be right, so it is counted apart
		// rather than folded into the top bucket.
		h.over++
		return
	}
	h.buckets[i]++
}

func index(v uint64) int {
	if v < 1 {
		return 0
	}
	decade, scale := 0, uint64(1)
	for scale*10 <= v {
		scale *= 10
		decade++
		if decade >= decades {
			return -1
		}
	}
	within := int((v - scale) * 100 / scale)
	if within >= perDecade {
		within = perDecade - 1
	}
	return decade*perDecade + within
}

// upper is the inclusive upper bound of bucket i in microseconds. The
// division is last so the arithmetic stays whole: a bound below the
// values inside its own bucket would be a percentile under the truth,
// which is the one error a benchmark may not make.
func upper(i int) uint64 {
	decade, within := i/perDecade, i%perDecade
	scale := uint64(1)
	for range decade {
		scale *= 10
	}
	return (scale*100 + (uint64(within)+1)*scale) / 100
}

// Merge folds another histogram in, which is how per worker counts
// become one run's number without a lock on the hot path.
func (h *Hist) Merge(other *Hist) {
	if other == nil || other.count == 0 {
		return
	}
	for i, n := range other.buckets {
		h.buckets[i] += n
	}
	h.count += other.count
	h.sum += other.sum
	h.over += other.over
	if other.max > h.max {
		h.max = other.max
	}
	if other.min < h.min {
		h.min = other.min
	}
}

// Count is how many durations were recorded.
func (h *Hist) Count() uint64 { return h.count }

// Quantile reads a percentile in milliseconds. An empty histogram has
// no percentiles, which is not the same as zero.
func (h *Hist) Quantile(q float64) (float64, bool) {
	if h.count == 0 {
		return 0, false
	}
	want := uint64(math.Ceil(float64(h.count) * q))
	if want == 0 {
		want = 1
	}
	var seen uint64
	for i, n := range h.buckets {
		seen += n
		if seen >= want {
			return ms(upper(i)), true
		}
	}
	return ms(h.max), true
}

// Percentiles is the same shape the other drivers report, so a socket
// run and a rest run read the same way in a result file.
func (h *Hist) Percentiles() map[string]float64 {
	out := map[string]float64{}
	if h.count == 0 {
		return out
	}
	for name, q := range map[string]float64{
		"p50": 0.50, "p90": 0.90, "p95": 0.95, "p99": 0.99, "p999": 0.999,
	} {
		if v, ok := h.Quantile(q); ok {
			out[name] = v
		}
	}
	out["min"] = ms(h.min)
	out["max"] = ms(h.max)
	out["mean"] = round3(h.sum / float64(h.count) / 1000)
	return out
}

func ms(us uint64) float64 { return round3(float64(us) / 1000) }

func round3(v float64) float64 { return math.Round(v*1000) / 1000 }
