// Package latency turns a pile of timed operations into the numbers a
// target is written in. pgbench transactions and REST requests are the
// same shape once they are measured, a millisecond and the second it
// happened in, so both drivers hand their samples here and both get
// distributions that can be compared line for line.
package latency

import (
	"math"
	"slices"
	"sort"
)

// Sample is one timed operation.
type Sample struct {
	MS float64
	// Epoch is the wall clock second the operation finished in, which
	// is what the buckets group by.
	Epoch int64
}

// Percentiles computes the distribution over every sample. An empty
// input gives an empty map rather than zeros, because a run that
// measured nothing must not read as a run that measured zero.
func Percentiles(samples []Sample) map[string]float64 {
	out := map[string]float64{}
	if len(samples) == 0 {
		return out
	}
	values := make([]float64, len(samples))
	for i, s := range samples {
		values[i] = s.MS
	}
	sort.Float64s(values)
	pick := func(p float64) float64 {
		idx := int(float64(len(values)) * p)
		if idx >= len(values) {
			idx = len(values) - 1
		}
		return values[idx]
	}
	out["p50"] = round3(pick(0.50))
	out["p90"] = round3(pick(0.90))
	out["p95"] = round3(pick(0.95))
	out["p99"] = round3(pick(0.99))
	out["p999"] = round3(pick(0.999))
	out["max"] = round3(values[len(values)-1])
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(len(values))
	out["mean"] = round3(mean)
	varsum := 0.0
	for _, v := range values {
		varsum += (v - mean) * (v - mean)
	}
	out["stddev"] = round3(math.Sqrt(varsum / float64(len(values))))
	return out
}

// Bucket is one progress window over the run.
type Bucket struct {
	Second int64   `json:"t"`
	Count  int     `json:"n"`
	Rate   float64 `json:"rate"`
	P50    float64 `json:"p50"`
	P99    float64 `json:"p99"`
}

// Buckets slices the run into windows of width seconds, so an average
// that hides a stall has somewhere to be caught.
func Buckets(samples []Sample, width int64) []Bucket {
	if len(samples) == 0 || width <= 0 {
		return nil
	}
	byWindow := map[int64][]float64{}
	var start int64 = math.MaxInt64
	for _, s := range samples {
		if s.Epoch < start {
			start = s.Epoch
		}
	}
	for _, s := range samples {
		w := (s.Epoch - start) / width
		byWindow[w] = append(byWindow[w], s.MS)
	}
	windows := make([]int64, 0, len(byWindow))
	for w := range byWindow {
		windows = append(windows, w)
	}
	slices.Sort(windows)
	var out []Bucket
	for _, w := range windows {
		vals := byWindow[w]
		sort.Float64s(vals)
		p50 := vals[len(vals)/2]
		p99 := vals[min(len(vals)*99/100, len(vals)-1)]
		out = append(out, Bucket{
			Second: w * width,
			Count:  len(vals),
			Rate:   round3(float64(len(vals)) / float64(width)),
			P50:    round3(p50),
			P99:    round3(p99),
		})
	}
	return out
}

func round3(v float64) float64 { return math.Round(v*1000) / 1000 }
