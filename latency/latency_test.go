package latency

import "testing"

func TestPercentilesOverAKnownDistribution(t *testing.T) {
	var samples []Sample
	for i := 1; i <= 100; i++ {
		samples = append(samples, Sample{MS: float64(i), Epoch: int64(i)})
	}
	p := Percentiles(samples)
	if p["p50"] != 51 || p["p99"] != 100 || p["max"] != 100 {
		t.Fatalf("percentiles = %v", p)
	}
	if p["mean"] != 50.5 {
		t.Fatalf("mean = %v", p["mean"])
	}
}

func TestNothingMeasuredIsNotZero(t *testing.T) {
	if p := Percentiles(nil); len(p) != 0 {
		t.Fatalf("an empty run reported %v", p)
	}
	if b := Buckets(nil, 30); b != nil {
		t.Fatalf("an empty run bucketed to %v", b)
	}
}

func TestBucketsSplitTheRunAndKeepTheirOwnTails(t *testing.T) {
	var samples []Sample
	// Ten seconds of one sample a second, and the last five are slow,
	// which an average over the whole run would hide.
	for i := range 10 {
		ms := 1.0
		if i >= 5 {
			ms = 100
		}
		samples = append(samples, Sample{MS: ms, Epoch: int64(1000 + i)})
	}
	b := Buckets(samples, 5)
	if len(b) != 2 {
		t.Fatalf("buckets = %v", b)
	}
	if b[0].P50 != 1 || b[1].P50 != 100 {
		t.Fatalf("the stall did not land in one bucket: %v", b)
	}
	if b[0].Second != 0 || b[1].Second != 5 || b[0].Count != 5 {
		t.Fatalf("windows = %v", b)
	}
}
