package sockets

import "testing"

func TestAPercentileIsTheBucketBoundAndNeverBelowTheTruth(t *testing.T) {
	h := NewHist()
	for i := range 1000 {
		h.Add(int64(i + 1)) // 1us through 1000us
	}
	if h.Count() != 1000 {
		t.Fatalf("counted %d", h.Count())
	}
	p99, ok := h.Quantile(0.99)
	if !ok {
		t.Fatal("a thousand samples gave no p99")
	}
	// The true p99 of 1..1000us is 990us, and the bound of the bucket it
	// falls in is a whisker above that. A percentile below the truth
	// would be the one kind of error that cannot be defended.
	if p99 < 0.990 || p99 > 1.0 {
		t.Errorf("p99 %v ms is not the bucket 990us falls in", p99)
	}
	if got := h.Percentiles()["max"]; got != 1.0 {
		t.Errorf("max %v ms, the largest sample was 1000us", got)
	}
	if got := h.Percentiles()["min"]; got != 0.001 {
		t.Errorf("min %v ms, the smallest sample was 1us", got)
	}
}

func TestABucketIsNeverWiderThanOnePercentOfWhatIsInIt(t *testing.T) {
	for _, v := range []uint64{1, 9, 10, 99, 100, 1234, 999_999, 1_000_000, 123_456_789} {
		i := index(v)
		if i < 0 {
			t.Fatalf("%d fell off the end", v)
		}
		bound := upper(i)
		if bound < v {
			t.Errorf("%d landed in a bucket whose bound is %d", v, bound)
		}
		if float64(bound-v)/float64(v) > 0.01 {
			t.Errorf("%d landed in a bucket bounded at %d, wider than one percent", v, bound)
		}
	}
}

func TestAnEmptyHistogramHasNoPercentilesRatherThanZeros(t *testing.T) {
	h := NewHist()
	if _, ok := h.Quantile(0.5); ok {
		t.Error("an empty histogram answered a percentile")
	}
	if len(h.Percentiles()) != 0 {
		t.Errorf("an empty histogram published %v", h.Percentiles())
	}
}

func TestMergingIsWhatMakesPerWorkerCountsOneNumber(t *testing.T) {
	a, b := NewHist(), NewHist()
	for i := range 500 {
		a.Add(int64(i + 1))
		b.Add(int64(i + 501))
	}
	whole := NewHist()
	whole.Merge(a)
	whole.Merge(b)
	whole.Merge(nil)
	if whole.Count() != 1000 {
		t.Fatalf("merged to %d", whole.Count())
	}
	if got := whole.Percentiles()["max"]; got != 1.0 {
		t.Errorf("max %v ms after a merge", got)
	}
	if got := whole.Percentiles()["min"]; got != 0.001 {
		t.Errorf("min %v ms after a merge", got)
	}
}

// Past a thousand seconds there is nothing left to measure, but the
// count still has to add up, so those land apart rather than in the top
// bucket where they would bend a percentile.
func TestSomethingAbsurdlyLateIsStillCounted(t *testing.T) {
	h := NewHist()
	h.Add(1000)
	h.Add(2_000_000_000_000)
	if h.Count() != 2 || h.over != 1 {
		t.Fatalf("count %d, over %d", h.Count(), h.over)
	}
	if got := h.Percentiles()["max"]; got != 2_000_000_000 {
		t.Errorf("max %v ms", got)
	}
}
