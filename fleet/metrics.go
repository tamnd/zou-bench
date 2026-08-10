package fleet

import (
	"bufio"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Metrics is one scrape, series name and labels as written, joined by
// the raw line so `zou_tenant_attaches_total{outcome="ok"}` is one key.
type Metrics map[string]float64

// Scrape reads the node's own numbers off its ops port. The node is the
// only thing that can see an attach it did not do for a request, an
// eviction at the ceiling, or how long a start took from the inside, so
// a fleet run reads both sides: what the client waited for, and what
// the node says it was doing.
func Scrape(url string) (Metrics, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	res, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", url, res.Status)
	}
	out := Metrics{}
	sc := bufio.NewScanner(res.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.LastIndex(line, " ")
		if i < 0 {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(line[i+1:]), 64)
		if err != nil {
			continue
		}
		out[strings.TrimSpace(line[:i])] = v
	}
	return out, sc.Err()
}

// Get reads one series, or zero when the node never emitted it. Zero is
// the right answer for a counter nothing has incremented, which is not
// the same as a scrape that failed, and a scrape that failed is an
// error at the call above this one.
func (m Metrics) Get(key string) float64 { return m[key] }

// Delta is what happened between two scrapes. Gauges are meaningless
// here and are dropped by the caller picking counters; a negative
// result means the node restarted mid phase and is kept as it is rather
// than clamped, because a hidden restart is worse than a negative
// number in a table.
func Delta(before, after Metrics) Metrics {
	out := Metrics{}
	for k, v := range after {
		out[k] = v - before[k]
	}
	return out
}

// AttachSeconds is the node's own view of how long its attaches took
// over an interval, mean seconds per attach out of the histogram's sum
// and count. It is not a percentile: the buckets are in the scrape too
// and the caller keeps them, but the mean is the one number that always
// exists, and an interval with no attaches has no mean at all.
func AttachSeconds(d Metrics) (float64, bool) {
	count := d["zou_tenant_attach_seconds_count"]
	if count <= 0 {
		return 0, false
	}
	return d["zou_tenant_attach_seconds_sum"] / count, true
}

// Buckets pulls one histogram's cumulative buckets out of a scrape in
// ascending order of le, which is what turns a delta into a percentile
// somebody can defend.
func Buckets(m Metrics, name string) []Bucket {
	var out []Bucket
	prefix := name + "_bucket{le=\""
	for k, v := range m {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		rest := strings.TrimPrefix(k, prefix)
		end := strings.Index(rest, "\"")
		if end < 0 {
			continue
		}
		le, err := strconv.ParseFloat(rest[:end], 64)
		if err != nil {
			// +Inf is the last bucket and its count is the total,
			// which _count already carries.
			continue
		}
		out = append(out, Bucket{LE: le, Count: v})
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].LE < out[j-1].LE; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// Bucket is one cumulative histogram bucket.
type Bucket struct {
	LE    float64 `json:"le"`
	Count float64 `json:"count"`
}

// Quantile reads a percentile off cumulative buckets, returning the
// upper bound of the bucket the rank falls in. That is a ceiling and
// not an interpolation, which is the only honest read of a histogram:
// the true value is somewhere inside the bucket and the bound is the
// most that can be claimed.
func Quantile(buckets []Bucket, q float64) (float64, bool) {
	if len(buckets) == 0 {
		return 0, false
	}
	total := buckets[len(buckets)-1].Count
	if total <= 0 {
		return 0, false
	}
	want := total * q
	for _, b := range buckets {
		if b.Count >= want {
			return b.LE, true
		}
	}
	return buckets[len(buckets)-1].LE, true
}
