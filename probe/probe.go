package probe

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"time"
)

// Dist mirrors zou's OpDist, the four anchors its quantile curve
// interpolates through. Field names match the serde names in
// crates/zou-store/src/sim.rs, that compatibility is the whole point
// of this package.
type Dist struct {
	P50 float64 `json:"p50_ms"`
	P95 float64 `json:"p95_ms"`
	P99 float64 `json:"p99_ms"`
	Max float64 `json:"max_ms"`
}

// Profile is a calibration file ZOU_STORE_SIM loads by path. It beats
// the built in profiles because every number in it was measured.
type Profile struct {
	Name     string  `json:"name"`
	Get      Dist    `json:"get"`
	Put      Dist    `json:"put"`
	List     Dist    `json:"list"`
	Delete   Dist    `json:"delete"`
	Mbps     float64 `json:"mbps"`
	Slowdown float64 `json:"slowdown"`
}

// Options shape a probe run. Zero values take the defaults below.
type Options struct {
	Samples int    // requests per op kind, default 200
	Payload int    // small object bytes, the latency curve pays this size, default 8192
	Large   int    // large object bytes for the throughput estimate, default 16 MiB
	LargeN  int    // large round trips per direction, default 3
	Prefix  string // key prefix inside the bucket, default zoubench-probe
	Name    string // profile name written into the file
}

func (o *Options) defaults() {
	if o.Samples <= 0 {
		o.Samples = 200
	}
	if o.Payload <= 0 {
		o.Payload = 8192
	}
	if o.Large <= 0 {
		o.Large = 16 << 20
	}
	if o.LargeN <= 0 {
		o.LargeN = 3
	}
	if o.Prefix == "" {
		o.Prefix = "zoubench-probe"
	}
	if o.Name == "" {
		o.Name = "probed"
	}
}

// Summary is what a probe run produced beyond the profile itself:
// how many requests it took and how many of them the provider
// throttled, so the printed report can show its work.
type Summary struct {
	Profile   Profile
	Attempts  int
	Slowdowns int
}

// Run measures the endpoint and returns a calibrated profile. Requests
// run sequentially on purpose: the profile models the latency one call
// pays, and concurrent probes would measure our own queueing instead.
// Every key the probe writes is deleted before it returns, the deletes
// being the delete measurement.
func Run(c *Client, opt Options, progress func(string)) (Summary, error) {
	opt.defaults()
	if progress == nil {
		progress = func(string) {}
	}
	r := &runner{c: c}
	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	dir := opt.Prefix + "/" + runID
	body := make([]byte, opt.Payload)
	for i := range body {
		body[i] = byte(i % 251)
	}

	// A few unrecorded round trips first, so connection setup and TLS
	// handshakes do not land in the curve as fake tail latency.
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("%s/warmup-%d", dir, i)
		if _, err := r.timed("warmup put", func() (int, string, error) { return c.put(key, body) }); err != nil {
			return Summary{}, err
		}
		if _, err := r.timed("warmup get", func() (int, string, error) { return c.get(key) }); err != nil {
			return Summary{}, err
		}
		if _, err := r.timed("warmup delete", func() (int, string, error) { return c.delete(key) }); err != nil {
			return Summary{}, err
		}
	}
	r.attempts, r.slowdowns = 0, 0

	keys := make([]string, opt.Samples)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s/%06d", dir, i)
	}

	puts := make([]float64, 0, opt.Samples)
	for _, key := range keys {
		ms, err := r.timed("put", func() (int, string, error) { return c.put(key, body) })
		if err != nil {
			return Summary{}, err
		}
		puts = append(puts, ms)
	}
	putDist := dist(puts)
	progress(fmt.Sprintf("put: %d samples, p50 %.1f ms, p99 %.1f ms", len(puts), putDist.P50, putDist.P99))

	gets := make([]float64, 0, opt.Samples)
	for _, key := range keys {
		ms, err := r.timed("get", func() (int, string, error) { return c.get(key) })
		if err != nil {
			return Summary{}, err
		}
		gets = append(gets, ms)
	}
	getDist := dist(gets)
	progress(fmt.Sprintf("get: %d samples, p50 %.1f ms, p99 %.1f ms", len(gets), getDist.P50, getDist.P99))

	listN := opt.Samples / 4
	if listN < 10 {
		listN = 10
	}
	lists := make([]float64, 0, listN)
	for i := 0; i < listN; i++ {
		ms, err := r.timed("list", func() (int, string, error) { return c.list(dir+"/", 100) })
		if err != nil {
			return Summary{}, err
		}
		lists = append(lists, ms)
	}
	listDist := dist(lists)
	progress(fmt.Sprintf("list: %d samples, p50 %.1f ms, p99 %.1f ms", len(lists), listDist.P50, listDist.P99))

	// Throughput from a handful of large bodies. Rate is bytes over
	// elapsed time minus the small op median, which strips the per
	// request floor and leaves the per byte term zou's transfer model
	// charges. If a transfer somehow beats the floor, the raw rate is
	// used rather than inventing a negative time.
	var rates []float64
	largeBody := make([]byte, opt.Large)
	for i := range largeBody {
		largeBody[i] = byte(i % 249)
	}
	for i := 0; i < opt.LargeN; i++ {
		key := fmt.Sprintf("%s/large-%d", dir, i)
		putMS, err := r.timed("large put", func() (int, string, error) { return c.put(key, largeBody) })
		if err != nil {
			return Summary{}, err
		}
		rates = append(rates, rate(opt.Large, putMS, putDist.P50))
		getMS, err := r.timed("large get", func() (int, string, error) { return c.get(key) })
		if err != nil {
			return Summary{}, err
		}
		rates = append(rates, rate(opt.Large, getMS, getDist.P50))
		if _, err := r.timed("large delete", func() (int, string, error) { return c.delete(key) }); err != nil {
			return Summary{}, err
		}
	}
	mbps := median(rates)
	if mbps < 0.1 {
		mbps = 0.1
	}
	progress(fmt.Sprintf("throughput: %.1f MB/s over %d large round trips", mbps, opt.LargeN*2))

	deletes := make([]float64, 0, opt.Samples)
	for _, key := range keys {
		ms, err := r.timed("delete", func() (int, string, error) { return c.delete(key) })
		if err != nil {
			return Summary{}, err
		}
		deletes = append(deletes, ms)
	}
	deleteDist := dist(deletes)
	progress(fmt.Sprintf("delete: %d samples, p50 %.1f ms, p99 %.1f ms", len(deletes), deleteDist.P50, deleteDist.P99))

	slowdown := 0.0
	if r.attempts > 0 {
		slowdown = math.Round(float64(r.slowdowns)/float64(r.attempts)*1e6) / 1e6
	}
	return Summary{
		Profile: Profile{
			Name:     opt.Name,
			Get:      getDist,
			Put:      putDist,
			List:     listDist,
			Delete:   deleteDist,
			Mbps:     math.Round(mbps*10) / 10,
			Slowdown: slowdown,
		},
		Attempts:  r.attempts,
		Slowdowns: r.slowdowns,
	}, nil
}

// Write saves the profile as indented json, the format
// SimConfig::parse loads when the spec head is a path.
func (p Profile) Write(path string) error {
	out, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(out, '\n'), 0o644)
}

// runner counts every HTTP round trip and every throttle response, so
// the profile's slowdown rate is measured the same way everything else
// is.
type runner struct {
	c         *Client
	attempts  int
	slowdowns int
}

// timed runs one op and returns the milliseconds the successful
// attempt took. Throttle statuses are retried on the same backoff
// schedule zou's real client uses, and their elapsed time is excluded
// from the curve because the sim charges backoff separately through
// the slowdown rate.
func (r *runner) timed(op string, f func() (int, string, error)) (float64, error) {
	for attempt := 1; ; attempt++ {
		t0 := time.Now()
		status, snippet, err := f()
		elapsed := float64(time.Since(t0).Microseconds()) / 1000
		r.attempts++
		if err != nil {
			return 0, fmt.Errorf("%s: %w", op, err)
		}
		if status < 300 {
			return elapsed, nil
		}
		if retryable(status) {
			r.slowdowns++
			if attempt < 4 {
				time.Sleep(time.Duration(100<<(attempt-1)) * time.Millisecond)
				continue
			}
		}
		return 0, fmt.Errorf("%s: http %d: %s", op, status, snippet)
	}
}

// retryable matches the status set zou's s3 client retries.
func retryable(status int) bool {
	switch status {
	case 429, 500, 502, 503, 504:
		return true
	}
	return false
}

func dist(samples []float64) Dist {
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)
	return Dist{
		P50: round2(pctl(sorted, 0.50)),
		P95: round2(pctl(sorted, 0.95)),
		P99: round2(pctl(sorted, 0.99)),
		Max: round2(pctl(sorted, 1.0)),
	}
}

// pctl is nearest rank over a sorted slice.
func pctl(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(q*float64(len(sorted)))) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

func median(v []float64) float64 {
	sorted := append([]float64(nil), v...)
	sort.Float64s(sorted)
	return pctl(sorted, 0.5)
}

// rate turns one large round trip into MB/s after subtracting the
// small op floor. baseMS and the elapsed time are both one way, so the
// difference is the time the bytes themselves took.
func rate(bytes int, elapsedMS, baseMS float64) float64 {
	seconds := (elapsedMS - baseMS) / 1000
	if seconds <= 0 {
		seconds = elapsedMS / 1000
	}
	if seconds <= 0 {
		return 0
	}
	return float64(bytes) / 1e6 / seconds
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }
