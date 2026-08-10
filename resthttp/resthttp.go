// Package resthttp drives a PostgREST compatible API the way an app
// does, over keep alive connections with a bearer token, and times
// every request.
//
// The point of measuring here rather than at the database is that the
// target is a user facing one: a warm read is what an app waits for,
// and between it and the row there is a json parse of a token, a
// grammar parse, a plan, a session with seven settings in it, and the
// json encoding of the answer. pgbench cannot see any of that.
package resthttp

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/tamnd/zou-bench/latency"
)

// Request is one shape the driver sends.
type Request struct {
	Name   string            `json:"name"`
	Method string            `json:"method"`
	Path   string            `json:"path"`
	Body   string            `json:"body"`
	Header map[string]string `json:"header"`
	// Auth names which token goes in the Authorization header: anon,
	// user, or service. Empty means anon, which is what an unauthenticated
	// supabase-js call sends.
	Auth string `json:"auth"`
	// Weight is how often this shape is picked relative to the others,
	// default 1.
	Weight int `json:"weight"`
	// Expect is the status the answer has to carry. Anything else is
	// counted as an error and named in the result, because a benchmark
	// measuring 404s is measuring nothing.
	Expect int `json:"expect"`
}

// Options is one run.
type Options struct {
	BaseURL  string
	Clients  int
	Duration time.Duration
	Warmup   time.Duration
	// Rate caps the whole run in requests per second, 0 is unlimited.
	// A capped run measures latency at a load somebody chose, which is
	// the only way a p99 compares across systems.
	Rate     int
	Requests []Request
	Tokens   map[string]string
	APIKey   string
}

// Outcome is what one run produced.
type Outcome struct {
	Samples  []latency.Sample
	Requests int
	Errors   int
	// Status counts every status seen, so a run that answered 401 to
	// everything cannot report a fast p50 and look healthy.
	Status map[int]int
	// PerRequest is the distribution per named shape. A mixed run's
	// overall p99 belongs to whichever shape is slowest, and the point
	// of naming them is to see which.
	PerRequest map[string]map[string]float64
	Failures   []string
	Bytes      int64
	Elapsed    time.Duration
}

// Run sends the workload and returns the timings. Every client keeps
// its own connection, which is what warm means: connection setup and
// TLS are not part of what NFR-10 is about, and an app's pool is warm
// by the second request.
func Run(ctx context.Context, o Options) (Outcome, error) {
	if len(o.Requests) == 0 {
		return Outcome{}, fmt.Errorf("no requests in the workload")
	}
	if o.Clients <= 0 {
		o.Clients = 8
	}
	picks := weighted(o.Requests)

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
			MaxIdleConns:        o.Clients * 2,
			MaxIdleConnsPerHost: o.Clients * 2,
			MaxConnsPerHost:     o.Clients * 2,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		},
	}
	defer client.CloseIdleConnections()

	if o.Warmup > 0 {
		warm := o
		warm.Warmup, warm.Duration = 0, o.Warmup
		if _, err := drive(ctx, client, warm, picks, false); err != nil {
			return Outcome{}, err
		}
	}
	return drive(ctx, client, o, picks, true)
}

// drive is the loop itself. keep false runs the same traffic and
// throws the timings away, which is what a warmup is.
func drive(ctx context.Context, client *http.Client, o Options, picks []Request, keep bool) (Outcome, error) {
	ctx, cancel := context.WithTimeout(ctx, o.Duration)
	defer cancel()

	type local struct {
		samples  []latency.Sample
		status   map[int]int
		perReq   map[string][]float64
		failures []string
		bytes    int64
		requests int
		errors   int
	}
	locals := make([]local, o.Clients)

	// A rate cap is shared by every client, so one slow client cannot
	// hand its share of the load to the others and quietly change the
	// workload.
	var ticks <-chan time.Time
	if o.Rate > 0 {
		t := time.NewTicker(time.Second / time.Duration(o.Rate))
		defer t.Stop()
		ticks = t.C
	}

	start := time.Now()
	var wg sync.WaitGroup
	for i := range o.Clients {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			l := &locals[id]
			l.status = map[int]int{}
			l.perReq = map[string][]float64{}
			// Every client draws from its own stream, seeded from its
			// id, so a run is reproducible in shape without every
			// client asking for the same row.
			rng := rand.New(rand.NewPCG(uint64(id)+1, 0x5eed))
			for {
				if ticks != nil {
					select {
					case <-ctx.Done():
						return
					case <-ticks:
					}
				} else if ctx.Err() != nil {
					return
				}
				req := picks[rng.IntN(len(picks))]
				ms, status, n, err := once(ctx, client, o, req, rng)
				if ctx.Err() != nil {
					return
				}
				l.requests++
				l.bytes += n
				want := req.Expect
				if want == 0 {
					want = 200
				}
				if err != nil {
					l.errors++
					if len(l.failures) < 5 {
						l.failures = append(l.failures, req.Name+": "+err.Error())
					}
					continue
				}
				l.status[status]++
				if status != want {
					l.errors++
					if len(l.failures) < 5 {
						l.failures = append(l.failures,
							fmt.Sprintf("%s: wanted %d, got %d", req.Name, want, status))
					}
					continue
				}
				if keep {
					l.samples = append(l.samples, latency.Sample{MS: ms, Epoch: time.Now().Unix()})
					l.perReq[req.Name] = append(l.perReq[req.Name], ms)
				}
			}
		}(i)
	}
	wg.Wait()

	out := Outcome{
		Status:     map[int]int{},
		PerRequest: map[string]map[string]float64{},
		Elapsed:    time.Since(start),
	}
	perReq := map[string][]float64{}
	for _, l := range locals {
		out.Samples = append(out.Samples, l.samples...)
		out.Requests += l.requests
		out.Errors += l.errors
		out.Bytes += l.bytes
		out.Failures = append(out.Failures, l.failures...)
		for s, n := range l.status {
			out.Status[s] += n
		}
		for name, vals := range l.perReq {
			perReq[name] = append(perReq[name], vals...)
		}
	}
	for name, vals := range perReq {
		samples := make([]latency.Sample, len(vals))
		for i, v := range vals {
			samples[i] = latency.Sample{MS: v}
		}
		p := latency.Percentiles(samples)
		p["requests"] = float64(len(vals))
		out.PerRequest[name] = p
	}
	if len(out.Failures) > 5 {
		out.Failures = out.Failures[:5]
	}
	return out, nil
}

// once sends one request and returns its wall time in milliseconds.
// The body is read to the end and thrown away, because a client that
// leaves it unread is not measuring the answer and does not get its
// connection back either.
func once(ctx context.Context, client *http.Client, o Options, r Request, rng *rand.Rand) (float64, int, int64, error) {
	method := r.Method
	if method == "" {
		method = http.MethodGet
	}
	path := Expand(r.Path, rng)
	var body io.Reader
	if r.Body != "" {
		body = strings.NewReader(Expand(r.Body, rng))
	}
	req, err := http.NewRequestWithContext(ctx, method, o.BaseURL+path, body)
	if err != nil {
		return 0, 0, 0, err
	}
	if o.APIKey != "" {
		req.Header.Set("apikey", o.APIKey)
	}
	auth := r.Auth
	if auth == "" {
		auth = "anon"
	}
	if tok := o.Tokens[auth]; tok != "" {
		req.Header.Set("authorization", "Bearer "+tok)
	}
	if r.Body != "" {
		req.Header.Set("content-type", "application/json")
	}
	for k, v := range r.Header {
		req.Header.Set(k, v)
	}
	t0 := time.Now()
	res, err := client.Do(req)
	if err != nil {
		return 0, 0, 0, err
	}
	n, _ := io.Copy(io.Discard, res.Body)
	res.Body.Close()
	return float64(time.Since(t0).Nanoseconds()) / 1e6, res.StatusCode, n, nil
}

// Expand fills the two placeholders a workload needs: a random integer
// in a range, for asking about a different row every time, and a random
// hex string, for writes that need to be unique.
func Expand(s string, rng *rand.Rand) string {
	for {
		i := strings.Index(s, "{{rand:")
		if i < 0 {
			break
		}
		j := strings.Index(s[i:], "}}")
		if j < 0 {
			break
		}
		spec := s[i+len("{{rand:") : i+j]
		lo, hi := 1, 1
		fmt.Sscanf(spec, "%d:%d", &lo, &hi)
		if hi < lo {
			hi = lo
		}
		s = s[:i] + fmt.Sprint(lo+rng.IntN(hi-lo+1)) + s[i+j+2:]
	}
	for strings.Contains(s, "{{hex}}") {
		s = strings.Replace(s, "{{hex}}", fmt.Sprintf("%08x", rng.Uint32()), 1)
	}
	return s
}

// weighted flattens the workload into a slice a uniform draw picks
// from, which keeps the mix exact rather than approximately right.
func weighted(reqs []Request) []Request {
	var out []Request
	for _, r := range reqs {
		w := r.Weight
		if w <= 0 {
			w = 1
		}
		for range w {
			out = append(out, r)
		}
	}
	return out
}
