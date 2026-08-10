// Package fleet drives a multi tenant zou node the way a fleet is
// actually used: a lot of small projects, most of them asleep, a few
// busy at any one moment, and the busy ones changing.
//
// It is a separate driver from resthttp because the thing under test is
// different. resthttp asks one project how fast it answers. This asks a
// node how many projects it can hold and what happens to the answers
// when the set of projects being asked for is bigger than the set it is
// allowed to keep up. Every request names its own tenant, so the url
// and the token change per request and the interesting number is not
// the mean, it is what the tail does while tenants are being started
// and stopped underneath the traffic.
package fleet

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
	"github.com/tamnd/zou-bench/resthttp"
)

// Options is one measured phase.
type Options struct {
	// BaseURL is the node, with no tenant and no api prefix in it, for
	// example http://127.0.0.1:54321. The driver routes by path
	// prefix, which is the way a node is reached without a wildcard
	// certificate and the only way one is reached without DNS.
	BaseURL string
	// Refs is the set of tenants this phase draws from. A phase that
	// draws from more tenants than the node's ceiling is the churn
	// phase, one that draws from fewer is the steady phase, and the
	// difference between their tails is the whole measurement.
	Refs []string
	// Secret is the jwt secret every tenant in this run was created
	// with. One secret across the fleet keeps token minting off the
	// request path, and since HS256 verification costs the same
	// whatever the key is, it changes nothing the run measures.
	Secret   string
	Clients  int
	Duration time.Duration
	Warmup   time.Duration
	// Rate caps the whole phase in requests per second, 0 is
	// unlimited. A capped phase is how a p99 is compared between two
	// configurations of the same node.
	Rate     int
	Requests []resthttp.Request
	// APIPrefix is what goes between the ref and the request path,
	// /rest/v1 unless a scenario says otherwise.
	APIPrefix string
}

// Outcome is what one phase produced.
type Outcome struct {
	Samples    []latency.Sample
	Requests   int
	Errors     int
	Status     map[int]int
	PerRequest map[string]map[string]float64
	Failures   []string
	Bytes      int64
	Elapsed    time.Duration
	// Touched is how many distinct tenants this phase asked for. A
	// phase meant to churn that touched forty of a thousand tenants
	// churned nothing, and the count is the only way to see that.
	Touched int
}

// URL builds the address one request goes to. The ref is the first path
// segment, which is what makes a node reachable with no DNS at all.
func URL(base, ref, prefix, path string) string {
	return strings.TrimSuffix(base, "/") + "/" + ref + prefix + path
}

// Drive sends the workload and returns the timings.
func Drive(ctx context.Context, o Options) (Outcome, error) {
	if len(o.Requests) == 0 {
		return Outcome{}, fmt.Errorf("no requests in the workload")
	}
	if len(o.Refs) == 0 {
		return Outcome{}, fmt.Errorf("no tenants to send to")
	}
	if o.Clients <= 0 {
		o.Clients = 8
	}
	if o.APIPrefix == "" {
		o.APIPrefix = "/rest/v1"
	}
	picks := weighted(o.Requests)
	tokens := map[string]string{
		"anon":    resthttp.KeyToken(o.Secret, "anon"),
		"service": resthttp.KeyToken(o.Secret, "service_role"),
		"user":    resthttp.UserToken(o.Secret, "00000000-0000-0000-0000-0000000000aa", "bench@zou.test"),
	}

	// One transport for the whole phase. Connections are to this node
	// and not to a tenant, so they are reused across tenants exactly
	// the way a browser or a load balancer would reuse them, and the
	// pool is sized off the client count rather than the tenant count.
	client := &http.Client{
		Timeout: 60 * time.Second,
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
		if _, err := drive(ctx, client, warm, picks, tokens, false); err != nil {
			return Outcome{}, err
		}
	}
	return drive(ctx, client, o, picks, tokens, true)
}

func drive(ctx context.Context, client *http.Client, o Options, picks []resthttp.Request, tokens map[string]string, keep bool) (Outcome, error) {
	ctx, cancel := context.WithTimeout(ctx, o.Duration)
	defer cancel()

	type local struct {
		samples  []latency.Sample
		status   map[int]int
		perReq   map[string][]float64
		touched  map[string]bool
		failures []string
		bytes    int64
		requests int
		errors   int
	}
	locals := make([]local, o.Clients)

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
			l.touched = map[string]bool{}
			rng := rand.New(rand.NewPCG(uint64(id)+1, 0x2112))
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
				// Uniform over the phase's tenant set. Real traffic is
				// skewed, but a skew is a second parameter to argue
				// about, and uniform is the shape that puts the most
				// pressure on the attach path, which is what this
				// measures.
				ref := o.Refs[rng.IntN(len(o.Refs))]
				ms, status, n, said, err := once(ctx, client, o, tokens, ref, req, rng)
				if ctx.Err() != nil {
					return
				}
				l.requests++
				l.bytes += n
				l.touched[ref] = true
				want := req.Expect
				if want == 0 {
					want = 200
				}
				if err != nil {
					l.errors++
					if len(l.failures) < 5 {
						l.failures = append(l.failures, ref+" "+req.Name+": "+err.Error())
					}
					continue
				}
				l.status[status]++
				if status != want {
					l.errors++
					if len(l.failures) < 5 {
						l.failures = append(l.failures,
							fmt.Sprintf("%s %s: wanted %d, got %d: %s", ref, req.Name, want, status, said))
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
	touched := map[string]bool{}
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
		for ref := range l.touched {
			touched[ref] = true
		}
	}
	out.Touched = len(touched)
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

// once sends one request. It returns what the server said as well as
// how long it took, because a status nobody expected is a bug report
// and the body is the half of it that says what went wrong.
func once(ctx context.Context, client *http.Client, o Options, tokens map[string]string, ref string, r resthttp.Request, rng *rand.Rand) (float64, int, int64, string, error) {
	method := r.Method
	if method == "" {
		method = http.MethodGet
	}
	path := resthttp.Expand(r.Path, rng)
	var body io.Reader
	if r.Body != "" {
		body = strings.NewReader(resthttp.Expand(r.Body, rng))
	}
	req, err := http.NewRequestWithContext(ctx, method, URL(o.BaseURL, ref, o.APIPrefix, path), body)
	if err != nil {
		return 0, 0, 0, "", err
	}
	auth := r.Auth
	if auth == "" {
		auth = "anon"
	}
	req.Header.Set("apikey", tokens["anon"])
	if tok := tokens[auth]; tok != "" {
		req.Header.Set("authorization", "Bearer "+tok)
	}
	if r.Body != "" {
		req.Header.Set("content-type", "application/json")
	}
	for k, v := range r.Header {
		req.Header.Set(k, v)
	}
	want := r.Expect
	if want == 0 {
		want = http.StatusOK
	}
	t0 := time.Now()
	res, err := client.Do(req)
	if err != nil {
		return 0, 0, 0, "", err
	}
	var said string
	var n int64
	if res.StatusCode == want {
		n, _ = io.Copy(io.Discard, res.Body)
	} else {
		// Only read on the unexpected path: reading every body into
		// memory would measure the harness and not the server.
		raw, _ := io.ReadAll(io.LimitReader(res.Body, 400))
		n = int64(len(raw))
		said = strings.TrimSpace(string(raw))
	}
	res.Body.Close()
	return float64(time.Since(t0).Nanoseconds()) / 1e6, res.StatusCode, n, said, nil
}

func weighted(reqs []resthttp.Request) []resthttp.Request {
	var out []resthttp.Request
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
