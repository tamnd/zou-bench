package fleet

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tamnd/zou-bench/resthttp"
)

func TestURLPutsTheRefFirst(t *testing.T) {
	got := URL("http://127.0.0.1:54321/", "acme-prod", "/rest/v1", "/bench_rows?id=eq.1")
	want := "http://127.0.0.1:54321/acme-prod/rest/v1/bench_rows?id=eq.1"
	if got != want {
		t.Fatalf("url = %q, want %q", got, want)
	}
}

func TestRefsAreFixedWidthAndSorted(t *testing.T) {
	refs := Refs("t", 3)
	if len(refs) != 3 || refs[0] != "t0001" || refs[2] != "t0003" {
		t.Fatalf("refs = %v", refs)
	}
}

// The whole point of this driver is that every request names a
// different project, so the test that matters is that a phase spreads
// over its tenant set instead of hammering one.
func TestDriveSpreadsOverEveryTenant(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ref := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")[0]
		mu.Lock()
		seen[ref]++
		mu.Unlock()
		if r.Header.Get("apikey") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	out, err := Drive(context.Background(), Options{
		BaseURL:  srv.URL,
		Refs:     Refs("t", 8),
		Secret:   "a secret at least thirty two characters long",
		Clients:  4,
		Duration: 400 * time.Millisecond,
		Requests: []resthttp.Request{{Name: "read", Path: "/bench_rows?select=id"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Requests == 0 || out.Errors != 0 {
		t.Fatalf("requests = %d, errors = %d, failures %v", out.Requests, out.Errors, out.Failures)
	}
	if out.Touched != 8 {
		t.Fatalf("touched %d of 8 tenants: %v", out.Touched, seen)
	}
	if len(out.PerRequest["read"]) == 0 {
		t.Fatal("no per request distribution")
	}
}

func TestDriveCountsAnUnexpectedStatusAsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	out, err := Drive(context.Background(), Options{
		BaseURL:  srv.URL,
		Refs:     []string{"t0001"},
		Secret:   "a secret at least thirty two characters long",
		Clients:  1,
		Duration: 200 * time.Millisecond,
		Requests: []resthttp.Request{{Name: "read", Path: "/bench_rows"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Errors != out.Requests || out.Requests == 0 {
		t.Fatalf("errors %d of %d requests", out.Errors, out.Requests)
	}
	if len(out.Samples) != 0 {
		t.Fatal("a 404 was timed into the distribution")
	}
}

func TestScrapeReadsCountersAndBuckets(t *testing.T) {
	body := `# HELP zou_tenants_attached tenants attached right now
# TYPE zou_tenants_attached gauge
zou_tenants_attached 42
zou_tenant_attaches_total{outcome="ok"} 100
zou_tenant_attach_seconds_bucket{le="0.1"} 10
zou_tenant_attach_seconds_bucket{le="1"} 80
zou_tenant_attach_seconds_bucket{le="10"} 100
zou_tenant_attach_seconds_bucket{le="+Inf"} 100
zou_tenant_attach_seconds_sum 55.5
zou_tenant_attach_seconds_count 100
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(body))
	}))
	defer srv.Close()
	m, err := Scrape(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if m.Get("zou_tenants_attached") != 42 {
		t.Fatalf("attached = %v", m.Get("zou_tenants_attached"))
	}
	if m.Get(`zou_tenant_attaches_total{outcome="ok"}`) != 100 {
		t.Fatalf("attaches = %v", m)
	}
	mean, ok := AttachSeconds(m)
	if !ok || mean < 0.554 || mean > 0.556 {
		t.Fatalf("mean = %v %v", mean, ok)
	}
	// The +Inf bucket is left out on purpose: _count already carries
	// its number and an infinity is not something json can write.
	buckets := Buckets(m, "zou_tenant_attach_seconds")
	if len(buckets) != 3 || buckets[0].LE != 0.1 || buckets[2].LE != 10 {
		t.Fatalf("buckets = %v", buckets)
	}
	if raw, err := json.Marshal(buckets); err != nil {
		t.Fatalf("buckets are not writable: %v", err)
	} else if strings.Contains(string(raw), "Inf") {
		t.Fatalf("an infinity reached the result file: %s", raw)
	}
	p50, _ := Quantile(buckets, 0.50)
	if p50 != 1 {
		t.Fatalf("p50 = %v, the bucket the median falls in has bound 1", p50)
	}
	p99, _ := Quantile(buckets, 0.99)
	if p99 != 10 {
		t.Fatalf("p99 = %v", p99)
	}
}

// An interval with no attaches has no mean attach time, and reporting
// zero would read as an instant attach.
func TestAttachSecondsSaysNothingWhenNothingAttached(t *testing.T) {
	if _, ok := AttachSeconds(Metrics{}); ok {
		t.Fatal("a mean was reported for an interval with no attaches")
	}
}

func TestDeltaIsAfterMinusBefore(t *testing.T) {
	d := Delta(Metrics{"a": 5, "b": 1}, Metrics{"a": 9, "c": 2})
	if d["a"] != 4 || d["c"] != 2 {
		t.Fatalf("delta = %v", d)
	}
}

func TestStateRemembersWhatIsAlreadyProvisioned(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fleet-state.json")
	s, err := LoadState(path, "/tmp/store", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if s.Has("t0001") {
		t.Fatal("a fresh state claimed a tenant")
	}
	if err := s.Add("t0002"); err != nil {
		t.Fatal(err)
	}
	if err := s.Add("t0001"); err != nil {
		t.Fatal(err)
	}
	again, err := LoadState(path, "/tmp/store", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if !again.Has("t0001") || !again.Has("t0002") || again.Count() != 2 {
		t.Fatalf("reloaded = %+v", again.Ready)
	}
	if again.Ready[0] != "t0001" {
		t.Fatalf("refs are not in order: %v", again.Ready)
	}
}

// A state file that describes another store describes tenants that are
// not there, and provisioning would skip a thousand refs that do not
// exist.
func TestStateRefusesADifferentStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fleet-state.json")
	s, err := LoadState(path, "/tmp/store", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Add("t0001"); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadState(path, "/tmp/other", "secret"); err == nil {
		t.Fatal("a state file from another store was accepted")
	}
	if _, err := LoadState(path, "/tmp/store", "another secret"); err == nil {
		t.Fatal("a state file with another secret was accepted")
	}
}
