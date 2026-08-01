package probe

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// The signed GetObject example from the AWS SigV4 documentation, the
// one test vector Amazon publishes with a full expected signature. If
// this passes, the signer speaks the same protocol every provider
// verifies.
func TestTheSignatureMatchesTheAwsPublishedExample(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Range", "bytes=0-9")
	when := time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)
	emptyHash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	sign(req, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "us-east-1", emptyHash, when)
	want := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request," +
		"SignedHeaders=host;range;x-amz-content-sha256;x-amz-date," +
		"Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
	if got := req.Header.Get("Authorization"); got != want {
		t.Fatalf("authorization mismatch\ngot  %s\nwant %s", got, want)
	}
}

// fakeS3 is a provider stand in with a known latency floor per op and
// a deliberate throttle cadence, so the probe's output is checkable.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	puts    int
	lists   int
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 Credential=probe/") ||
		!strings.Contains(auth, "SignedHeaders=host;x-amz-content-sha256;x-amz-date,") {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := strings.TrimPrefix(r.URL.Path, "/bucket/")
	switch {
	case r.Method == http.MethodPut:
		f.puts++
		// Every 9th put gets throttled, which should surface as a
		// nonzero slowdown rate without failing the probe.
		if f.puts%9 == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		body := make([]byte, 0, 64)
		buf := make([]byte, 32<<10)
		for {
			n, err := r.Body.Read(buf)
			body = append(body, buf[:n]...)
			if err != nil {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		f.objects[key] = body
	case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
		f.lists++
		if !strings.HasPrefix(r.URL.Query().Get("prefix"), "probe-test/") {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		time.Sleep(time.Millisecond)
		w.Write([]byte("<ListBucketResult></ListBucketResult>"))
	case r.Method == http.MethodGet:
		body, ok := f.objects[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		time.Sleep(time.Millisecond)
		w.Write(body)
	case r.Method == http.MethodDelete:
		delete(f.objects, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func TestTheProbeMeasuresAProviderAndWritesALoadableCalibrationFile(t *testing.T) {
	fake := &fakeS3{objects: map[string][]byte{}}
	srv := httptest.NewServer(fake)
	defer srv.Close()

	c := &Client{
		Endpoint:  srv.URL,
		Region:    "us-east-1",
		Bucket:    "bucket",
		AccessKey: "probe",
		SecretKey: "secret",
	}
	sum, err := Run(c, Options{
		Samples: 40,
		Payload: 64,
		Large:   1 << 20,
		LargeN:  2,
		Prefix:  "probe-test",
		Name:    "fake-provider",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	p := sum.Profile

	if p.Name != "fake-provider" {
		t.Fatalf("name = %q", p.Name)
	}
	// The fake sleeps 2 ms per put and 1 ms per get, so the medians
	// must sit at or above those floors and stay ordered.
	if p.Put.P50 < 2 {
		t.Fatalf("put p50 %.2f ms is below the server's 2 ms floor", p.Put.P50)
	}
	if p.Get.P50 < 1 {
		t.Fatalf("get p50 %.2f ms is below the server's 1 ms floor", p.Get.P50)
	}
	for _, d := range []Dist{p.Get, p.Put, p.List, p.Delete} {
		if !(d.P50 <= d.P95 && d.P95 <= d.P99 && d.P99 <= d.Max) {
			t.Fatalf("non monotone dist %+v", d)
		}
	}
	if p.Slowdown <= 0 {
		t.Fatalf("the fake throttled every 9th put but slowdown = %v", p.Slowdown)
	}
	if sum.Slowdowns == 0 || sum.Attempts <= sum.Slowdowns {
		t.Fatalf("attempts %d slowdowns %d", sum.Attempts, sum.Slowdowns)
	}
	if p.Mbps <= 0 {
		t.Fatalf("mbps = %v", p.Mbps)
	}
	if fake.lists == 0 {
		t.Fatal("no list requests reached the server")
	}
	if len(fake.objects) != 0 {
		t.Fatalf("probe left %d keys behind", len(fake.objects))
	}

	path := filepath.Join(t.TempDir(), "fake-provider.calibration.json")
	if err := p.Write(path); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var back Profile
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back != p {
		t.Fatalf("round trip mismatch\nwrote %+v\nread  %+v", p, back)
	}
	// The file must expose the exact field names zou's SimProfile
	// deserializes, since that struct is the real consumer.
	var fields map[string]any
	json.Unmarshal(raw, &fields)
	for _, want := range []string{"name", "get", "put", "list", "delete", "mbps", "slowdown"} {
		if _, ok := fields[want]; !ok {
			t.Fatalf("calibration file lacks %q", want)
		}
	}
	if _, ok := fields["get"].(map[string]any)["p50_ms"]; !ok {
		t.Fatal("dist fields are not the p50_ms family zou expects")
	}
}

func TestCanonicalQuerySortsAndEncodesLikeTheSpec(t *testing.T) {
	q := url.Values{}
	q.Set("prefix", "a b/c")
	q.Set("list-type", "2")
	q.Add("k", "z")
	q.Add("k", "a")
	got := canonicalQuery(q)
	want := "k=a&k=z&list-type=2&prefix=a%20b%2Fc"
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}
