package resthttp

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestARunSendsWhatTheWorkloadSaysAndTimesIt(t *testing.T) {
	var seen atomic.Int64
	var sawKey, sawBearer atomic.Value
	sawKey.Store("")
	sawBearer.Store("")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Add(1)
		sawKey.Store(r.Header.Get("apikey"))
		sawBearer.Store(r.Header.Get("authorization"))
		w.Write([]byte(`[{"id":1}]`))
	}))
	defer srv.Close()

	out, err := Run(context.Background(), Options{
		BaseURL:  srv.URL,
		Clients:  2,
		Duration: 200 * time.Millisecond,
		APIKey:   "anon-key",
		Tokens:   map[string]string{"user": "user-token"},
		Requests: []Request{{Name: "read", Path: "/rest/v1/t", Auth: "user"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Requests == 0 || len(out.Samples) != out.Requests {
		t.Fatalf("requests %d, samples %d", out.Requests, len(out.Samples))
	}
	if out.Errors != 0 || out.Status[200] != out.Requests {
		t.Fatalf("errors %d, status %v", out.Errors, out.Status)
	}
	if sawKey.Load() != "anon-key" || sawBearer.Load() != "Bearer user-token" {
		t.Fatalf("headers: key %q bearer %q", sawKey.Load(), sawBearer.Load())
	}
	if out.PerRequest["read"]["requests"] != float64(out.Requests) {
		t.Fatalf("per request = %v", out.PerRequest)
	}
	if out.Bytes == 0 {
		t.Fatal("the body was never read, so neither was the answer")
	}
}

func TestAnAnswerNobodyAskedForIsAnErrorAndNotALatency(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no", http.StatusUnauthorized)
	}))
	defer srv.Close()

	out, err := Run(context.Background(), Options{
		BaseURL:  srv.URL,
		Clients:  1,
		Duration: 150 * time.Millisecond,
		Requests: []Request{{Name: "read", Path: "/rest/v1/t"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// The whole point: a wall of 401s is fast, and a benchmark that
	// timed them would report the fastest numbers it ever produced.
	if out.Errors != out.Requests || len(out.Samples) != 0 {
		t.Fatalf("errors %d of %d, samples %d", out.Errors, out.Requests, len(out.Samples))
	}
	if len(out.Failures) == 0 || !strings.Contains(out.Failures[0], "401") {
		t.Fatalf("failures = %v", out.Failures)
	}
}

func TestTheWarmupIsNotMeasured(t *testing.T) {
	var seen atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Add(1)
		w.Write([]byte("[]"))
	}))
	defer srv.Close()

	out, err := Run(context.Background(), Options{
		BaseURL:  srv.URL,
		Clients:  1,
		Warmup:   200 * time.Millisecond,
		Duration: 200 * time.Millisecond,
		Requests: []Request{{Name: "read", Path: "/rest/v1/t"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if int64(out.Requests) >= seen.Load() {
		t.Fatalf("the warmup landed in the result: counted %d of %d served", out.Requests, seen.Load())
	}
}

func TestTheMixIsExactAndThePlaceholdersMove(t *testing.T) {
	paths := make(chan string, 4096)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case paths <- r.URL.RequestURI():
		default:
		}
		w.Write([]byte("[]"))
	}))
	defer srv.Close()

	out, err := Run(context.Background(), Options{
		BaseURL:  srv.URL,
		Clients:  2,
		Duration: 300 * time.Millisecond,
		Requests: []Request{
			{Name: "hot", Path: "/rest/v1/t?id=eq.{{rand:1:1000}}", Weight: 9},
			{Name: "cold", Path: "/rest/v1/u", Weight: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	close(paths)
	ids := map[string]bool{}
	for p := range paths {
		if strings.HasPrefix(p, "/rest/v1/t") {
			ids[p] = true
		}
	}
	if len(ids) < 5 {
		t.Fatalf("the random id never moved: %d distinct paths", len(ids))
	}
	hot := out.PerRequest["hot"]["requests"]
	cold := out.PerRequest["cold"]["requests"]
	if cold == 0 || hot < cold*3 {
		t.Fatalf("the mix is not 9 to 1: hot %v cold %v", hot, cold)
	}
}

func TestARateCapHoldsTheLoadWhereItWasPut(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("[]"))
	}))
	defer srv.Close()

	out, err := Run(context.Background(), Options{
		BaseURL:  srv.URL,
		Clients:  4,
		Duration: time.Second,
		Rate:     100,
		Requests: []Request{{Name: "read", Path: "/rest/v1/t"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Loose bounds on purpose: a ticker on a busy CI box is not a
	// clock, and what is under test is that the cap is a cap.
	if out.Requests > 200 || out.Requests < 20 {
		t.Fatalf("100 rps for a second sent %d requests", out.Requests)
	}
}

func TestAWorkloadWithNoRequestsIsAnError(t *testing.T) {
	if _, err := Run(context.Background(), Options{BaseURL: "http://127.0.0.1:1"}); err == nil {
		t.Fatal("expected an error")
	}
}

func TestAMintedTokenIsSignedTheWayTheServerChecks(t *testing.T) {
	tok := UserToken("secret", "11111111-1111-1111-1111-111111111111", "a@b.c")
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		t.Fatalf("token = %q", tok)
	}
	mac := hmac.New(sha256.New, []byte("secret"))
	mac.Write([]byte(parts[0] + "." + parts[1]))
	want := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if parts[2] != want {
		t.Fatalf("signature %q, want %q", parts[2], want)
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var claims map[string]any
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatal(err)
	}
	if claims["role"] != "authenticated" || claims["sub"] != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("claims = %v", claims)
	}
	if claims["exp"].(float64) <= float64(time.Now().Unix()) {
		t.Fatalf("the token is already expired: %v", claims["exp"])
	}
	if KeyToken("secret", "anon") == KeyToken("secret", "service_role") {
		t.Fatal("the two keys are the same token")
	}
}
