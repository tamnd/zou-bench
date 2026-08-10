package main

import (
	"encoding/json"
	"math"
	"testing"
)

// A rate over a window of no time is an infinity, and losing an hour of
// numbers to it is the failure this guards against.
func TestJsonableKeepsTheRestOfTheReport(t *testing.T) {
	var bad []string
	in := map[string]any{
		"steady": map[string]any{
			"rps":        math.Inf(1),
			"latency_ms": map[string]float64{"p50": 1.5},
		},
		"tenants": 1000,
	}
	out := jsonable(in, "", &bad)
	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("still not writable: %v", err)
	}
	if len(bad) != 1 || bad[0] != "steady/rps" {
		t.Fatalf("bad = %v", bad)
	}
	var back map[string]any
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	steady := back["steady"].(map[string]any)
	if steady["rps"] != nil {
		t.Fatalf("rps = %v, want null", steady["rps"])
	}
	if steady["latency_ms"].(map[string]any)["p50"] != 1.5 {
		t.Fatalf("the good numbers did not survive: %v", steady)
	}
}
