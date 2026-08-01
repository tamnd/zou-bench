// Package scenario loads benchmark scenario files. A scenario is a
// small json document naming a workload shape, the harness never tunes
// the server, so everything here is client side.
package scenario

import (
	"encoding/json"
	"fmt"
	"os"
)

type Scenario struct {
	Name    string `json:"name"`
	Init    bool   `json:"init"`
	Scale   int    `json:"scale"`
	Clients int    `json:"clients"`
	Threads int    `json:"threads"`
	// Duration is the measured window in seconds. Warmup runs the same
	// workload first and is discarded, so cold cache effects land in
	// the warmup unless a scenario wants them (warmup 0).
	Duration int `json:"duration"`
	Warmup   int `json:"warmup"`
	// Builtin is a pgbench builtin name (tpcb-like, select-only,
	// simple-update). Script points at a custom pgbench script file
	// instead, relative to the scenario file.
	Builtin string `json:"builtin"`
	Script  string `json:"script"`
	// Rate caps throughput in transactions per second, 0 is unlimited.
	// Rate limited runs measure latency at a fixed load, which is the
	// honest way to compare tail latency across systems.
	Rate int `json:"rate"`
}

// Load reads a scenario and returns it along with the raw document,
// which goes into the result file verbatim so a result always carries
// the exact scenario that produced it.
func Load(path string) (Scenario, map[string]any, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Scenario{}, nil, err
	}
	var sc Scenario
	if err := json.Unmarshal(raw, &sc); err != nil {
		return Scenario{}, nil, fmt.Errorf("%s: %w", path, err)
	}
	if sc.Name == "" {
		return Scenario{}, nil, fmt.Errorf("%s: scenario has no name", path)
	}
	if sc.Clients == 0 {
		sc.Clients = 8
	}
	if sc.Threads == 0 {
		sc.Threads = sc.Clients
	}
	if sc.Duration == 0 {
		sc.Duration = 60
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return Scenario{}, nil, err
	}
	return sc, doc, nil
}
