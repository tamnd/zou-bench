// Package scenario loads benchmark scenario files. A scenario is a
// small json document naming a workload shape, the harness never tunes
// the server, so everything here is client side.
package scenario

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/tamnd/zou-bench/resthttp"
)

type Scenario struct {
	Name string `json:"name"`
	// Kind is pgbench when empty, or rest for a scenario the http
	// driver sends. The two share clients, threads, duration, warmup
	// and rate, and nothing else.
	Kind    string `json:"kind"`
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
	// Setup is a sql file applied before a rest run, relative to the
	// scenario file. The http driver cannot create its own data, and
	// the schema it measures against belongs with the workload rather
	// than in whoever's shell started the server.
	Setup string `json:"setup"`
	// Requests is the rest workload. Each entry carries a name, a
	// method, a path, which token to send, and a weight.
	Requests []resthttp.Request `json:"requests"`
	// Cycles, Query, and Port shape an attach scenario: how many
	// stop and start rounds to measure, the query each round times
	// from process spawn to first row, and the port the spawned
	// server listens on.
	Cycles int    `json:"cycles"`
	Query  string `json:"query"`
	Port   int    `json:"port"`
	// Segment, Drills, CheckpointSecs, and CompactSecs shape a
	// sustain scenario: seconds of pgbench load per segment with one
	// kill drill inside each, the rotation the drills fire in, and
	// the cadences of the harness driven CHECKPOINT and compaction
	// sweep. The cadences live in the scenario because nothing else
	// drives folding or compaction on a zou dev node, so they are
	// part of the workload shape, not tuning.
	Segment        int      `json:"segment"`
	Drills         []string `json:"drills"`
	CheckpointSecs int      `json:"checkpoint_secs"`
	CompactSecs    int      `json:"compact_secs"`
	// GCSecs, GCRetention and GCWindow drive the collection sweep, off
	// when the cadence is zero. A store only grows on its own, so a
	// soak long enough to be interesting is also long enough to fill
	// the disk with history nobody asked to keep: a 6 hour run at
	// scale 100 left 49 GB behind for a 1.6 GB database. The two
	// windows are short here on purpose. Retention is the promise
	// point in time recovery has to keep, and a benchmark store makes
	// no such promise past the run it is in.
	GCSecs      int    `json:"gc_secs"`
	GCRetention string `json:"gc_retention"`
	GCWindow    string `json:"gc_window"`
	// ServerSideInit generates pgbench rows inside the server instead
	// of streaming them from the client. Same data, but a big scale
	// initializes in a fraction of the time, which matters when the
	// scenario exists to measure the hours after the load, not the
	// load itself.
	ServerSideInit bool `json:"server_side_init"`
	// Tenants, WorkingSet, MaxAttached, IdleSecs and SharedBuffers
	// shape a fleet scenario: how many projects the store holds, how
	// many of them the steady phase draws from, and the node's own
	// budgets. The budgets live in the scenario because they are the
	// shape of the deployment being measured, not tuning: a ceiling
	// below the fleet size is what makes the churn phase churn.
	Tenants       int    `json:"tenants"`
	WorkingSet    int    `json:"working_set"`
	MaxAttached   int    `json:"max_attached"`
	IdleSecs      int    `json:"idle_secs"`
	SharedBuffers string `json:"shared_buffers"`
	// Hold is seconds the fleet's hold phase watches a node nobody is
	// asking anything of, sampling every SampleSecs. It is the long
	// tail measurement: what a project costs while it is asleep. A hold
	// longer than IdleSecs covers both halves of that, because the
	// projects the phase before it attached are let go partway through
	// and the same window then measures a node holding nothing.
	Hold       int `json:"hold"`
	SampleSecs int `json:"sample_secs"`
}

// IsREST reports whether the http driver owns this scenario.
func (s Scenario) IsREST() bool { return s.Kind == "rest" }

// IsAttach reports whether the attach driver owns this scenario.
func (s Scenario) IsAttach() bool { return s.Kind == "attach" }

// IsSustain reports whether the soak driver owns this scenario.
func (s Scenario) IsSustain() bool { return s.Kind == "sustain" }

// IsFleet reports whether the many tenant driver owns this scenario.
func (s Scenario) IsFleet() bool { return s.Kind == "fleet" }

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
	if sc.IsREST() && len(sc.Requests) == 0 {
		return Scenario{}, nil, fmt.Errorf("%s: a rest scenario needs requests", path)
	}
	if sc.IsAttach() {
		if sc.Cycles == 0 {
			sc.Cycles = 10
		}
		if sc.Query == "" {
			sc.Query = "select 1"
		}
		if sc.Port == 0 {
			sc.Port = 5432
		}
	}
	if sc.IsFleet() {
		if len(sc.Requests) == 0 {
			return Scenario{}, nil, fmt.Errorf("%s: a fleet scenario needs requests", path)
		}
		if sc.Tenants == 0 {
			sc.Tenants = 1000
		}
		if sc.WorkingSet == 0 {
			sc.WorkingSet = sc.Tenants / 10
		}
		if sc.MaxAttached == 0 {
			sc.MaxAttached = 100
		}
		if sc.IdleSecs == 0 {
			sc.IdleSecs = 300
		}
		if sc.SampleSecs == 0 {
			sc.SampleSecs = 15
		}
		// A working set larger than the fleet is a scenario that
		// cannot do what it says, and the time to say so is at load.
		if sc.WorkingSet > sc.Tenants {
			return Scenario{}, nil, fmt.Errorf("%s: working_set %d is larger than tenants %d", path, sc.WorkingSet, sc.Tenants)
		}
	}
	if sc.IsSustain() {
		if sc.Segment == 0 {
			sc.Segment = 600
		}
		if len(sc.Drills) == 0 {
			sc.Drills = []string{"pusher", "crash", "death"}
		}
		// An unknown drill name is refused at load time rather than
		// hours into a soak, when the rotation finally reaches it and
		// the run so far is wasted.
		for _, d := range sc.Drills {
			switch d {
			case "pusher", "crash", "death":
			default:
				return Scenario{}, nil, fmt.Errorf("%s: unknown drill %q", path, d)
			}
		}
		if sc.CheckpointSecs == 0 {
			sc.CheckpointSecs = 60
		}
		if sc.CompactSecs == 0 {
			sc.CompactSecs = 120
		}
		if sc.Port == 0 {
			sc.Port = 5497
		}
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
