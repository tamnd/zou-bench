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
	// Driver is pgbench when empty, or wire for the in process client
	// that sends the same tpcb-like transaction and times every
	// statement in it. pgbench reports per statement latencies as means
	// only, so a bound on one statement's tail cannot be read off a
	// pgbench run at all. The wire driver runs the same shape and is
	// otherwise the slower of the two, so it is asked for by name
	// rather than being what a scenario gets by default.
	Driver string `json:"driver"`
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
	// FoldSecs drives the merge fold, off when zero. Collection only
	// deletes what a compaction sweep retired, and an ordinary sweep
	// retires nothing below the newest image: an old image is the base
	// some read under it needs and every record the tenant wrote sits
	// in some delta, so the page layers grow with the write volume of
	// the run and nothing ever brings them down. The fold is what buys
	// the right to drop them, and it folds no higher than the oldest
	// lsn a checkpoint inside GCRetention still names, so the two
	// cadences are one policy read twice. It is expensive by design,
	// so this belongs in the minutes, not in the seconds the sweep
	// runs on.
	FoldSecs int `json:"fold_secs"`
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
	// SettleSecs is the head of the hold that is sampled and then left
	// out of the rates, because a node that has just been under load is
	// still writing the load down, and that is the load's cost rather
	// than a sleeping project's.
	// DrainSecs is the same idea on the far side of the transition: the
	// gauge reads zero as soon as the slots are dropped, and the
	// postmasters go on flushing after that, so the seconds following
	// the last detach are charged to neither window.
	Hold       int `json:"hold"`
	SampleSecs int `json:"sample_secs"`
	SettleSecs int `json:"settle_secs"`
	DrainSecs  int `json:"drain_secs"`
	// Sockets, Shards, ConnectRate, Rows, Batch, Writers and
	// HeartbeatSecs shape a sockets scenario: how many websockets the
	// node holds, how many change subscriptions they divide themselves
	// between, how fast they are opened, and the write load committed
	// under them. Shards is what turns one committed row into a fan out,
	// because every socket on a shard is owed the same row, so the
	// deliveries a run expects are the rows times the sockets per shard
	// rather than the rows.
	// Table is what the sockets subscribe to and the writer writes, and
	// DrainSecs is read here too: on a sockets run it is the seconds the
	// sockets are read after the window closes, so a row committed in the
	// last instant of it still has somewhere to land.
	Sockets       int    `json:"sockets"`
	Shards        int    `json:"shards"`
	ConnectRate   int    `json:"connect_rate"`
	Rows          int    `json:"rows"`
	Batch         int    `json:"batch"`
	Writers       int    `json:"writers"`
	HeartbeatSecs int    `json:"heartbeat_secs"`
	Table         string `json:"table"`
}

// IsREST reports whether the http driver owns this scenario.
func (s Scenario) IsREST() bool { return s.Kind == "rest" }

// IsAttach reports whether the attach driver owns this scenario.
func (s Scenario) IsAttach() bool { return s.Kind == "attach" }

// IsSustain reports whether the soak driver owns this scenario.
func (s Scenario) IsSustain() bool { return s.Kind == "sustain" }

// IsFleet reports whether the many tenant driver owns this scenario.
func (s Scenario) IsFleet() bool { return s.Kind == "fleet" }

// IsSockets reports whether the realtime driver owns this scenario.
func (s Scenario) IsSockets() bool { return s.Kind == "sockets" }

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
	if sc.IsSockets() {
		if sc.Sockets == 0 {
			return Scenario{}, nil, fmt.Errorf("%s: a sockets scenario needs sockets", path)
		}
		if sc.Table == "" {
			sc.Table = "pulse"
		}
		if sc.Shards == 0 {
			sc.Shards = 1
		}
		// A shard nobody joined would take rows the run then reports as
		// undelivered, so the arithmetic is refused at load rather than
		// blamed on the node an hour later.
		if sc.Shards > sc.Sockets {
			return Scenario{}, nil, fmt.Errorf("%s: %d shards and only %d sockets, so some shard would have nobody on it", path, sc.Shards, sc.Sockets)
		}
		if sc.Batch == 0 {
			sc.Batch = 25
		}
		if sc.Writers == 0 {
			sc.Writers = 4
		}
		if sc.DrainSecs == 0 {
			sc.DrainSecs = 10
		}
		if sc.HeartbeatSecs == 0 {
			sc.HeartbeatSecs = 30
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
	// The wire driver knows one transaction, which is the one it exists
	// to time. A scenario that names another workload and this driver
	// is asking for something it will not get, and it is cheaper to
	// hear that now than to read a result file later and wonder which
	// of the two the numbers came from.
	switch sc.Driver {
	case "", "pgbench":
	case "wire":
		if sc.Script != "" {
			return Scenario{}, nil, fmt.Errorf("%s: the wire driver runs tpcb-like, not the script %s", path, sc.Script)
		}
		if sc.Builtin != "" && sc.Builtin != "tpcb-like" {
			return Scenario{}, nil, fmt.Errorf("%s: the wire driver runs tpcb-like, not %s", path, sc.Builtin)
		}
		if sc.Rate > 0 {
			return Scenario{}, nil, fmt.Errorf("%s: the wire driver does not pace itself, so it cannot hold a rate of %d", path, sc.Rate)
		}
	default:
		return Scenario{}, nil, fmt.Errorf("%s: unknown driver %q", path, sc.Driver)
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
