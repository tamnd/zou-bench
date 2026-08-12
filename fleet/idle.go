package fleet

// What a node costs when nobody is asking it for anything.
//
// The steady and churn phases measure a node under load, which is the
// number a fleet is sized on. The long tail question is the other one:
// eight hundred projects that are mostly asleep, where the bill is not
// throughput but whatever the node does on its own, forever, per
// project. That is a rate rather than a total, and it is only a rate if
// it is watched over a window with nothing in it.
//
// So a hold phase asks for nothing and samples two things on a cadence:
// how many projects the node is holding, and the store counters. The
// gauge matters because the two rates are different questions. A
// project the node has attached is paying for a lease it renews, and a
// project nobody has asked for since the idle budget expired is a
// prefix in a bucket, which should be paying for nothing at all.
//
// Both rates fall out of one window, because a hold longer than the
// node's idle budget contains both: the projects are attached at the
// start of it and detached by the end.
//
// The first minutes of a hold are not that window, though. A node that
// has just been under load is still writing the load down, and that work
// is the load's cost rather than the sleeping project's, so a settle is
// sampled and then thrown away, see After.
//
// The same is true on the other side of the transition. A project is let
// go by stopping its postmaster, which shuts down and flushes on its own
// time, while the gauge says zero the moment the slot is dropped. Work
// arriving in that stretch belongs to the projects that just left, so
// the drain after the last detach is left out too, see Rates.

import "sort"

// Sample is one reading: seconds since the hold began, how many
// projects the node had attached, and the store counters as they stood.
// The counters are cumulative for the life of the node, so a rate is
// the difference between two of these over the seconds between them.
type Sample struct {
	Elapsed  float64 `json:"t"`
	Attached int     `json:"attached"`
	Puts     int64   `json:"puts"`
	Gets     int64   `json:"gets"`
	Lists    int64   `json:"lists"`
	Deletes  int64   `json:"deletes"`
}

// Ops is a count of store operations, or a rate of them, by the four
// kinds a price card charges for separately.
type Ops struct {
	Puts    float64 `json:"puts"`
	Gets    float64 `json:"gets"`
	Lists   float64 `json:"lists"`
	Deletes float64 `json:"deletes"`
}

func (o Ops) scale(by float64) Ops {
	return Ops{o.Puts * by, o.Gets * by, o.Lists * by, o.Deletes * by}
}

func (o Ops) add(b Ops) Ops {
	return Ops{o.Puts + b.Puts, o.Gets + b.Gets, o.Lists + b.Lists, o.Deletes + b.Deletes}
}

// Total is every kind added up, for the one line summary. A price card
// charges four different prices, so this is never what a dollar is
// computed from.
func (o Ops) Total() float64 { return o.Puts + o.Gets + o.Lists + o.Deletes }

// Idle is what a window with no traffic in it says.
//
// The two rates are node wide and per project respectively, and they
// are not the same shape on purpose. Nothing attached is one node doing
// its own housekeeping, and that cost does not multiply by how many
// projects are registered. An attached project renews its own lease, so
// that one does.
type Idle struct {
	// Seconds the node spent holding nothing and holding something.
	DormantSeconds  float64 `json:"dormant_seconds"`
	AttachedSeconds float64 `json:"attached_seconds"`
	// Project seconds: an interval with a hundred projects attached for
	// ten seconds is a thousand of these, which is what a per project
	// rate divides by.
	ProjectSeconds float64 `json:"project_seconds"`
	// The ops each of those windows saw.
	DormantOps  Ops `json:"dormant_ops"`
	AttachedOps Ops `json:"attached_ops"`
	// Node wide ops an hour with nothing attached.
	DormantPerHour Ops `json:"dormant_ops_per_hour"`
	// Ops an hour one attached project costs, with the node's own
	// dormant rate taken out of it first, so this is the project and
	// not the node it happens to be on.
	AttachedPerProjectHour Ops `json:"attached_ops_per_project_hour"`
	// Seconds after the last detach that were left out of the dormant
	// window, being the stopping projects still flushing.
	DrainSeconds float64 `json:"drain_seconds"`
	// False when the window never went dormant, in which case the
	// per project rate carries the node's housekeeping in it and is an
	// upper bound rather than a measurement.
	SawDormant bool `json:"saw_dormant"`
	Samples    int  `json:"samples"`
}

// After drops the samples taken in the first `seconds` of a hold, which
// is how a settle is spent: sampled, kept in the result file so the
// throwing away is visible, and left out of the rates.
//
// The phase before the hold flushed a load, and a node writes a load
// down after answering it. Charging that to a project that is asleep
// would answer the wrong question, and it would answer it differently
// depending on how hard the phase before happened to push.
func After(samples []Sample, seconds float64) []Sample {
	var out []Sample
	for _, s := range samples {
		if s.Elapsed >= seconds {
			out = append(out, s)
		}
	}
	return out
}

// Rates classifies every interval between consecutive samples by what
// the node was holding at the start of it, and turns the two piles into
// rates.
//
// The start of the interval rather than the end because the counters
// grew during it: an interval that begins with a hundred attached and
// ends with none was a hundred attached for most of it, and the
// detaches themselves are work the attached rate should carry rather
// than work a dormant node is blamed for.
//
// `drain` is how long after the last sample that still saw something
// attached the node is taken to be stopping rather than stopped. Those
// intervals are counted in neither window: the postmasters flushing in
// them are not the node's own housekeeping, and charging them to the
// projects that are already gone would price a detach twice.
func Rates(samples []Sample, drain float64) Idle {
	out := Idle{Samples: len(samples), DrainSeconds: drain}
	if len(samples) < 2 {
		return out
	}
	sort.SliceStable(samples, func(i, j int) bool { return samples[i].Elapsed < samples[j].Elapsed })
	// When the node last held something, so an interval can ask how
	// long ago that was.
	lastAttached := -1.0
	for _, s := range samples {
		if s.Attached > 0 {
			lastAttached = s.Elapsed
		}
	}
	for i := 1; i < len(samples); i++ {
		prev, cur := samples[i-1], samples[i]
		seconds := cur.Elapsed - prev.Elapsed
		if seconds <= 0 {
			continue
		}
		// A counter that went backwards means the node restarted under
		// the hold, and there is no honest rate across that, so the
		// interval is dropped rather than counted as a negative one.
		ops := Ops{
			Puts:    float64(cur.Puts - prev.Puts),
			Gets:    float64(cur.Gets - prev.Gets),
			Lists:   float64(cur.Lists - prev.Lists),
			Deletes: float64(cur.Deletes - prev.Deletes),
		}
		if ops.Puts < 0 || ops.Gets < 0 || ops.Lists < 0 || ops.Deletes < 0 {
			continue
		}
		if prev.Attached == 0 {
			if lastAttached >= 0 && prev.Elapsed < lastAttached+drain {
				continue
			}
			out.DormantSeconds += seconds
			out.DormantOps = out.DormantOps.add(ops)
			continue
		}
		out.AttachedSeconds += seconds
		out.ProjectSeconds += seconds * float64(prev.Attached)
		out.AttachedOps = out.AttachedOps.add(ops)
	}
	if out.DormantSeconds > 0 {
		out.SawDormant = true
		out.DormantPerHour = out.DormantOps.scale(3600 / out.DormantSeconds)
	}
	if out.ProjectSeconds > 0 {
		// The node's own rate, charged for the seconds the attached
		// window lasted, is not the projects' cost, so it comes out
		// before the division. Without a dormant window there is
		// nothing to take out and the answer says so.
		own := out.DormantPerHour.scale(out.AttachedSeconds / 3600)
		net := Ops{
			Puts:    max0(out.AttachedOps.Puts - own.Puts),
			Gets:    max0(out.AttachedOps.Gets - own.Gets),
			Lists:   max0(out.AttachedOps.Lists - own.Lists),
			Deletes: max0(out.AttachedOps.Deletes - own.Deletes),
		}
		out.AttachedPerProjectHour = net.scale(3600 / out.ProjectSeconds)
	}
	return out
}

func max0(v float64) float64 {
	if v < 0 {
		return 0
	}
	return v
}
