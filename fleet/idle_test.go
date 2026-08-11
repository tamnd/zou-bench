package fleet

import "testing"

// A hold that outlasts the node's idle budget contains both windows,
// and the two rates have to come apart cleanly: ten seconds with two
// projects attached and ten with none, with the puts landing in the
// first half.
func TestRatesSplitTheWindowByWhatWasAttached(t *testing.T) {
	got := Rates([]Sample{
		{Elapsed: 0, Attached: 2, Puts: 0},
		{Elapsed: 10, Attached: 2, Puts: 20},
		{Elapsed: 20, Attached: 0, Puts: 40},
		{Elapsed: 30, Attached: 0, Puts: 41},
	})
	if !got.SawDormant {
		t.Fatal("a window that ended with nothing attached saw dormant")
	}
	if got.AttachedSeconds != 20 || got.DormantSeconds != 10 {
		t.Fatalf("seconds attached %v dormant %v", got.AttachedSeconds, got.DormantSeconds)
	}
	if got.ProjectSeconds != 40 {
		t.Fatalf("project seconds = %v, two projects for twenty seconds", got.ProjectSeconds)
	}
	// One put in ten dormant seconds is 360 an hour, node wide.
	if got.DormantPerHour.Puts != 360 {
		t.Fatalf("dormant puts an hour = %v", got.DormantPerHour.Puts)
	}
	// Forty puts over the attached window, of which the node's own
	// housekeeping accounts for 2 (360 an hour for twenty seconds), so
	// 38 over forty project seconds is 3420 an hour.
	if got.AttachedPerProjectHour.Puts != 3420 {
		t.Fatalf("attached puts per project hour = %v", got.AttachedPerProjectHour.Puts)
	}
}

// Without a dormant window there is nothing to subtract, and the honest
// answer is the gross rate with a flag saying it is one.
func TestAWindowThatNeverWentDormantSaysSo(t *testing.T) {
	got := Rates([]Sample{
		{Elapsed: 0, Attached: 1, Gets: 0},
		{Elapsed: 3600, Attached: 1, Gets: 100},
	})
	if got.SawDormant {
		t.Fatal("nothing was ever dormant")
	}
	if got.AttachedPerProjectHour.Gets != 100 {
		t.Fatalf("gets per project hour = %v", got.AttachedPerProjectHour.Gets)
	}
}

// A counter that shrank is a node that restarted, and there is no rate
// across that, so the interval is dropped rather than counted negative.
func TestARestartedCounterDropsItsIntervalRatherThanGoingNegative(t *testing.T) {
	got := Rates([]Sample{
		{Elapsed: 0, Attached: 0, Puts: 100},
		{Elapsed: 10, Attached: 0, Puts: 5},
		{Elapsed: 20, Attached: 0, Puts: 15},
	})
	if got.DormantSeconds != 10 {
		t.Fatalf("dormant seconds = %v, the restarted interval is not one", got.DormantSeconds)
	}
	if got.DormantPerHour.Puts != 3600 {
		t.Fatalf("dormant puts an hour = %v", got.DormantPerHour.Puts)
	}
}

// The settle is the flush the phase before left behind, and a rate taken
// through it is that phase's number rather than a sleeping project's.
func TestASettleLeavesTheLoadsOwnFlushOutOfTheRate(t *testing.T) {
	samples := []Sample{
		{Elapsed: 0, Attached: 1, Puts: 0},
		{Elapsed: 60, Attached: 1, Puts: 5000},
		{Elapsed: 120, Attached: 1, Puts: 5001},
		{Elapsed: 180, Attached: 1, Puts: 5002},
	}
	kept := After(samples, 120)
	if len(kept) != 2 || kept[0].Elapsed != 120 {
		t.Fatalf("kept %+v", kept)
	}
	// One put a minute rather than the five thousand the load left.
	if got := Rates(kept).AttachedPerProjectHour.Puts; got != 60 {
		t.Fatalf("puts per project hour = %v", got)
	}
}

func TestOneSampleIsNoRateAtAll(t *testing.T) {
	got := Rates([]Sample{{Elapsed: 0, Attached: 3}})
	if got.SawDormant || got.AttachedSeconds != 0 || got.Samples != 1 {
		t.Fatalf("%+v", got)
	}
}
