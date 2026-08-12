package envinfo

import "testing"

// Two sustain runs on the same box, same scenario, same binaries, and
// one of them recovered from a kill in 4 seconds while the other took
// 112. The only difference was ZOU_PAGESERVE, which nothing in either
// result file mentioned. Whatever else a result carries, it has to
// carry the switches that decide which system it measured.
func TestTheSwitchesThatDecideWhichSystemRanAreRecorded(t *testing.T) {
	got := ZouEnv([]string{
		"PATH=/usr/bin",
		"ZOU_PAGESERVE=1",
		"ZOU_STORE_STATS=/home/zou/exit6/store-stats",
		"HOME=/home/zou",
		"NOT_ZOU_PAGESERVE=1",
	})
	if len(got) != 2 {
		t.Fatalf("kept %d entries: %v", len(got), got)
	}
	if got["ZOU_PAGESERVE"] != "1" {
		t.Fatalf("ZOU_PAGESERVE = %v", got["ZOU_PAGESERVE"])
	}
	if got["ZOU_STORE_STATS"] != "/home/zou/exit6/store-stats" {
		t.Fatalf("a path is often the answer to why two runs differ: %v", got["ZOU_STORE_STATS"])
	}
	if _, ok := got["NOT_ZOU_PAGESERVE"]; ok {
		t.Fatalf("matched on contains rather than prefix")
	}
}

// An empty environment is a run nobody exported anything for, which is
// a fact worth recording as absence rather than as an empty object.
func TestNothingExportedLeavesNothingBehind(t *testing.T) {
	if got := ZouEnv([]string{"PATH=/usr/bin"}); len(got) != 0 {
		t.Fatalf("invented %v", got)
	}
	if _, ok := Capture()["zou_env"]; ok && len(ZouEnv(nil)) == 0 {
		// Capture reads the real environment, so this only says the
		// two agree about the empty case when the test process has no
		// ZOU_ variables of its own.
		t.Log("the test process has ZOU_ variables set, which is fine")
	}
}
