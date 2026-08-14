package sustain

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseCompactStatusReadsTheArrayAndRefusesGarbage(t *testing.T) {
	shards, err := ParseCompactStatus([]byte(`[{"shard":0,"debt":42,"amp":3.5,"bound":5},{"shard":1,"debt":0,"amp":6.25,"bound":5}]`))
	if err != nil {
		t.Fatal(err)
	}
	if len(shards) != 2 || shards[1].Shard != 1 || shards[1].Amp != 6.25 || shards[0].Debt != 42 || shards[0].Bound != 5 {
		t.Fatalf("shards = %+v", shards)
	}
	amp, over := MaxAmp(shards)
	if amp != 6.25 || !over {
		t.Fatalf("amp = %v over = %v", amp, over)
	}
	// A server log line where the json should be must error, not read
	// as an empty healthy store.
	if _, err := ParseCompactStatus([]byte("checkpoint starting: time")); err == nil {
		t.Fatal("expected an error for non json status output")
	}
}

func TestMaxAmpComparesEachShardAgainstItsOwnBound(t *testing.T) {
	amp, over := MaxAmp([]ShardStatus{
		{Shard: 0, Amp: 4.0, Bound: 5},
		{Shard: 1, Amp: 3.0, Bound: 2},
	})
	if amp != 4.0 || !over {
		t.Fatalf("amp = %v over = %v, shard 1 is over its own bound", amp, over)
	}
	if _, over := MaxAmp([]ShardStatus{{Shard: 0, Amp: 4.0, Bound: 5}}); over {
		t.Fatal("under bound read as over")
	}
}

func TestBoundHeldForgivesTransientsButNotStreaks(t *testing.T) {
	if !BoundHeld(nil) {
		t.Fatal("no samples must not fail the gate")
	}
	// Alternating over and under is the grace case: every over sample
	// is followed by a sweep that brought it back.
	if !BoundHeld([]bool{false, true, false, true, false}) {
		t.Fatal("alternating recovery must pass")
	}
	if BoundHeld([]bool{false, true, true, false}) {
		t.Fatal("two consecutive over samples must fail")
	}
	if !BoundHeld([]bool{true}) {
		t.Fatal("a single trailing over sample has no following sample to condemn it")
	}
}

func TestRTOSummaryGroupsByModeAndSkipsUnmeasured(t *testing.T) {
	f := func(v float64) *float64 { return &v }
	drills := []Drill{
		{Seq: 0, Mode: "crash", RTOms: f(100)},
		{Seq: 1, Mode: "crash", RTOms: f(300)},
		{Seq: 2, Mode: "death"}, // never recovered, must not appear as a zero
		{Seq: 3, Mode: "pusher", RTOms: f(50)},
	}
	s := RTOSummary(drills)
	crash, ok := s["crash"].(map[string]any)
	if !ok {
		t.Fatalf("summary = %v", s)
	}
	if crash["count"].(int) != 2 || crash["max"].(float64) != 300 {
		t.Fatalf("crash = %v", crash)
	}
	if _, ok := s["death"]; ok {
		t.Fatal("an unmeasured drill must stay absent, not fabricate a zero")
	}
	if s["pusher"].(map[string]any)["p50"].(float64) != 50 {
		t.Fatalf("pusher = %v", s["pusher"])
	}
}

func TestLedgerHandsOutEachIdOnceAndChunksTheAcked(t *testing.T) {
	l := NewLedger()
	for i := 1; i <= 5; i++ {
		id := l.Next()
		if id != int64(i) {
			t.Fatalf("Next() = %d, want %d", id, i)
		}
		// id 3 simulates an insert whose ack was lost in a kill: the
		// id is skipped, never retried.
		if id != 3 {
			l.Ack(id)
		}
	}
	if l.Acked() != 4 || l.MaxAcked() != 5 {
		t.Fatalf("acked = %d max = %d", l.Acked(), l.MaxAcked())
	}
	chunks := l.Chunks(3)
	if len(chunks) != 2 {
		t.Fatalf("chunks = %v", chunks)
	}
	var flat []int64
	for _, c := range chunks {
		if len(c) > 3 {
			t.Fatalf("chunk over size: %v", c)
		}
		flat = append(flat, c...)
	}
	want := []int64{1, 2, 4, 5}
	if len(flat) != len(want) {
		t.Fatalf("flat = %v", flat)
	}
	for i := range want {
		if flat[i] != want[i] {
			t.Fatalf("flat = %v, want %v", flat, want)
		}
	}
}

// A check that could not run and an identity that came back broken
// both read as false, so the reason has to survive into the result
// file or every read failure looks like lost data.
func TestDrillCarriesWhyACheckCouldNotRun(t *testing.T) {
	broke := false
	d := Drill{Seq: 6, Mode: "death", LedgerOK: false, BalanceOK: &broke,
		CheckError: "balance check: read tcp: connection reset by peer"}
	out, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), `"check_error":"balance check: read tcp: connection reset by peer"`) {
		t.Fatalf("drill = %s", out)
	}
	clean, err := json.Marshal(Drill{Seq: 7, Mode: "crash", LedgerOK: true})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(clean), "check_error") {
		t.Fatalf("a drill whose checks ran carries no error: %s", clean)
	}
}

func TestParseFoldReportReadsWhatTheFoldDid(t *testing.T) {
	raw := []byte(`{"horizon":"0/8B000000","data_checksums":true,"shards":[` +
		`{"shard":0,"horizon":"0/8B000000","retired":12,"outputs":2,"imaged":3400,"unbased":1,"pinned":1,"bytes_before":136770905,"bytes_after":8715593},` +
		`{"shard":1,"horizon":"0/8B000000","retired":3,"outputs":1,"imaged":900,"unbased":0,"pinned":0,"bytes_before":2000,"bytes_after":500}]}`)
	rep, err := ParseFoldReport(raw)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Horizon != "0/8B000000" {
		t.Fatalf("horizon %q", rep.Horizon)
	}
	if !rep.DataChecksums {
		t.Fatal("the fold said checksums were on and the report dropped it")
	}
	if len(rep.Shards) != 2 {
		t.Fatalf("shards %d", len(rep.Shards))
	}
	before, after, retired := rep.FoldTotals()
	if before != 136772905 || after != 8716093 || retired != 15 {
		t.Fatalf("totals %d %d %d", before, after, retired)
	}
}

// A fold that was allowed to cut nowhere still ran, and reads as a
// report with no shards rather than as a failure. The distinction
// matters on a young store, where the oldest checkpoint is genesis and
// there is nothing below it to fold for hours.
func TestParseFoldReportAcceptsAFoldThatMovedNothing(t *testing.T) {
	rep, err := ParseFoldReport([]byte(`{"horizon":"0/0","shards":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	before, after, retired := rep.FoldTotals()
	if before != 0 || after != 0 || retired != 0 {
		t.Fatalf("totals %d %d %d", before, after, retired)
	}
}

// Noise has to be an error, not an empty report: a sample recorded
// from garbage would read as a fold that ran and found nothing, which
// is the one outcome indistinguishable from a healthy store.
func TestParseFoldReportRefusesNoise(t *testing.T) {
	for _, raw := range []string{"", "not json", "[]", `{"shards":[]}`} {
		if _, err := ParseFoldReport([]byte(raw)); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
}
