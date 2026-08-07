package sustain

import "testing"

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
