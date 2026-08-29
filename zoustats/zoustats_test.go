package zoustats

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// write builds a counter file the way zou lays it out, so Read is
// tested against the real format and not against itself.
func write(t *testing.T, mutate func(c *Counters)) string {
	t.Helper()
	var c Counters
	c[0] = magic
	c[1] = format
	if mutate != nil {
		mutate(&c)
	}
	raw := make([]byte, slots*8)
	for i, v := range c {
		binary.LittleEndian.PutUint64(raw[i*8:], v)
	}
	path := filepath.Join(t.TempDir(), "store-stats")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReadRejectsForeignFiles(t *testing.T) {
	dir := t.TempDir()
	short := filepath.Join(dir, "short")
	os.WriteFile(short, []byte("hello"), 0o644)
	if _, err := Read(short); err == nil {
		t.Fatal("short file accepted")
	}
	zeros := filepath.Join(dir, "zeros")
	os.WriteFile(zeros, make([]byte, slots*8), 0o644)
	if _, err := Read(zeros); err == nil {
		t.Fatal("zeroed file accepted")
	}
	if _, err := Read(filepath.Join(dir, "absent")); err == nil {
		t.Fatal("missing file accepted")
	}
}

func TestDiffSubtractsAndCatchesRestarts(t *testing.T) {
	before, err := Read(write(t, func(c *Counters) {
		c[countSlot(3, 3)] = 100 // put, shards class
	}))
	if err != nil {
		t.Fatal(err)
	}
	after, err := Read(write(t, func(c *Counters) {
		c[countSlot(3, 3)] = 350
		c[countSlot(3, 3)+1] = 8192 * 250
	}))
	if err != nil {
		t.Fatal(err)
	}
	d, err := Diff(before, after)
	if err != nil {
		t.Fatal(err)
	}
	if got := d[countSlot(3, 3)]; got != 250 {
		t.Fatalf("put count delta %d, want 250", got)
	}
	if _, err := Diff(after, before); err == nil {
		t.Fatal("shrinking counter not caught")
	}
}

func TestReportAndSumSplitKindsAndClasses(t *testing.T) {
	c, err := Read(write(t, func(c *Counters) {
		c[countSlot(0, 3)] = 9000 // get, page
		c[countSlot(0, 3)+1] = 9000 * 8192
		c[countSlot(2, 1)] = 30 // put_if_match, wal
		c[countSlot(2, 1)+1] = 30 * 1024
		c[countSlot(3, 3)] = 6000 // put, shards
		c[countSlot(3, 3)+1] = 6000 * 8192
		c[bucketBase+0*buckets+10] = 9000 // all gets in [1024us, 2048us)
		c[conflictSlot] = 2
	}))
	if err != nil {
		t.Fatal(err)
	}
	r := Report(c)
	ops := r["ops"].(map[string]any)
	if _, there := ops["delete"]; there {
		t.Fatal("op kind with no traffic reported")
	}
	get := ops["get"].(map[string]any)
	if get["count"].(uint64) != 9000 || get["p50_us"].(uint64) != 2048 {
		t.Fatalf("get block wrong: %v", get)
	}
	if r["cas_conflicts"].(uint64) != 2 {
		t.Fatal("conflicts lost")
	}

	s := Sum(c)
	if s.Gets != 9000 || s.Puts != 6030 || s.GetBytes != 9000*8192 {
		t.Fatalf("sum wrong: %+v", s)
	}
	if !s.AnyTraffic {
		t.Fatal("traffic not seen")
	}
}

func TestReportCarriesReadTiers(t *testing.T) {
	c, err := Read(write(t, func(c *Counters) {
		c[tierSlot(0)] = 90     // cache calls
		c[tierSlot(0)+1] = 400  // cache pages
		c[tierSlot(0)+2+6] = 90 // all inside 128 us
		c[tierSlot(2)] = 10
		c[tierSlot(2)+1] = 10
		c[tierSlot(2)+2+14] = 10 // 16 to 32 ms
	}))
	if err != nil {
		t.Fatal(err)
	}
	report := Report(c)
	reads, ok := report["reads"].(map[string]any)
	if !ok {
		t.Fatal("no reads block")
	}
	cache := reads["cache"].(map[string]any)
	if cache["calls"].(uint64) != 90 || cache["pages"].(uint64) != 400 {
		t.Fatalf("cache tier wrong: %v", cache)
	}
	if p50 := cache["p50_us"].(uint64); p50 != 128 {
		t.Fatalf("cache p50 %d", p50)
	}
	store := reads["store"].(map[string]any)
	if p50 := store["p50_us"].(uint64); p50 != 32768 {
		t.Fatalf("store p50 %d", p50)
	}
	if _, there := reads["local"]; there {
		t.Fatal("empty tier reported")
	}
}

// A run has to be able to answer where the page service time went,
// otherwise a slow socket and a serve loop stuck in ingest look the
// same from outside.
func TestReportCarriesPageServicePhases(t *testing.T) {
	c, err := Read(write(t, func(c *Counters) {
		c[phaseSlot(0)] = 12       // park
		c[phaseSlot(0)+1+16] = 12  // 64 to 128 ms
		c[phaseSlot(1)] = 500      // read
		c[phaseSlot(1)+1+7] = 500  // inside 256 us
		c[phaseSlot(2)] = 300      // ingest
		c[phaseSlot(2)+1+12] = 300 // 4 to 8 ms
	}))
	if err != nil {
		t.Fatal(err)
	}
	svc, ok := Report(c)["pagesvc"].(map[string]any)
	if !ok {
		t.Fatal("no pagesvc block")
	}
	park := svc["park"].(map[string]any)
	if park["calls"].(uint64) != 12 || park["p50_us"].(uint64) != 131072 {
		t.Fatalf("park phase wrong: %v", park)
	}
	if p50 := svc["read"].(map[string]any)["p50_us"].(uint64); p50 != 256 {
		t.Fatalf("read p50 %d", p50)
	}
	if p50 := svc["ingest"].(map[string]any)["p50_us"].(uint64); p50 != 8192 {
		t.Fatalf("ingest p50 %d", p50)
	}
}

// The commit steps are the only place a result says whether a write
// waited on the store or on the pipeline in front of it, and they sit
// past the phases in the file, so a decode that stops early would drop
// them silently rather than fail.
func TestReportCarriesCommitSteps(t *testing.T) {
	c, err := Read(write(t, func(c *Counters) {
		c[stepSlot(0)] = 400      // push
		c[stepSlot(0)+1+11] = 400 // 2 to 4 ms
		c[stepSlot(4)] = 90       // put
		c[stepSlot(4)+1+13] = 90  // 8 to 16 ms
		c[stepSlot(6)] = 400      // durable
		c[stepSlot(6)+1+14] = 400 // 16 to 32 ms
	}))
	if err != nil {
		t.Fatal(err)
	}
	rep := Report(c)
	commit, ok := rep["commit"].(map[string]any)
	if !ok {
		t.Fatal("no commit block")
	}
	if len(commit) != 3 {
		t.Fatalf("steps with no samples were reported: %v", commit)
	}
	push := commit["push"].(map[string]any)
	if push["samples"].(uint64) != 400 || push["p50_us"].(uint64) != 4096 {
		t.Fatalf("push step wrong: %v", push)
	}
	if p50 := commit["put"].(map[string]any)["p50_us"].(uint64); p50 != 16384 {
		t.Fatalf("put p50 %d", p50)
	}
	if p50 := commit["durable"].(map[string]any)["p50_us"].(uint64); p50 != 32768 {
		t.Fatalf("durable p50 %d", p50)
	}
	if _, spilled := rep["pagesvc"]; spilled {
		t.Fatal("commit samples leaked into the page service phases")
	}
}

// The park causes and the gap histogram sit past the commit steps, and
// they are the two counters that separate a page service which is
// behind from one being asked for a position nothing has reached. A
// decode that got the base wrong would read them out of the commit
// steps and report a plausible number.
func TestReportCarriesParkCausesAndGap(t *testing.T) {
	c, err := Read(write(t, func(c *Counters) {
		c[causeBase+0] = 7   // touched
		c[causeBase+1] = 120 // untouched
		c[gapBase] = 127
		c[gapBase+1+20] = 127 // 1 to 2 MiB of wal ahead
	}))
	if err != nil {
		t.Fatal(err)
	}
	rep := Report(c)
	cause, ok := rep["park_cause"].(map[string]any)
	if !ok {
		t.Fatal("no park_cause block")
	}
	if cause["touched"].(uint64) != 7 || cause["untouched"].(uint64) != 120 {
		t.Fatalf("park causes wrong: %v", cause)
	}
	if _, there := cause["unclear"]; there {
		t.Fatal("a cause nothing hit was reported")
	}
	gap, ok := rep["park_gap"].(map[string]any)
	if !ok {
		t.Fatal("no park_gap block")
	}
	if gap["samples"].(uint64) != 127 {
		t.Fatalf("gap samples %v", gap["samples"])
	}
	if p50 := gap["p50_bytes"].(uint64); p50 != 1<<21 {
		t.Fatalf("gap p50 %d bytes", p50)
	}
	if _, wrongUnit := gap["p50_us"]; wrongUnit {
		t.Fatal("wal bytes reported under a microsecond heading")
	}
	if _, spilled := rep["commit"]; spilled {
		t.Fatal("park counters leaked into the commit steps")
	}
}

// A put that was slow because the bucket asked for less traffic and a
// put that was slow because the object was large are the same latency
// bucket, and this is the only counter that tells them apart.
func TestReportCarriesRetriesAndPacking(t *testing.T) {
	c, err := Read(write(t, func(c *Counters) {
		c[retryBase+0] = 44 // throttle
		c[retryBase+3] = 1  // exhausted
		c[packSlot(0)] = 4000
		c[packSlot(0)+1] = 1000
		c[packSlot(1)] = 900
		c[packSlot(1)+1] = 900 // wal frames that did not compress
	}))
	if err != nil {
		t.Fatal(err)
	}
	rep := Report(c)
	retry, ok := rep["retries"].(map[string]any)
	if !ok {
		t.Fatal("no retries block")
	}
	if retry["throttle"].(uint64) != 44 || retry["exhausted"].(uint64) != 1 {
		t.Fatalf("retries wrong: %v", retry)
	}
	if _, there := retry["server"]; there {
		t.Fatal("a retry reason nothing hit was reported")
	}
	packed, ok := rep["packed"].(map[string]any)
	if !ok {
		t.Fatal("no packed block")
	}
	layer := packed["layer"].(map[string]any)
	if layer["raw_bytes"].(uint64) != 4000 || layer["ratio"].(float64) != 4 {
		t.Fatalf("layer packing wrong: %v", layer)
	}
	if ratio := packed["wal"].(map[string]any)["ratio"].(float64); ratio != 1 {
		t.Fatalf("incompressible wal reported a ratio of %v rather than one", ratio)
	}
	if _, there := packed["file"]; there {
		t.Fatal("a compressor caller with no traffic was reported")
	}
}

// The layout is written on the other side of a language boundary, in
// crates/zou-store/src/stats.rs, and nothing in a Go build checks it.
// So the total is pinned here: a section that grows or moves over
// there changes this number, and a reader that agrees on the format
// byte while disagreeing on where the sections start decodes garbage
// that looks like measurements.
func TestSlotCountMatchesTheRustLayout(t *testing.T) {
	if slots != 830 {
		t.Fatalf("layout is %d slots, zou format 8 is 830", slots)
	}
	if packSlot(packs-1)+1 != slots-1 {
		t.Fatal("the last section does not end at the last slot")
	}
}
