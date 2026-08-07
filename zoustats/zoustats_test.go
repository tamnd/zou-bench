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
