package scenario

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAppliesDefaultsAndKeepsTheRawDocument(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s.json")
	if err := os.WriteFile(path, []byte(`{"name":"x","scale":10}`), 0o644); err != nil {
		t.Fatal(err)
	}
	sc, doc, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if sc.Clients != 8 || sc.Threads != 8 || sc.Duration != 60 {
		t.Fatalf("defaults = %+v", sc)
	}
	if doc["scale"].(float64) != 10 {
		t.Fatalf("doc = %v", doc)
	}
}

func TestLoadRejectsANamelessScenario(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s.json")
	if err := os.WriteFile(path, []byte(`{"scale":10}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Load(path); err == nil {
		t.Fatal("expected an error for a nameless scenario")
	}
}

func TestSustainDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s.json")
	if err := os.WriteFile(path, []byte(`{"name":"x","kind":"sustain","duration":900}`), 0o644); err != nil {
		t.Fatal(err)
	}
	sc, _, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !sc.IsSustain() {
		t.Fatal("kind sustain did not answer IsSustain")
	}
	if sc.Segment != 600 || sc.CheckpointSecs != 60 || sc.CompactSecs != 120 || sc.Port != 5497 {
		t.Fatalf("defaults = %+v", sc)
	}
	if len(sc.Drills) != 3 || sc.Drills[0] != "pusher" || sc.Drills[1] != "crash" || sc.Drills[2] != "death" {
		t.Fatalf("drills = %v", sc.Drills)
	}
}

func TestSustainRejectsAnUnknownDrill(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s.json")
	if err := os.WriteFile(path, []byte(`{"name":"x","kind":"sustain","drills":["pusher","reboot"]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Load(path); err == nil {
		t.Fatal("expected an error for an unknown drill name")
	}
}

func TestFleetDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s.json")
	body := `{"name":"x","kind":"fleet","requests":[{"name":"r","path":"/t"}]}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	sc, _, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !sc.IsFleet() {
		t.Fatal("kind fleet did not answer IsFleet")
	}
	if sc.Tenants != 1000 || sc.WorkingSet != 100 || sc.MaxAttached != 100 || sc.IdleSecs != 300 {
		t.Fatalf("defaults = %+v", sc)
	}
}

func TestFleetRefusesAWorkingSetLargerThanTheFleet(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s.json")
	body := `{"name":"x","kind":"fleet","tenants":10,"working_set":50,"requests":[{"name":"r","path":"/t"}]}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Load(path); err == nil {
		t.Fatal("expected an error for a working set larger than the fleet")
	}
}

func TestFleetNeedsRequests(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s.json")
	if err := os.WriteFile(path, []byte(`{"name":"x","kind":"fleet"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Load(path); err == nil {
		t.Fatal("expected an error for a fleet scenario with no requests")
	}
}

func TestSocketsFillsInWhatARunNeedsAndRefusesEmptyShards(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s.json")
	body := `{"name":"x","kind":"sockets","sockets":1000,"shards":100}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	sc, _, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if sc.Table != "pulse" || sc.Batch != 25 || sc.Writers != 4 || sc.DrainSecs != 10 || sc.HeartbeatSecs != 30 {
		t.Fatalf("defaults = %+v", sc)
	}

	// Shards nobody joined would take rows that then read as
	// undelivered, so the arithmetic is refused before a run, not
	// explained after one.
	empty := filepath.Join(dir, "empty.json")
	if err := os.WriteFile(empty, []byte(`{"name":"x","kind":"sockets","sockets":10,"shards":100}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Load(empty); err == nil {
		t.Fatal("expected an error for more shards than sockets")
	}

	none := filepath.Join(dir, "none.json")
	if err := os.WriteFile(none, []byte(`{"name":"x","kind":"sockets"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Load(none); err == nil {
		t.Fatal("expected an error for a sockets scenario with no sockets")
	}
}

func TestEveryCheckedInScenarioLoads(t *testing.T) {
	entries, err := os.ReadDir("../scenarios")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		// Custom pgbench scripts live next to the scenarios that name
		// them, only the json documents are scenarios.
		if filepath.Ext(e.Name()) != ".json" {
			continue
		}
		if _, _, err := Load(filepath.Join("../scenarios", e.Name())); err != nil {
			t.Fatal(err)
		}
	}
}
