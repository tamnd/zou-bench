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
