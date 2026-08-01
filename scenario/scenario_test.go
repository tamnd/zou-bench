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

func TestEveryCheckedInScenarioLoads(t *testing.T) {
	entries, err := os.ReadDir("../scenarios")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if _, _, err := Load(filepath.Join("../scenarios", e.Name())); err != nil {
			t.Fatal(err)
		}
	}
}
