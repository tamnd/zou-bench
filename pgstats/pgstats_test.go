package pgstats

import "testing"

func TestDeltaSubtractsNumericFieldsViewByView(t *testing.T) {
	before := map[string]map[string]any{
		"wal": {"wal_bytes": 1000.0, "wal_records": 10.0, "stats_reset": "2026-08-01"},
		"io":  {"fsyncs": 5.0},
	}
	after := map[string]map[string]any{
		"wal": {"wal_bytes": 5000.0, "wal_records": 42.0, "stats_reset": "2026-08-01"},
		"io":  {"fsyncs": 25.0},
		"new": {"only_after": 1.0},
	}
	d := Delta(before, after)
	if d["wal"]["wal_bytes"] != 4000 || d["wal"]["wal_records"] != 32 {
		t.Fatalf("wal delta = %v", d["wal"])
	}
	if d["io"]["fsyncs"] != 20 {
		t.Fatalf("io delta = %v", d["io"])
	}
	if _, present := d["wal"]["stats_reset"]; present {
		t.Fatal("non numeric fields must drop out")
	}
	if _, present := d["new"]; present {
		t.Fatal("views missing on one side must drop out")
	}
}
