// Package pgstats snapshots the server's own counters through psql,
// before and after a run, and computes the deltas. pg_stat_wal says
// how many WAL bytes the workload really generated, pg_stat_io says
// how the server touched storage, and neither is visible from the
// client side summary. Everything rides row_to_json so the parser
// survives column changes between Postgres versions.
package pgstats

import (
	"encoding/json"
	"os/exec"
	"strings"
)

var views = map[string]string{
	"wal":        "select row_to_json(w) from pg_stat_wal w",
	"bgwriter":   "select row_to_json(b) from pg_stat_bgwriter b",
	"checkpoint": "select row_to_json(c) from pg_stat_checkpointer c",
	"database":   "select row_to_json(d) from pg_stat_database d where d.datname = current_database()",
	"io":         "select row_to_json(t) from (select coalesce(sum(reads),0) as reads, coalesce(sum(read_bytes),0) as read_bytes, coalesce(sum(writes),0) as writes, coalesce(sum(write_bytes),0) as write_bytes, coalesce(sum(fsyncs),0) as fsyncs from pg_stat_io) t",
}

// Snapshot reads every view in one psql invocation per view. A view a
// server version lacks is skipped silently, the delta then skips it
// too.
func Snapshot(psql string, connArgs []string) map[string]map[string]any {
	out := map[string]map[string]any{}
	for name, query := range views {
		args := append([]string{"-X", "-A", "-t", "-c", query}, connArgs...)
		raw, err := exec.Command(psql, args...).Output()
		if err != nil {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(string(raw))), &row); err != nil {
			continue
		}
		out[name] = row
	}
	return out
}

// Version asks the server what it is.
func Version(psql string, connArgs []string) string {
	args := append([]string{"-X", "-A", "-t", "-c", "select version()"}, connArgs...)
	raw, err := exec.Command(psql, args...).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}

// Settings returns every non default server setting, because a result
// without the server's tuning is not comparable to anything.
func Settings(psql string, connArgs []string) map[string]any {
	q := "select coalesce(json_object_agg(name, setting), '{}'::json) from pg_settings where source not in ('default', 'override')"
	args := append([]string{"-X", "-A", "-t", "-c", q}, connArgs...)
	raw, err := exec.Command(psql, args...).Output()
	if err != nil {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(raw))), &out); err != nil {
		return nil
	}
	return out
}

// Delta subtracts numeric fields, view by view. Non numeric fields and
// views missing on either side drop out.
func Delta(before, after map[string]map[string]any) map[string]map[string]float64 {
	out := map[string]map[string]float64{}
	for view, b := range before {
		a, ok := after[view]
		if !ok {
			continue
		}
		d := map[string]float64{}
		for k, bv := range b {
			bf, ok1 := bv.(float64)
			af, ok2 := a[k].(float64)
			if ok1 && ok2 {
				d[k] = af - bf
			}
		}
		if len(d) > 0 {
			out[view] = d
		}
	}
	return out
}
