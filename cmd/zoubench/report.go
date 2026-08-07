package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

type column struct {
	name string
	get  func(map[string]any) string
}

func cmdReport(argv []string) {
	if len(argv) == 0 {
		usage()
	}
	var rows []map[string]any
	for _, path := range argv {
		raw, err := os.ReadFile(path)
		die(err)
		var row map[string]any
		die(json.Unmarshal(raw, &row))
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i]["scenario"] != rows[j]["scenario"] {
			return str(rows[i]["scenario"]) < str(rows[j]["scenario"])
		}
		return str(rows[i]["label"]) < str(rows[j]["label"])
	})

	// Sustain results answer different questions (recovery, drift,
	// invariants) than a throughput run, so they get their own table
	// instead of a row of mostly empty cells in the shared one.
	var bench, soak []map[string]any
	for _, r := range rows {
		if c, ok := r["config"].(map[string]any); ok && str(c["kind"]) == "sustain" {
			soak = append(soak, r)
		} else {
			bench = append(bench, r)
		}
	}

	cols := []column{
		// A simulated run wears the marker in every table it appears
		// in, per the M1b rules: simulated numbers hold a place until
		// a real bucket run replaces them, they never pass as real.
		simLabelCol(),
		{"tps", func(r map[string]any) string { return num(r["tps"]) }},
		{"avg ms", func(r map[string]any) string { return num(r["latency_avg_ms"]) }},
		{"p50", func(r map[string]any) string { return nested(r, "latency_ms", "p50") }},
		{"p95", func(r map[string]any) string { return nested(r, "latency_ms", "p95") }},
		{"p99", func(r map[string]any) string { return nested(r, "latency_ms", "p99") }},
		{"p999", func(r map[string]any) string { return nested(r, "latency_ms", "p999") }},
		{"init s", func(r map[string]any) string { return num(r["init_seconds"]) }},
		{"rss peak MB", func(r map[string]any) string { return kbToMB(nested(r, "server", "rss_peak_kb")) }},
		{"cpu s", func(r map[string]any) string { return nested(r, "server", "cpu_s_total") }},
		{"wal MB", func(r map[string]any) string { return bytesToMB(deep(r, "pg_delta", "wal", "wal_bytes")) }},
		{"store +MB", func(r map[string]any) string { return bytesToMB(nested(r, "store", "bytes_delta")) }},
		{"w-amp", func(r map[string]any) string { return nested(r, "store", "write_amplification") }},
		{"$/M txns", func(r map[string]any) string { return nested(r, "cost", "usd_per_million_txns") }},
		dateCol(),
	}

	printTables(bench, cols)

	sustainCols := []column{
		simLabelCol(),
		{"hours", func(r map[string]any) string { return num(r["hours"]) }},
		{"segs", func(r map[string]any) string { return alen(r["segments"]) }},
		{"drills", func(r map[string]any) string { return alen(r["drills"]) }},
		{"rto pusher p50/max", func(r map[string]any) string { return rtoCell(r, "pusher") }},
		{"rto crash p50/max", func(r map[string]any) string { return rtoCell(r, "crash") }},
		{"rto death p50/max", func(r map[string]any) string { return rtoCell(r, "death") }},
		// The magnitude lives in the segments, the top level only says
		// whether the bound held, so the worst sample is dug out here.
		{"amp max", func(r map[string]any) string { return segMax(r, "amp_timeline", "amp_max") }},
		{"amp ok", func(r map[string]any) string { return boolCell(r["amp_bound_held"]) }},
		{"viol", func(r map[string]any) string { return num(r["violations"]) }},
		{"tps mean", func(r map[string]any) string { return num(r["tps_mean"]) }},
		// The worst segment's slope, because a leak is a slope that
		// stays positive and an average across segments would let one
		// good segment launder a bad one.
		{"rss slope kb/min", func(r map[string]any) string { return segMax(r, "", "rss_slope_kb_per_min") }},
		dateCol(),
	}

	printTables(soak, sustainCols)
}

func printTables(rows []map[string]any, cols []column) {
	scenario := ""
	for _, r := range rows {
		if s := str(r["scenario"]); s != scenario {
			scenario = s
			fmt.Printf("\n## %s\n\n", scenario)
			fmt.Print("|")
			for _, c := range cols {
				fmt.Printf(" %s |", c.name)
			}
			fmt.Print("\n|")
			for range cols {
				fmt.Print("---|")
			}
			fmt.Println()
		}
		fmt.Print("|")
		for _, c := range cols {
			fmt.Printf(" %s |", c.get(r))
		}
		fmt.Println()
	}
}

func simLabelCol() column {
	return column{"label", func(r map[string]any) string {
		l := str(r["label"])
		if r["simulated"] != nil {
			l += " (sim)"
		}
		return l
	}}
}

func dateCol() column {
	return column{"date", func(r map[string]any) string {
		d := str(r["date"])
		if len(d) >= 10 {
			return d[:10]
		}
		return d
	}}
}

// alen renders the length of a json array cell, empty when absent so a
// run that recorded nothing does not read as a run with zero of them.
func alen(v any) string {
	if a, ok := v.([]any); ok {
		return fmt.Sprintf("%d", len(a))
	}
	return ""
}

// rtoCell renders one drill mode's recovery summary as p50/max, the
// two ends a recovery target is usually written in.
func rtoCell(r map[string]any, mode string) string {
	m, ok := r["rto_ms"].(map[string]any)
	if !ok {
		return ""
	}
	mm, ok := m[mode].(map[string]any)
	if !ok {
		return ""
	}
	p50, mx := num(mm["p50"]), num(mm["max"])
	if p50 == "" && mx == "" {
		return ""
	}
	return p50 + "/" + mx
}

func boolCell(v any) string {
	if b, ok := v.(bool); ok {
		if b {
			return "yes"
		}
		return "no"
	}
	return ""
}

// segMax digs the maximum of a per segment value out of a sustain
// result. With list empty it reads the key off each segment, with a
// list name it reads the key off each entry of that list per segment.
func segMax(r map[string]any, list, key string) string {
	segs, ok := r["segments"].([]any)
	if !ok {
		return ""
	}
	best, found := 0.0, false
	take := func(m map[string]any) {
		if f, ok := m[key].(float64); ok {
			if !found || f > best {
				best, found = f, true
			}
		}
	}
	for _, s := range segs {
		seg, ok := s.(map[string]any)
		if !ok {
			continue
		}
		if list == "" {
			take(seg)
			continue
		}
		entries, ok := seg[list].([]any)
		if !ok {
			continue
		}
		for _, e := range entries {
			if m, ok := e.(map[string]any); ok {
				take(m)
			}
		}
	}
	if !found {
		return ""
	}
	return num(best)
}

func str(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func num(v any) string {
	if f, ok := v.(float64); ok {
		if f == float64(int64(f)) {
			return fmt.Sprintf("%d", int64(f))
		}
		return fmt.Sprintf("%g", f)
	}
	return ""
}

func nested(r map[string]any, outer, inner string) string {
	if m, ok := r[outer].(map[string]any); ok {
		return num(m[inner])
	}
	return ""
}

func kbToMB(kb string) string {
	var f float64
	if _, err := fmt.Sscanf(kb, "%g", &f); err != nil || f == 0 {
		return ""
	}
	return fmt.Sprintf("%d", int(f/1024+0.5))
}

func bytesToMB(b string) string {
	var f float64
	if _, err := fmt.Sscanf(b, "%g", &f); err != nil || f == 0 {
		return ""
	}
	return fmt.Sprintf("%d", int(f/(1024*1024)+0.5))
}

// deep digs two levels below a top level key, for pg_delta style maps.
func deep(r map[string]any, outer, mid, inner string) string {
	m, ok := r[outer].(map[string]any)
	if !ok {
		return ""
	}
	return nested(m, mid, inner)
}
