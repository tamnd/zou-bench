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

	cols := []column{
		{"label", func(r map[string]any) string { return str(r["label"]) }},
		{"tps", func(r map[string]any) string { return num(r["tps"]) }},
		{"avg ms", func(r map[string]any) string { return num(r["latency_avg_ms"]) }},
		{"p50", func(r map[string]any) string { return nested(r, "latency_ms", "p50") }},
		{"p95", func(r map[string]any) string { return nested(r, "latency_ms", "p95") }},
		{"p99", func(r map[string]any) string { return nested(r, "latency_ms", "p99") }},
		{"init s", func(r map[string]any) string { return num(r["init_seconds"]) }},
		{"rss peak MB", func(r map[string]any) string { return kbToMB(nested(r, "server", "rss_peak_kb")) }},
		{"cpu s", func(r map[string]any) string { return nested(r, "server", "cpu_s_total") }},
		{"date", func(r map[string]any) string {
			d := str(r["date"])
			if len(d) >= 10 {
				return d[:10]
			}
			return d
		}},
	}

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
