package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type scenario struct {
	Name     string `json:"name"`
	Init     bool   `json:"init"`
	Scale    int    `json:"scale"`
	Clients  int    `json:"clients"`
	Threads  int    `json:"threads"`
	Duration int    `json:"duration"`
	Builtin  string `json:"builtin"`
}

func pgtool(name string) string {
	if bin := os.Getenv("PGBIN"); bin != "" {
		return filepath.Join(bin, name)
	}
	return name
}

// dsnArgs turns a key=value DSN into pgbench connection flags.
func dsnArgs(dsn string) []string {
	kv := map[string]string{}
	for _, part := range strings.Fields(dsn) {
		if k, v, ok := strings.Cut(part, "="); ok {
			kv[k] = v
		}
	}
	var args []string
	if v := kv["host"]; v != "" {
		args = append(args, "-h", v)
	}
	if v := kv["port"]; v != "" {
		args = append(args, "-p", v)
	}
	if v := kv["user"]; v != "" {
		args = append(args, "-U", v)
	}
	db := kv["dbname"]
	if db == "" {
		db = "postgres"
	}
	return append(args, db)
}

func cmdRun(argv []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	dsn := fs.String("dsn", "", "key=value libpq DSN")
	label := fs.String("label", "", "pg18, zou-minio, neon, ...")
	datadir := fs.String("datadir", "", "local server datadir for process sampling")
	outdir := fs.String("outdir", "results", "result directory")
	// Accept the scenario path before or after the flags.
	var rest []string
	for _, a := range argv {
		if !strings.HasPrefix(a, "-") && strings.HasSuffix(a, ".json") {
			rest = append(rest, a)
		}
	}
	fs.Parse(without(argv, rest))
	if len(rest) != 1 || *dsn == "" || *label == "" {
		usage()
	}

	raw, err := os.ReadFile(rest[0])
	die(err)
	var sc scenario
	die(json.Unmarshal(raw, &sc))
	if sc.Clients == 0 {
		sc.Clients = 8
	}
	if sc.Threads == 0 {
		sc.Threads = sc.Clients
	}
	if sc.Duration == 0 {
		sc.Duration = 60
	}
	var config map[string]any
	die(json.Unmarshal(raw, &config))

	conn := dsnArgs(*dsn)
	host, _ := os.Hostname()
	result := map[string]any{
		"scenario": sc.Name,
		"label":    *label,
		"date":     time.Now().UTC().Format("2006-01-02T15:04:05Z07:00"),
		"host":     host,
		"config":   config,
	}

	var sampler *treeSampler
	if *datadir != "" {
		pid, err := serverPid(*datadir)
		die(err)
		sampler = newTreeSampler(pid)
		sampler.start()
	}

	if sc.Init {
		t0 := time.Now()
		cmd := exec.Command(pgtool("pgbench"), append([]string{"-i", "-q", "-s", strconv.Itoa(sc.Scale)}, conn...)...)
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		die(cmd.Run())
		result["init_seconds"] = round1(time.Since(t0).Seconds())
	}

	logdir, err := os.MkdirTemp("", "zoubench")
	die(err)
	defer os.RemoveAll(logdir)

	args := []string{
		"-c", strconv.Itoa(sc.Clients),
		"-j", strconv.Itoa(sc.Threads),
		"-T", strconv.Itoa(sc.Duration),
		"-r",
		"-l", "--log-prefix", filepath.Join(logdir, "pgbench_log"),
	}
	if sc.Builtin != "" && sc.Builtin != "tpcb-like" {
		args = append(args, "-b", sc.Builtin)
	}
	args = append(args, conn...)
	cmd := exec.Command(pgtool("pgbench"), args...)
	var stdout strings.Builder
	cmd.Stdout, cmd.Stderr = &stdout, os.Stderr
	die(cmd.Run())

	parseSummary(stdout.String(), result)
	result["latency_ms"] = percentiles(parseTxnLogs(logdir))
	result["raw_summary"] = stdout.String()

	if sampler != nil {
		result["server"] = sampler.finish()
	}

	die(os.MkdirAll(*outdir, 0o755))
	stamp := strings.NewReplacer(":", "", "-", "").Replace(result["date"].(string))[:15]
	path := filepath.Join(*outdir, fmt.Sprintf("%s-%s-%s.json", sc.Name, *label, stamp))
	out, err := json.MarshalIndent(result, "", "  ")
	die(err)
	die(os.WriteFile(path, out, 0o644))
	fmt.Println(path)

	brief := map[string]any{}
	for k, v := range result {
		if k != "raw_summary" && k != "config" {
			brief[k] = v
		}
	}
	pretty, _ := json.MarshalIndent(brief, "", "  ")
	fmt.Println(string(pretty))
}

// without returns argv with the given positional entries removed, so the
// flag package only sees flags.
func without(argv, drop []string) []string {
	var out []string
	for _, a := range argv {
		keep := true
		for _, d := range drop {
			if a == d {
				keep = false
			}
		}
		if keep {
			out = append(out, a)
		}
	}
	return out
}

func serverPid(datadir string) (int, error) {
	raw, err := os.ReadFile(filepath.Join(datadir, "postmaster.pid"))
	if err != nil {
		return 0, err
	}
	first, _, _ := strings.Cut(string(raw), "\n")
	return strconv.Atoi(strings.TrimSpace(first))
}

var summaryPatterns = []struct {
	re  *regexp.Regexp
	key string
	num bool
}{
	{regexp.MustCompile(`(?m)^tps = ([0-9.]+)`), "tps", false},
	{regexp.MustCompile(`latency average = ([0-9.]+) ms`), "latency_avg_ms", false},
	{regexp.MustCompile(`initial connection time = ([0-9.]+) ms`), "connect_ms", false},
	{regexp.MustCompile(`number of transactions actually processed: (\d+)`), "transactions", true},
	{regexp.MustCompile(`number of failed transactions: (\d+)`), "failed", true},
}

var stmtRe = regexp.MustCompile(`^\s+([0-9.]+)\s+\d+\s+(.+)`)

func parseSummary(text string, result map[string]any) {
	for _, p := range summaryPatterns {
		if m := p.re.FindStringSubmatch(text); m != nil {
			if p.num {
				n, _ := strconv.Atoi(m[1])
				result[p.key] = n
			} else {
				f, _ := strconv.ParseFloat(m[1], 64)
				result[p.key] = f
			}
		}
	}
	stmts := map[string]float64{}
	inStmts := false
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, "statement latencies") {
			inStmts = true
			continue
		}
		if !inStmts {
			continue
		}
		if m := stmtRe.FindStringSubmatch(line); m != nil {
			f, _ := strconv.ParseFloat(m[1], 64)
			stmt := strings.TrimSpace(m[2])
			if len(stmt) > 60 {
				stmt = stmt[:60]
			}
			stmts[stmt] = f
		}
	}
	if len(stmts) > 0 {
		result["statement_latency_ms"] = stmts
	}
}

// parseTxnLogs reads pgbench -l files, one line per transaction with
// the latency in microseconds as the third field.
func parseTxnLogs(logdir string) []float64 {
	var latencies []float64
	entries, _ := os.ReadDir(logdir)
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "pgbench_log") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(logdir, e.Name()))
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(raw), "\n") {
			parts := strings.Fields(line)
			if len(parts) < 3 {
				continue
			}
			if us, err := strconv.Atoi(parts[2]); err == nil {
				latencies = append(latencies, float64(us)/1000.0)
			}
		}
	}
	return latencies
}

func percentiles(values []float64) map[string]float64 {
	out := map[string]float64{}
	if len(values) == 0 {
		return out
	}
	sort.Float64s(values)
	for _, p := range []int{50, 95, 99} {
		idx := len(values) * p / 100
		if idx >= len(values) {
			idx = len(values) - 1
		}
		out[fmt.Sprintf("p%d", p)] = round3(values[idx])
	}
	out["max"] = round3(values[len(values)-1])
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	out["mean"] = round3(sum / float64(len(values)))
	return out
}

func round1(v float64) float64 { return float64(int(v*10+0.5)) / 10 }
func round3(v float64) float64 { return float64(int(v*1000+0.5)) / 1000 }

func die(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "zoubench:", err)
		os.Exit(1)
	}
}
