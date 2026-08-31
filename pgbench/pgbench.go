// Package pgbench execs pgbench and turns its outputs into numbers:
// the summary block, the -r per statement latencies, the -i init phase
// breakdown, and the -l per transaction log, which is the only honest
// source for percentiles.
package pgbench

import (
	"math"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/tamnd/zou-bench/latency"
)

// Tool returns the path of a postgres client binary, honoring PGBIN so
// the vendored build's tools win over anything on PATH.
func Tool(name string) string {
	if bin := os.Getenv("PGBIN"); bin != "" {
		return filepath.Join(bin, name)
	}
	return name
}

// DSNArgs turns a key=value libpq DSN into pgbench and psql flags.
func DSNArgs(dsn string) []string {
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

// DSNParts turns the same DSN into what a client that speaks the wire
// protocol itself needs: an address to dial, a role, and a database.
// The address is a host:port pair, or the socket path when the DSN
// names a directory rather than a host, which is how libpq reads it too.
// A DSN that names no role gets the operating system account, again the
// way libpq reads it: the baseline scenarios connect over a socket as
// whoever is running them and never spell a user out, and a startup
// packet with no user in it is refused before the first statement.
func DSNParts(dsn string) (addr, user, database string) {
	kv := map[string]string{}
	for _, part := range strings.Fields(dsn) {
		if k, v, ok := strings.Cut(part, "="); ok {
			kv[k] = v
		}
	}
	host, port := kv["host"], kv["port"]
	if port == "" {
		port = "5432"
	}
	if strings.ContainsAny(host, `/\`) {
		addr = filepath.Join(host, ".s.PGSQL."+port)
	} else {
		if host == "" {
			host = "127.0.0.1"
		}
		addr = host + ":" + port
	}
	database = kv["dbname"]
	if database == "" {
		database = "postgres"
	}
	user = kv["user"]
	if user == "" {
		user = osUser()
	}
	return addr, user, database
}

// osUser is the account the harness is running as, which is what libpq
// falls back to. PGUSER first, then the account itself, then the name
// every postgres has a role for, so a lookup that fails on a box
// without a passwd entry still produces a connectable name.
func osUser() string {
	if v := os.Getenv("PGUSER"); v != "" {
		return v
	}
	if u, err := user.Current(); err == nil && u.Username != "" {
		return u.Username
	}
	return "postgres"
}

var summaryPatterns = []struct {
	re  *regexp.Regexp
	key string
	num bool
}{
	{regexp.MustCompile(`(?m)^tps = ([0-9.]+)`), "tps", false},
	{regexp.MustCompile(`latency average = ([0-9.]+) ms`), "latency_avg_ms", false},
	{regexp.MustCompile(`latency stddev = ([0-9.]+) ms`), "latency_stddev_ms", false},
	{regexp.MustCompile(`initial connection time = ([0-9.]+) ms`), "connect_ms", false},
	{regexp.MustCompile(`number of transactions actually processed: (\d+)`), "transactions", true},
	{regexp.MustCompile(`number of failed transactions: (\d+)`), "failed", true},
}

var stmtRe = regexp.MustCompile(`^\s+([0-9.]+)\s+\d+\s+(.+)`)

// ParseSummary extracts the headline numbers and the -r per statement
// table from a pgbench run's stdout into result.
func ParseSummary(text string, result map[string]any) {
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

var initDoneRe = regexp.MustCompile(`done in ([0-9.]+) s \((.+)\)`)
var initPhaseRe = regexp.MustCompile(`([a-z -]+?) ([0-9.]+) s`)

// ParseInitPhases reads the closing line of pgbench -i, which breaks
// the wall time into phases like "client-side generate 200.31 s,
// vacuum 30.01 s, primary keys 70.22 s". The phase split is what makes
// a slow init diagnosable from the result file alone.
func ParseInitPhases(text string) (float64, map[string]float64) {
	m := initDoneRe.FindStringSubmatch(text)
	if m == nil {
		return 0, nil
	}
	total, _ := strconv.ParseFloat(m[1], 64)
	phases := map[string]float64{}
	for _, pm := range initPhaseRe.FindAllStringSubmatch(m[2], -1) {
		f, _ := strconv.ParseFloat(pm[2], 64)
		phases[strings.TrimSpace(pm[1])] = f
	}
	return total, phases
}

// Txn is one line of the pgbench -l per transaction log.
type Txn struct {
	LatencyMS float64
	Epoch     int64
}

// ParseTxnLogs reads every pgbench_log file in dir. Log line format:
// client_id transaction_no time_us script_no epoch_s epoch_us.
func ParseTxnLogs(dir string) []Txn {
	var txns []Txn
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "pgbench_log") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(raw), "\n") {
			parts := strings.Fields(line)
			if len(parts) < 5 {
				continue
			}
			us, err1 := strconv.Atoi(parts[2])
			epoch, err2 := strconv.ParseInt(parts[4], 10, 64)
			if err1 != nil || err2 != nil {
				continue
			}
			txns = append(txns, Txn{LatencyMS: float64(us) / 1000.0, Epoch: epoch})
		}
	}
	return txns
}

// samples turns the transaction log into what the latency package
// works on, so pgbench runs and REST runs are summarized by the same
// code and their tables can be read against each other.
func samples(txns []Txn) []latency.Sample {
	out := make([]latency.Sample, len(txns))
	for i, t := range txns {
		out[i] = latency.Sample{MS: t.LatencyMS, Epoch: t.Epoch}
	}
	return out
}

// Percentiles computes the latency distribution over every transaction.
func Percentiles(txns []Txn) map[string]float64 {
	return latency.Percentiles(samples(txns))
}

// Bucket is one progress window over the run.
type Bucket struct {
	Second int64   `json:"t"`
	Txns   int     `json:"txns"`
	TPS    float64 `json:"tps"`
	P50    float64 `json:"p50_ms"`
	P99    float64 `json:"p99_ms"`
}

// Buckets slices the transaction log into fixed windows so warmup,
// steady state, and any late degradation stay visible. An average over
// the whole run hides all three.
func Buckets(txns []Txn, width int64) []Bucket {
	var out []Bucket
	for _, b := range latency.Buckets(samples(txns), width) {
		out = append(out, Bucket{
			Second: b.Second,
			Txns:   b.Count,
			TPS:    b.Rate,
			P50:    b.P50,
			P99:    b.P99,
		})
	}
	return out
}

func round3(v float64) float64 { return math.Round(v*1000) / 1000 }
