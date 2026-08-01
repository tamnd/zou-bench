package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/tamnd/zou-bench/cost"
	"github.com/tamnd/zou-bench/envinfo"
	"github.com/tamnd/zou-bench/pgbench"
	"github.com/tamnd/zou-bench/pgstats"
	"github.com/tamnd/zou-bench/sampler"
	"github.com/tamnd/zou-bench/scenario"
	"github.com/tamnd/zou-bench/storefs"
	"github.com/tamnd/zou-bench/zoustats"
)

func cmdRun(argv []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	dsn := fs.String("dsn", "", "key=value libpq DSN")
	label := fs.String("label", "", "pg18, zou-minio, neon, ...")
	datadir := fs.String("datadir", "", "local server datadir for process sampling")
	storedir := fs.String("storedir", "", "local store path for footprint and amplification")
	statsfile := fs.String("zoustats", "", "zou store op counter file, what ZOU_STORE_STATS pointed at")
	pricecard := fs.String("pricecard", "", "price card name from pricecards/ for a cost block")
	cardsdir := fs.String("cardsdir", "pricecards", "price card directory")
	simulated := fs.String("simulated", "", "mark the run simulated with this sim spec, defaults to ZOU_STORE_SIM or ZOU_STORE_DELAY when either is set")
	outdir := fs.String("outdir", "results", "result directory")
	// Accept the scenario path before or after the flags.
	var rest []string
	for _, a := range argv {
		if !strings.HasPrefix(a, "-") && strings.HasSuffix(a, ".json") && !strings.Contains(a, string(filepath.Separator)+"pricecards"+string(filepath.Separator)) {
			rest = append(rest, a)
		}
	}
	fs.Parse(without(argv, rest))
	if len(rest) != 1 || *dsn == "" || *label == "" {
		usage()
	}

	sc, doc, err := scenario.Load(rest[0])
	die(err)

	conn := pgbench.DSNArgs(*dsn)
	psql := pgbench.Tool("psql")
	result := map[string]any{
		"scenario": sc.Name,
		"label":    *label,
		"date":     time.Now().UTC().Format("2006-01-02T15:04:05Z07:00"),
		"config":   doc,
		"env":      envinfo.Capture(),
	}
	// A run under simulated store behavior is stamped so its numbers
	// can never pass as real. The env fallback covers the usual case
	// where the harness and the server share a shell, the flag covers
	// a server started elsewhere with sim vars this process cannot see.
	simSpec := *simulated
	if simSpec == "" {
		if v := os.Getenv("ZOU_STORE_SIM"); v != "" {
			simSpec = v
		} else if v := os.Getenv("ZOU_STORE_DELAY"); v != "" {
			simSpec = "delay:" + v
		}
	}
	if simSpec != "" {
		result["simulated"] = simSpec
	}
	if v := pgstats.Version(psql, conn); v != "" {
		result["server_version"] = v
	}
	if s := pgstats.Settings(psql, conn); s != nil {
		result["server_settings"] = s
	}
	if *datadir != "" {
		if fsinfo := envinfo.Filesystem(*datadir); fsinfo != nil {
			result["datadir_fs"] = fsinfo
		}
	}
	if *storedir != "" {
		if fsinfo := envinfo.Filesystem(*storedir); fsinfo != nil {
			result["store_fs"] = fsinfo
		}
	}

	var tree *sampler.Tree
	if *datadir != "" {
		pid, err := serverPid(*datadir)
		die(err)
		tree = sampler.NewTree(pid)
		tree.Start()
	}
	sys := sampler.NewSystem()
	sys.Start()

	var storeBefore storefs.Footprint
	if *storedir != "" {
		storeBefore, err = storefs.Measure(*storedir)
		die(err)
	}

	// Store op counters accumulate for the life of one zou boot, so
	// the run's own ops are the difference between here and the end.
	// Init counts on purpose: loading the data is part of what the
	// run cost.
	var opsBefore zoustats.Counters
	if *statsfile != "" {
		opsBefore, err = zoustats.Read(*statsfile)
		die(err)
	}

	if sc.Init {
		t0 := time.Now()
		cmd := exec.Command(pgbench.Tool("pgbench"), append([]string{"-i", "-q", "-s", strconv.Itoa(sc.Scale)}, conn...)...)
		var initOut strings.Builder
		cmd.Stdout, cmd.Stderr = &initOut, &initOut
		err := cmd.Run()
		fmt.Fprint(os.Stderr, initOut.String())
		die(err)
		result["init_seconds"] = round1(time.Since(t0).Seconds())
		if total, phases := pgbench.ParseInitPhases(initOut.String()); total > 0 {
			result["init_phases_s"] = phases
		}
	}

	if sc.Warmup > 0 {
		warm := exec.Command(pgbench.Tool("pgbench"), append(benchArgs(sc, sc.Warmup, "", rest[0]), conn...)...)
		warm.Stdout, warm.Stderr = os.Stderr, os.Stderr
		die(warm.Run())
	}

	before := pgstats.Snapshot(psql, conn)

	logdir, err := os.MkdirTemp("", "zoubench")
	die(err)
	defer os.RemoveAll(logdir)

	args := benchArgs(sc, sc.Duration, logdir, rest[0])
	cmd := exec.Command(pgbench.Tool("pgbench"), append(args, conn...)...)
	var stdout strings.Builder
	cmd.Stdout, cmd.Stderr = &stdout, os.Stderr
	die(cmd.Run())

	after := pgstats.Snapshot(psql, conn)

	pgbench.ParseSummary(stdout.String(), result)
	txns := pgbench.ParseTxnLogs(logdir)
	result["latency_ms"] = pgbench.Percentiles(txns)
	result["buckets_30s"] = pgbench.Buckets(txns, 30)
	result["raw_summary"] = stdout.String()
	if d := pgstats.Delta(before, after); len(d) > 0 {
		result["pg_delta"] = d
	}

	if tree != nil {
		result["server"] = tree.Finish()
	}
	result["system"] = sys.Finish()

	usage := cost.Usage{}
	if *storedir != "" {
		storeAfter, err := storefs.Measure(*storedir)
		die(err)
		usage.StorageBytes = storeAfter.Bytes
		store := map[string]any{
			"path":          *storedir,
			"bytes_before":  storeBefore.Bytes,
			"bytes_after":   storeAfter.Bytes,
			"bytes_delta":   storeAfter.Bytes - storeBefore.Bytes,
			"objects_after": storeAfter.Objects,
		}
		// Write amplification: bytes the store grew per byte of WAL the
		// workload generated. Checkpoints and manifest churn land here.
		if d, ok := result["pg_delta"].(map[string]map[string]float64); ok {
			if wal, ok := d["wal"]["wal_bytes"]; ok && wal > 0 {
				store["wal_bytes"] = int64(wal)
				store["write_amplification"] = round3(float64(storeAfter.Bytes-storeBefore.Bytes) / wal)
			}
		}
		result["store"] = store
	}

	if *statsfile != "" {
		opsAfter, err := zoustats.Read(*statsfile)
		die(err)
		delta, err := zoustats.Diff(opsBefore, opsAfter)
		die(err)
		result["store_ops"] = zoustats.Report(delta)
		if t := zoustats.Sum(delta); t.AnyTraffic {
			usage.Puts, usage.Gets = t.Puts, t.Gets
			usage.Lists, usage.Deletes = t.Lists, t.Deletes
			usage.EgressBytes = t.GetBytes
			usage.Measured = true
		}
	}

	if n, ok := result["transactions"].(int); ok {
		usage.Txns = int64(n)
	}

	// A simulated run against a provider profile prices itself with
	// that provider's card by default: the op counts are real, only
	// the latency was simulated, so the dollars answer what this run
	// would have cost there. An explicit --pricecard still wins.
	cardName := *pricecard
	if cardName == "" && simSpec != "" {
		cardName = simCard(simSpec)
	}
	if cardName != "" {
		card, err := cost.Find(*cardsdir, cardName)
		die(err)
		result["cost"] = cost.Compute(card, usage)
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
		switch k {
		case "raw_summary", "config", "server_settings", "env", "buckets_30s":
		default:
			brief[k] = v
		}
	}
	pretty, _ := json.MarshalIndent(brief, "", "  ")
	fmt.Println(string(pretty))
}

// simCard maps a ZOU_STORE_SIM profile name to the price card that
// matches it, so a simulated run picks the right dollars without a
// flag. Specs with overrides map by their head, calibration file paths
// and unknown profiles map to nothing.
func simCard(spec string) string {
	head, _, _ := strings.Cut(spec, ",")
	switch head {
	case "s3-standard":
		return "aws-s3-standard"
	case "s3-express":
		return "aws-s3-express-one-zone"
	case "r2":
		return "cloudflare-r2"
	case "gcs":
		return "gcs-standard"
	case "b2":
		return "backblaze-b2"
	case "wasabi":
		return "wasabi"
	}
	return ""
}

// benchArgs builds the pgbench invocation for a window of seconds.
// logdir empty means no per transaction log, which is how warmup runs.
func benchArgs(sc scenario.Scenario, seconds int, logdir, scenarioPath string) []string {
	args := []string{
		"-c", strconv.Itoa(sc.Clients),
		"-j", strconv.Itoa(sc.Threads),
		"-T", strconv.Itoa(seconds),
		"-r",
	}
	if logdir != "" {
		args = append(args, "-l", "--log-prefix", filepath.Join(logdir, "pgbench_log"))
	}
	if sc.Rate > 0 {
		args = append(args, "-R", strconv.Itoa(sc.Rate))
	}
	if sc.Script != "" {
		args = append(args, "-f", filepath.Join(filepath.Dir(scenarioPath), sc.Script))
	} else if sc.Builtin != "" && sc.Builtin != "tpcb-like" {
		args = append(args, "-b", sc.Builtin)
	}
	return args
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

func round1(v float64) float64 { return float64(int(v*10+0.5)) / 10 }
func round3(v float64) float64 { return float64(int(v*1000+0.5)) / 1000 }

func die(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "zoubench:", err)
		os.Exit(1)
	}
}
