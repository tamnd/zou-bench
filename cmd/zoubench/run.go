package main

import (
	"archive/tar"
	"compress/gzip"
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
	"github.com/tamnd/zou-bench/tpcb"
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

	// Three marks rather than two, because a run that loads its own
	// data is two workloads and they answer different questions.
	// This is the first: before anything is loaded.
	//
	// Store op counters accumulate for the life of one zou boot, so
	// what a phase did is the difference between two reads of the
	// file, and the phases have to be cut where the timed window is
	// cut. They were not: the store was measured here and the wal
	// bytes were taken after the load, so every init scenario divided
	// the whole of loading scale 100 by sixty seconds of transactions
	// and published a write amplification in the hundreds and a
	// dollars per million transactions that was mostly the load.
	var storeStart, storeBefore storefs.Footprint
	if *storedir != "" {
		storeStart, err = storefs.Measure(*storedir)
		die(err)
	}
	var opsStart, opsBefore zoustats.Counters
	if *statsfile != "" {
		opsStart, err = zoustats.Read(*statsfile)
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

	wireAddr, wireUser, wireDB := pgbench.DSNParts(*dsn)
	if sc.Warmup > 0 {
		if sc.Driver == "wire" {
			// The warmup has to be the workload it is warming, or the
			// caches it fills are the wrong ones.
			_, _, err := tpcb.Run(wireAddr, wireUser, wireDB, sc.Scale, sc.Clients,
				time.Duration(sc.Warmup)*time.Second)
			die(err)
		} else {
			warm := exec.Command(pgbench.Tool("pgbench"), append(benchArgs(sc, sc.Warmup, "", rest[0]), conn...)...)
			warm.Stdout, warm.Stderr = os.Stderr, os.Stderr
			die(warm.Run())
		}
	}

	// The second mark, where the timed window starts. Everything the
	// result publishes as this workload's rate, latency and cost is
	// the difference between here and the end, and it is taken beside
	// the postgres snapshot rather than before the load so the two
	// cover the same seconds.
	if *storedir != "" {
		storeBefore, err = storefs.Measure(*storedir)
		die(err)
	}
	if *statsfile != "" {
		opsBefore, err = zoustats.Read(*statsfile)
		die(err)
	}

	before := pgstats.Snapshot(psql, conn)

	logdir, err := os.MkdirTemp("", "zoubench")
	die(err)
	defer os.RemoveAll(logdir)

	var txns []pgbench.Txn
	if sc.Driver == "wire" {
		window := time.Duration(sc.Duration) * time.Second
		started := time.Now()
		wire, failed, err := tpcb.Run(wireAddr, wireUser, wireDB, sc.Scale, sc.Clients, window)
		die(err)
		elapsed := time.Since(started).Seconds()
		txns = make([]pgbench.Txn, len(wire))
		var sum float64
		for i, t := range wire {
			txns[i] = pgbench.Txn{LatencyMS: t.LatencyMS, Epoch: t.Epoch}
			sum += t.LatencyMS
		}
		// The same keys pgbench's summary block fills, computed the same
		// way, so a wire result and a pgbench result of the same
		// scenario sit in one table without a footnote saying which
		// column came from where.
		result["transactions"] = len(wire)
		result["failed"] = failed
		if elapsed > 0 {
			result["tps"] = round3(float64(len(wire)) / elapsed)
		}
		if len(wire) > 0 {
			result["latency_avg_ms"] = round3(sum / float64(len(wire)))
		}
		result["statement_ms"] = tpcb.Percentiles(wire)
		result["driver"] = "wire"
	} else {
		args := benchArgs(sc, sc.Duration, logdir, rest[0])
		cmd := exec.Command(pgbench.Tool("pgbench"), append(args, conn...)...)
		var stdout strings.Builder
		cmd.Stdout, cmd.Stderr = &stdout, os.Stderr
		die(cmd.Run())
		pgbench.ParseSummary(stdout.String(), result)
		txns = pgbench.ParseTxnLogs(logdir)
		result["raw_summary"] = stdout.String()
	}

	after := pgstats.Snapshot(psql, conn)

	result["latency_ms"] = pgbench.Percentiles(txns)
	result["buckets_30s"] = pgbench.Buckets(txns, 30)
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
		// What the run ingested, for the dollars per GB ingested line.
		// The store's growth rather than everything written to it,
		// since a page rewritten ten times was ingested once, and a
		// phase that folded more than it wrote can grow by less than
		// nothing, which is not an ingest of a negative amount.
		if grew := storeAfter.Bytes - storeBefore.Bytes; grew > 0 {
			usage.IngestedBytes = grew
		}
		store := map[string]any{
			"path":          *storedir,
			"bytes_before":  storeBefore.Bytes,
			"bytes_after":   storeAfter.Bytes,
			"bytes_delta":   storeAfter.Bytes - storeBefore.Bytes,
			"objects_after": storeAfter.Objects,
		}
		if sc.Init {
			// Where the store was before the data was loaded, so the
			// line above can be read as this workload's growth without
			// having to know whether the run loaded anything.
			store["bytes_at_start"] = storeStart.Bytes
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
			// Bytes moved, not bytes egressed. The two are the same
			// count and a different price: transfer is charged on top
			// of the request on the cards that do that at all, and
			// egress is charged only when the reader is out on the
			// internet, which in this harness it never is.
			usage.UploadBytes, usage.DownloadBytes = t.PutBytes, t.GetBytes
			usage.Measured = true
		}
	}

	// What loading the data cost, on its own. It is the same store and
	// the same counters over the phase before the timed window, and it
	// is kept apart because it answers a different question: the
	// workload lines say what a second of this traffic costs, this
	// says what putting a scale 100 database into the store cost once.
	// Folded together they say neither, since a sixty second run that
	// loaded ten million rows is mostly the load.
	loadUsage := cost.Usage{}
	if sc.Init {
		load := map[string]any{"seconds": result["init_seconds"]}
		if *storedir != "" {
			grew := storeBefore.Bytes - storeStart.Bytes
			load["store_bytes"] = grew
			load["store_objects"] = storeBefore.Objects - storeStart.Objects
			loadUsage.StorageBytes = storeBefore.Bytes
			if grew > 0 {
				loadUsage.IngestedBytes = grew
			}
		}
		if *statsfile != "" {
			if d, err := zoustats.Diff(opsStart, opsBefore); err == nil {
				load["store_ops"] = zoustats.Report(d)
				if t := zoustats.Sum(d); t.AnyTraffic {
					loadUsage.Puts, loadUsage.Gets = t.Puts, t.Gets
					loadUsage.Lists, loadUsage.Deletes = t.Lists, t.Deletes
					loadUsage.UploadBytes, loadUsage.DownloadBytes = t.PutBytes, t.GetBytes
					loadUsage.Measured = true
				}
			}
		}
		result["load"] = load
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
		if load, ok := result["load"].(map[string]any); ok {
			load["cost"] = cost.Compute(card, loadUsage)
		}
	}

	die(os.MkdirAll(*outdir, 0o755))
	stamp := strings.NewReplacer(":", "", "-", "").Replace(result["date"].(string))[:15]
	stem := fmt.Sprintf("%s-%s-%s", sc.Name, *label, stamp)
	// The per transaction logs, kept rather than thrown away with the
	// temporary directory they were written into. The percentiles above
	// are computed from them and cannot be recomputed without them, so
	// a published tail whose logs are gone is a number nobody can check
	// and a bucket width nobody can change their mind about. Compressed
	// because they are one line a transaction and compress about ten to
	// one, which is what makes keeping them affordable.
	if logs := filepath.Join(*outdir, stem+".txnlog.tar.gz"); keepLogs(logdir, logs) == nil {
		result["txn_log"] = filepath.Base(logs)
	}
	path := filepath.Join(*outdir, stem+".json")
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
		return "aws-s3-express"
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

// keepLogs archives a directory of pgbench per transaction logs next to
// the result they belong to.
//
// It returns an error rather than dying on one: a run that produced
// real numbers and could not write its logs should still write its
// numbers, and the caller records the archive in the result only when
// there is one. An empty directory, which is what a scenario with the
// transaction log turned off leaves, is not an archive worth writing.
func keepLogs(dir, dest string) error {
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) == 0 {
		if err == nil {
			err = fmt.Errorf("%s: no per transaction logs", dir)
		}
		return err
	}
	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer f.Close()
	zw := gzip.NewWriter(f)
	tw := tar.NewWriter(zw)
	for _, e := range entries {
		info, err := e.Info()
		if err != nil || info.IsDir() {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return err
		}
		hdr := &tar.Header{
			Name:    e.Name(),
			Mode:    0o644,
			Size:    int64(len(body)),
			ModTime: info.ModTime(),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if _, err := tw.Write(body); err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return zw.Close()
}
