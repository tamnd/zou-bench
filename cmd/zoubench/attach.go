package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/tamnd/zou-bench/envinfo"
	"github.com/tamnd/zou-bench/pgwire"
	"github.com/tamnd/zou-bench/scenario"
	"github.com/tamnd/zou-bench/zoustats"
)

// cmdAttach measures cold attach: spawn the postmaster, poll the
// socket until one real query answers, write down every phase, stop,
// repeat. pg_ctl -w is not used because its readiness poll ticks at
// 100 ms, on the same order as the budget under test, so the process
// is spawned directly and readiness is our own millisecond poll over
// the wire protocol.
func cmdAttach(argv []string) {
	fs := flag.NewFlagSet("attach", flag.ExitOnError)
	pgbin := fs.String("pgbin", "", "directory holding the postgres binary")
	datadir := fs.String("datadir", "", "data directory to attach")
	label := fs.String("label", "", "zou-minio, pg18, ...")
	sockdir := fs.String("sockdir", "/tmp", "unix socket directory")
	dbname := fs.String("dbname", "postgres", "database the timed query runs in")
	pguser := fs.String("user", "", "role for the timed query, default the OS user")
	statsfile := fs.String("zoustats", "", "zou store op counter file, what ZOU_STORE_STATS pointed at")
	outdir := fs.String("outdir", "results", "result directory")
	var rest []string
	for _, a := range argv {
		if !strings.HasPrefix(a, "-") && strings.HasSuffix(a, ".json") {
			rest = append(rest, a)
		}
	}
	fs.Parse(without(argv, rest))
	if len(rest) != 1 || *pgbin == "" || *datadir == "" || *label == "" {
		usage()
	}
	sc, doc, err := scenario.Load(rest[0])
	die(err)
	if !sc.IsAttach() {
		die(fmt.Errorf("%s is not an attach scenario", rest[0]))
	}
	if *pguser == "" {
		u, err := user.Current()
		die(err)
		*pguser = u.Username
	}

	result := map[string]any{
		"scenario": sc.Name,
		"label":    *label,
		"date":     time.Now().UTC().Format("2006-01-02T15:04:05Z07:00"),
		"config":   doc,
		"env":      envinfo.Capture(),
	}
	if v := os.Getenv("ZOU_STORE_SIM"); v != "" {
		result["simulated"] = v
	} else if v := os.Getenv("ZOU_STORE_DELAY"); v != "" {
		result["simulated"] = "delay:" + v
	}
	var opsBefore zoustats.Counters
	if *statsfile != "" {
		opsBefore, err = zoustats.Read(*statsfile)
		die(err)
	}

	addr := filepath.Join(*sockdir, ".s.PGSQL."+strconv.Itoa(sc.Port))
	postgres := filepath.Join(*pgbin, "postgres")
	var cycles []map[string]any
	var firstRows, connects []float64
	for cycle := 0; cycle < sc.Cycles; cycle++ {
		t0 := time.Now()
		var log strings.Builder
		server := exec.Command(postgres, "-D", *datadir, "-p", strconv.Itoa(sc.Port), "-k", *sockdir)
		server.Stdout, server.Stderr = &log, &log
		die(server.Start())

		conn, err := pgwire.WaitReady(addr, *pguser, *dbname, t0.Add(60*time.Second))
		if err != nil {
			server.Process.Kill()
			server.Wait()
			fmt.Fprint(os.Stderr, log.String())
			die(fmt.Errorf("cycle %d never became ready: %w", cycle, err))
		}
		connectMS := ms(time.Since(t0))
		if _, err := conn.Query(sc.Query); err != nil {
			conn.Close()
			server.Process.Kill()
			server.Wait()
			die(fmt.Errorf("cycle %d query: %w", cycle, err))
		}
		firstRowMS := ms(time.Since(t0))
		conn.Close()

		// SIGINT is the fast shutdown pg_ctl -m fast sends.
		tStop := time.Now()
		die(server.Process.Signal(syscall.SIGINT))
		die(server.Wait())
		stopMS := ms(time.Since(tStop))

		cycles = append(cycles, map[string]any{
			"connect_ms":   connectMS,
			"first_row_ms": firstRowMS,
			"stop_ms":      stopMS,
		})
		connects = append(connects, connectMS)
		firstRows = append(firstRows, firstRowMS)
	}

	result["cycles"] = cycles
	result["attach_ms"] = map[string]any{
		"p50":         quantile(firstRows, 0.50),
		"p95":         quantile(firstRows, 0.95),
		"max":         quantile(firstRows, 1.0),
		"connect_p50": quantile(connects, 0.50),
	}
	if *statsfile != "" {
		opsAfter, err := zoustats.Read(*statsfile)
		die(err)
		delta, err := zoustats.Diff(opsBefore, opsAfter)
		die(err)
		result["store_ops"] = zoustats.Report(delta)
	}

	die(os.MkdirAll(*outdir, 0o755))
	stamp := strings.NewReplacer(":", "", "-", "").Replace(result["date"].(string))[:15]
	path := filepath.Join(*outdir, fmt.Sprintf("%s-%s-%s.json", sc.Name, *label, stamp))
	out, err := json.MarshalIndent(result, "", "  ")
	die(err)
	die(os.WriteFile(path, out, 0o644))
	fmt.Println(path)
	brief, _ := json.MarshalIndent(result["attach_ms"], "", "  ")
	fmt.Println(string(brief))
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}

// quantile interpolates nothing: it returns the smallest measured
// value at or above the asked rank, which for cycle counts this small
// is the only honest read.
func quantile(vals []float64, q float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sorted := append([]float64(nil), vals...)
	sort.Float64s(sorted)
	idx := int(float64(len(sorted)-1) * q)
	return sorted[idx]
}
