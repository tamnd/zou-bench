package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/tamnd/zou-bench/envinfo"
	"github.com/tamnd/zou-bench/latency"
	"github.com/tamnd/zou-bench/pgbench"
	"github.com/tamnd/zou-bench/pgstats"
	"github.com/tamnd/zou-bench/resthttp"
	"github.com/tamnd/zou-bench/sampler"
	"github.com/tamnd/zou-bench/scenario"
	"github.com/tamnd/zou-bench/zoustats"
)

// cmdREST measures the http surface rather than the database under it,
// which is what the warm read target is written about. Everything the
// pgbench runner captures around the run is captured here too, because
// the interesting question about a rest number is usually what the
// server was doing while it was answering.
func cmdREST(argv []string) {
	fs := flag.NewFlagSet("rest", flag.ExitOnError)
	url := fs.String("url", "", "base url of the api, e.g. http://127.0.0.1:54321")
	label := fs.String("label", "", "zou-localfs, postgrest-pg18, ...")
	secret := fs.String("jwt-secret", "", "project jwt secret, the tokens are minted from it")
	anonKey := fs.String("anon-key", "", "anon api key, when it is not minted from the secret")
	userSub := fs.String("user", "11111111-1111-1111-1111-111111111111", "sub claim of the signed in user")
	dsn := fs.String("dsn", "", "libpq DSN, for the setup file and the server side stats")
	datadir := fs.String("datadir", "", "local server datadir for process sampling")
	statsfile := fs.String("zoustats", "", "zou store op counter file")
	outdir := fs.String("outdir", "results", "result directory")
	simulated := fs.String("simulated", "", "mark the run simulated with this sim spec")
	var rest []string
	for _, a := range argv {
		if !strings.HasPrefix(a, "-") && strings.HasSuffix(a, ".json") {
			rest = append(rest, a)
		}
	}
	fs.Parse(without(argv, rest))
	if len(rest) != 1 || *url == "" || *label == "" {
		usage()
	}

	sc, doc, err := scenario.Load(rest[0])
	die(err)
	if !sc.IsREST() {
		die(fmt.Errorf("%s is not a rest scenario", rest[0]))
	}

	// The workload's own schema and rows. Applying it every run is on
	// purpose: a number measured against whatever happened to be in
	// the database is not a number about this scenario.
	if sc.Setup != "" {
		if *dsn == "" {
			die(fmt.Errorf("%s has a setup file, so the run needs a --dsn to apply it", rest[0]))
		}
		path := filepath.Join(filepath.Dir(rest[0]), sc.Setup)
		cmd := exec.Command(pgbench.Tool("psql"), append([]string{"-v", "ON_ERROR_STOP=1", "-q", "-f", path}, pgbench.DSNArgs(*dsn)...)...)
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		die(cmd.Run())
	}

	tokens := map[string]string{}
	key := *anonKey
	if *secret != "" {
		tokens["anon"] = resthttp.KeyToken(*secret, "anon")
		tokens["service"] = resthttp.KeyToken(*secret, "service_role")
		tokens["user"] = resthttp.UserToken(*secret, *userSub, "bench@example.com")
		if key == "" {
			key = tokens["anon"]
		}
	}

	result := map[string]any{
		"scenario": sc.Name,
		"label":    *label,
		"date":     time.Now().UTC().Format("2006-01-02T15:04:05Z07:00"),
		"config":   doc,
		"env":      envinfo.Capture(),
		"base_url": *url,
	}
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

	var conn []string
	if *dsn != "" {
		conn = pgbench.DSNArgs(*dsn)
		psql := pgbench.Tool("psql")
		if v := pgstats.Version(psql, conn); v != "" {
			result["server_version"] = v
		}
		if s := pgstats.Settings(psql, conn); s != nil {
			result["server_settings"] = s
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

	var opsBefore zoustats.Counters
	if *statsfile != "" {
		opsBefore, err = zoustats.Read(*statsfile)
		die(err)
	}

	var before map[string]map[string]any
	if conn != nil {
		before = pgstats.Snapshot(pgbench.Tool("psql"), conn)
	}

	out, err := resthttp.Run(context.Background(), resthttp.Options{
		BaseURL:  strings.TrimSuffix(*url, "/"),
		Clients:  sc.Clients,
		Duration: time.Duration(sc.Duration) * time.Second,
		Warmup:   time.Duration(sc.Warmup) * time.Second,
		Rate:     sc.Rate,
		Requests: sc.Requests,
		Tokens:   tokens,
		APIKey:   key,
	})
	die(err)

	if conn != nil {
		after := pgstats.Snapshot(pgbench.Tool("psql"), conn)
		if d := pgstats.Delta(before, after); len(d) > 0 {
			result["pg_delta"] = d
		}
	}

	// tps and latency_avg_ms carry the pgbench names on purpose, so a
	// rest run and a pgbench run land in the same report table and
	// nobody has to remember which column a row came from.
	secs := out.Elapsed.Seconds()
	result["tps"] = round1(float64(len(out.Samples)) / secs)
	result["requests"] = out.Requests
	result["errors"] = out.Errors
	result["status"] = out.Status
	result["bytes"] = out.Bytes
	result["latency_ms"] = latency.Percentiles(out.Samples)
	result["latency_avg_ms"] = latency.Percentiles(out.Samples)["mean"]
	result["buckets_30s"] = latency.Buckets(out.Samples, 30)
	result["per_request"] = out.PerRequest
	if len(out.Failures) > 0 {
		result["failures"] = out.Failures
	}

	if tree != nil {
		result["server"] = tree.Finish()
	}
	result["system"] = sys.Finish()

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
	blob, err := json.MarshalIndent(result, "", "  ")
	die(err)
	die(os.WriteFile(path, blob, 0o644))
	fmt.Println(path)

	// A run that answered nothing but errors has to say so where
	// somebody reading the terminal will see it, not only in the file.
	if out.Errors > 0 {
		fmt.Fprintf(os.Stderr, "zoubench: %d of %d requests were not what the workload expected: %s\n",
			out.Errors, out.Requests, strings.Join(out.Failures, "; "))
	}
	brief := map[string]any{}
	for k, v := range result {
		switch k {
		case "config", "server_settings", "env", "buckets_30s", "system":
		default:
			brief[k] = v
		}
	}
	pretty, _ := json.MarshalIndent(brief, "", "  ")
	fmt.Println(string(pretty))
}
