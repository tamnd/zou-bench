package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/tamnd/zou-bench/envinfo"
	"github.com/tamnd/zou-bench/fleet"
	"github.com/tamnd/zou-bench/latency"
	"github.com/tamnd/zou-bench/pgbench"
	"github.com/tamnd/zou-bench/resthttp"
	"github.com/tamnd/zou-bench/sampler"
	"github.com/tamnd/zou-bench/scenario"
	"github.com/tamnd/zou-bench/storefs"
	"github.com/tamnd/zou-bench/zoustats"
)

// cmdFleet measures a node holding many projects rather than one
// project answering fast.
//
// The node is started here, unlike run and rest, because half of what
// is being measured is what the node does when nobody asked it to: the
// attach a request triggers, the eviction at the ceiling, the memory
// the whole tree settles at. None of that is visible from the outside
// of a server somebody else started.
//
// Provisioning is separated from measuring and written down as it goes,
// because a thousand tenants is a thousand initdbs and a phase that
// fails after them must not cost them again.
func cmdFleet(argv []string) {
	fs := flag.NewFlagSet("fleet", flag.ExitOnError)
	zoubin := fs.String("zoubin", "", "path to the zou binary")
	pgbin := fs.String("pgbin", "", "directory holding the patched postgres")
	store := fs.String("store", "", "store target, a directory or an s3 url")
	workdir := fs.String("workdir", "", "runtime directories, logs, and the state file")
	label := fs.String("label", "", "zou-localfs, zou-minio, ...")
	outdir := fs.String("outdir", "results", "result directory")
	phases := fs.String("phases", "provision,steady,churn", "which phases to run")
	jobs := fs.Int("jobs", 8, "tenants provisioned at once")
	cpus := fs.String("cpus", "", "cpu list the node is pinned to, for example 0-7")
	httpPort := fs.Int("http", 54321, "http door")
	pgPort := fs.Int("pg", 54322, "postgres door")
	opsPort := fs.Int("ops", 54323, "ops port, where the node's own numbers are read")
	prefix := fs.String("prefix", "t", "tenant ref prefix")
	keep := fs.Bool("keep-node", false, "leave the node running after the run")
	var rest []string
	for _, a := range argv {
		if !strings.HasPrefix(a, "-") && strings.HasSuffix(a, ".json") {
			rest = append(rest, a)
		}
	}
	fs.Parse(without(argv, rest))
	if len(rest) != 1 || *zoubin == "" || *pgbin == "" || *store == "" || *workdir == "" || *label == "" {
		usage()
	}
	sc, doc, err := scenario.Load(rest[0])
	die(err)
	if !sc.IsFleet() {
		die(fmt.Errorf("%s is not a fleet scenario", rest[0]))
	}
	want := map[string]bool{}
	for _, p := range strings.Split(*phases, ",") {
		want[strings.TrimSpace(p)] = true
	}

	die(os.MkdirAll(*workdir, 0o755))
	runtime := filepath.Join(*workdir, "runtime")
	statsPath := filepath.Join(*workdir, "store-stats")
	logPath := filepath.Join(*workdir, "serve.log")
	statePath := filepath.Join(*workdir, "fleet-state.json")

	// One secret for the whole fleet. Every tenant still has its own
	// entry, its own database and its own prefix, and HS256 costs the
	// same whatever the key is, so this changes nothing a run measures
	// and it keeps token minting off the request path.
	secret := os.Getenv("ZOU_FLEET_SECRET")
	if secret == "" {
		secret = "fleet-bench-secret-at-least-32-characters-long"
	}
	state, err := fleet.LoadState(statePath, *store, secret)
	die(err)
	refs := fleet.Refs(*prefix, sc.Tenants)

	result := map[string]any{
		"scenario": sc.Name,
		"label":    *label,
		"date":     time.Now().UTC().Format("2006-01-02T15:04:05Z07:00"),
		"config":   doc,
		"env":      envinfo.Capture(),
		"node": map[string]any{
			"max_attached":   sc.MaxAttached,
			"idle_secs":      sc.IdleSecs,
			"shared_buffers": sc.SharedBuffers,
			"cpus":           *cpus,
			"tenants":        sc.Tenants,
		},
	}
	if v := os.Getenv("ZOU_STORE_SIM"); v != "" {
		result["simulated"] = v
	} else if v := os.Getenv("ZOU_STORE_DELAY"); v != "" {
		result["simulated"] = "delay:" + v
	}

	node, err := startServe(serveArgs{
		zoubin: *zoubin, pgbin: *pgbin, store: *store, runtime: runtime,
		log: logPath, stats: statsPath, cpus: *cpus,
		http: *httpPort, pg: *pgPort, ops: *opsPort,
		maxAttached: sc.MaxAttached, idle: sc.IdleSecs, sharedBuffers: sc.SharedBuffers,
	})
	die(err)
	stopped := false
	stop := func() {
		if stopped || *keep {
			return
		}
		stopped = true
		node.Process.Signal(syscall.SIGINT)
		node.Wait()
	}
	defer stop()
	base := fmt.Sprintf("http://127.0.0.1:%d", *httpPort)
	opsURL := fmt.Sprintf("http://127.0.0.1:%d/metrics", *opsPort)
	if err := waitHealthy(fmt.Sprintf("http://127.0.0.1:%d/healthz", *opsPort), 60*time.Second); err != nil {
		stop()
		dump(logPath)
		die(err)
	}

	opsBefore, _ := zoustats.Read(statsPath)
	tree := sampler.NewTree(node.Process.Pid)
	tree.Start()
	sys := sampler.NewSystem()
	sys.Start()

	if want["provision"] {
		fmt.Printf("provisioning %d tenants, %d already ready\n", len(refs), state.Count())
		prov, err := provision(provisionArgs{
			zoubin: *zoubin, store: *store, secret: secret, base: base,
			pgPort: *pgPort, setup: setupPath(rest[0], sc.Setup),
			refs: refs, state: state, jobs: *jobs,
		})
		if err != nil {
			stop()
			dump(logPath)
			die(err)
		}
		result["provision"] = prov
	}

	phase := func(name string, drawFrom []string) {
		before, errBefore := fleet.Scrape(opsURL)
		out, err := fleet.Drive(context.Background(), fleet.Options{
			BaseURL:  base,
			Refs:     drawFrom,
			Secret:   secret,
			Clients:  sc.Clients,
			Duration: time.Duration(sc.Duration) * time.Second,
			Warmup:   time.Duration(sc.Warmup) * time.Second,
			Rate:     sc.Rate,
			Requests: sc.Requests,
		})
		if err != nil {
			stop()
			dump(logPath)
			die(err)
		}
		block := map[string]any{
			"tenants_drawn_from": len(drawFrom),
			"tenants_touched":    out.Touched,
			"requests":           out.Requests,
			"errors":             out.Errors,
			"rps":                round2(float64(out.Requests) / out.Elapsed.Seconds()),
			"latency_ms":         latency.Percentiles(out.Samples),
			"buckets":            latency.Buckets(out.Samples, 30),
			"per_request":        out.PerRequest,
			"status":             out.Status,
			"bytes_read":         out.Bytes,
			"seconds":            round2(out.Elapsed.Seconds()),
		}
		if len(out.Failures) > 0 {
			block["failures"] = out.Failures
		}
		after, errAfter := fleet.Scrape(opsURL)
		if errBefore == nil && errAfter == nil {
			d := fleet.Delta(before, after)
			node := map[string]any{
				"attached_now":       after.Get("zou_tenants_attached"),
				"attaches_ok":        d.Get("zou_tenant_attaches_total{outcome=\"ok\"}"),
				"attaches_error":     d.Get("zou_tenant_attaches_total{outcome=\"error\"}"),
				"registry_hits":      d.Get("zou_registry_lookups_total{result=\"hit\"}"),
				"registry_misses":    d.Get("zou_registry_lookups_total{result=\"miss\"}"),
				"attach_buckets_s":   fleet.Buckets(d, "zou_tenant_attach_seconds"),
				"http_requests_rest": d.Get("zou_http_requests_total{surface=\"rest\",status=\"200\"}"),
			}
			if mean, ok := fleet.AttachSeconds(d); ok {
				node["attach_mean_s"] = round3(mean)
			}
			if p50, ok := fleet.Quantile(fleet.Buckets(d, "zou_tenant_attach_seconds"), 0.50); ok {
				node["attach_p50_s_ceiling"] = p50
			}
			if p99, ok := fleet.Quantile(fleet.Buckets(d, "zou_tenant_attach_seconds"), 0.99); ok {
				node["attach_p99_s_ceiling"] = p99
			}
			block["node"] = node
		}
		if fp, err := storefs.Measure(runtime); err == nil {
			block["runtime_bytes"] = fp.Bytes
		}
		fmt.Printf("%s: %d requests, %d errors, p50 %.1f ms, p99 %.1f ms\n",
			name, out.Requests, out.Errors,
			latency.Percentiles(out.Samples)["p50"], latency.Percentiles(out.Samples)["p99"])
		result[name] = block
	}

	ready := state.Ready
	if want["steady"] {
		// The steady phase draws from a working set the node's ceiling
		// can hold, so nothing is evicted and the answer is what a
		// packed node costs when the projects being used fit.
		width := sc.WorkingSet
		if width <= 0 || width > len(ready) {
			width = len(ready)
		}
		phase("steady", ready[:width])
	}
	if want["churn"] {
		// The churn phase draws from every tenant, so with a ceiling
		// below the fleet size the node is attaching and evicting for
		// the whole window, which is the number a fleet is sized on.
		phase("churn", ready)
	}

	result["process"] = tree.Finish()
	result["system"] = sys.Finish()
	if opsAfter, err := zoustats.Read(statsPath); err == nil {
		if delta, err := zoustats.Diff(opsBefore, opsAfter); err == nil {
			result["store_ops"] = zoustats.Report(delta)
		}
	}
	if fp, err := storefs.Measure(*store); err == nil && fp.Objects > 0 {
		result["store"] = map[string]any{
			"bytes":            fp.Bytes,
			"objects":          fp.Objects,
			"bytes_per_tenant": fp.Bytes / int64(max(state.Count(), 1)),
		}
	}
	if fp, err := storefs.Measure(runtime); err == nil {
		result["runtime"] = map[string]any{"bytes": fp.Bytes, "objects": fp.Objects}
	}
	result["tenants_ready"] = state.Count()
	stop()

	die(os.MkdirAll(*outdir, 0o755))
	stamp := strings.NewReplacer(":", "", "-", "").Replace(result["date"].(string))[:15]
	path := filepath.Join(*outdir, fmt.Sprintf("%s-%s-%s.json", sc.Name, *label, stamp))
	out, err := json.MarshalIndent(result, "", "  ")
	die(err)
	die(os.WriteFile(path, out, 0o644))
	fmt.Println(path)
}

type serveArgs struct {
	zoubin, pgbin, store, runtime, log, stats, cpus string
	http, pg, ops, maxAttached, idle                int
	sharedBuffers                                   string
}

// startServe runs the node under test. taskset is how a box with more
// cores than the node is supposed to have still measures that node: the
// harness and the tenants it drives must not be sharing the cores the
// answer is about.
func startServe(a serveArgs) (*exec.Cmd, error) {
	argv := []string{a.zoubin, "serve", a.store,
		"--pg-bin", a.pgbin,
		"--runtime", a.runtime,
		"--http", strconv.Itoa(a.http),
		"--pg", strconv.Itoa(a.pg),
		"--pool", "0",
		"--ops", strconv.Itoa(a.ops),
	}
	if a.maxAttached > 0 {
		argv = append(argv, "--max-attached", strconv.Itoa(a.maxAttached))
	}
	if a.idle > 0 {
		argv = append(argv, "--idle-secs", strconv.Itoa(a.idle))
	}
	if a.sharedBuffers != "" {
		argv = append(argv, "--shared-buffers", a.sharedBuffers)
	}
	if a.cpus != "" {
		argv = append([]string{"taskset", "-c", a.cpus}, argv...)
	}
	logf, err := os.OpenFile(a.log, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	defer logf.Close()
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stdout, cmd.Stderr = logf, logf
	cmd.Env = append(os.Environ(), "ZOU_STORE_STATS="+a.stats)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return cmd, nil
}

func waitHealthy(url string, within time.Duration) error {
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		res, err := client.Get(url)
		if err == nil {
			res.Body.Close()
			if res.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("the node was not healthy within %s", within)
}

type provisionArgs struct {
	zoubin, store, secret, base, setup string
	pgPort                             int
	refs                               []string
	state                              *fleet.State
	jobs                               int
}

// provision makes the tenants that are not there yet. Three steps per
// tenant: register the ref, which writes one small object and starts
// nothing; apply the schema over the postgres door, which is the step
// that runs initdb and captures the genesis, so it is the cold create
// and it is timed; and one http request, which is what applies the
// tenant contract the api needs and is therefore its own first touch.
func provision(a provisionArgs) (map[string]any, error) {
	if a.jobs <= 0 {
		a.jobs = 8
	}
	var todo []string
	for _, ref := range a.refs {
		if !a.state.Has(ref) {
			todo = append(todo, ref)
		}
	}
	start := time.Now()
	var mu sync.Mutex
	var createMS, firstHTTPMS []float64
	var failed []string
	work := make(chan string)
	var wg sync.WaitGroup
	for range a.jobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: 120 * time.Second}
			for ref := range work {
				t0 := time.Now()
				if err := makeTenant(a, ref); err != nil {
					mu.Lock()
					if len(failed) < 5 {
						failed = append(failed, ref+": "+err.Error())
					}
					mu.Unlock()
					continue
				}
				created := msSince(t0)
				t1 := time.Now()
				err := touch(client, a.base, ref, a.secret)
				touched := msSince(t1)
				mu.Lock()
				if err != nil {
					if len(failed) < 5 {
						failed = append(failed, ref+" first http: "+err.Error())
					}
				} else {
					createMS = append(createMS, created)
					firstHTTPMS = append(firstHTTPMS, touched)
				}
				mu.Unlock()
				if err == nil {
					if err := a.state.Add(ref); err != nil {
						return
					}
				}
			}
		}()
	}
	for _, ref := range todo {
		work <- ref
	}
	close(work)
	wg.Wait()

	out := map[string]any{
		"tenants_made":     len(createMS),
		"tenants_skipped":  len(a.refs) - len(todo),
		"seconds":          round2(time.Since(start).Seconds()),
		"cold_create_ms":   latency.Percentiles(asSamples(createMS)),
		"first_request_ms": latency.Percentiles(asSamples(firstHTTPMS)),
		"jobs":             a.jobs,
	}
	if len(createMS) > 0 {
		out["tenants_per_minute"] = round2(float64(len(createMS)) / time.Since(start).Minutes())
	}
	if len(failed) > 0 {
		out["failures"] = failed
		return out, fmt.Errorf("%d tenants could not be provisioned, first: %s", len(failed), failed[0])
	}
	return out, nil
}

// makeTenant registers a ref and applies the schema, which is the point
// at which a registered project becomes a database.
func makeTenant(a provisionArgs, ref string) error {
	out, err := exec.Command(a.zoubin, "tenant", a.store, "create", ref, "--secret", a.secret).CombinedOutput()
	if err != nil && !strings.Contains(string(out), "already") {
		return fmt.Errorf("tenant create: %s", strings.TrimSpace(string(out)))
	}
	if a.setup == "" {
		return nil
	}
	dsn := fmt.Sprintf("host=127.0.0.1 port=%d user=service_role.%s dbname=postgres", a.pgPort, ref)
	cmd := exec.Command(pgbench.Tool("psql"),
		append([]string{"-v", "ON_ERROR_STOP=1", "-q", "-f", a.setup}, pgbench.DSNArgs(dsn)...)...)
	cmd.Env = append(os.Environ(), "PGPASSWORD="+resthttp.KeyToken(a.secret, "service_role"))
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("setup: %s", strings.TrimSpace(string(out)))
	}
	return nil
}

// touch sends the one request that makes the api side of a tenant real,
// which is where the tenant contract, the roles and the auth helpers
// are applied.
func touch(client *http.Client, base, ref, secret string) error {
	url := fleet.URL(base, ref, "/rest/v1", "/bench_rows?select=id&limit=1")
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("apikey", resthttp.KeyToken(secret, "anon"))
	req.Header.Set("authorization", "Bearer "+resthttp.KeyToken(secret, "service_role"))
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("first request answered %s", res.Status)
	}
	return nil
}

func setupPath(scenarioPath, setup string) string {
	if setup == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(scenarioPath), setup)
}

func asSamples(vals []float64) []latency.Sample {
	out := make([]latency.Sample, len(vals))
	for i, v := range vals {
		out[i] = latency.Sample{MS: v}
	}
	return out
}

func msSince(t time.Time) float64 {
	return float64(time.Since(t).Microseconds()) / 1000.0
}

func dump(path string) {
	if raw, err := os.ReadFile(path); err == nil {
		lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
		if len(lines) > 40 {
			lines = lines[len(lines)-40:]
		}
		fmt.Fprintln(os.Stderr, strings.Join(lines, "\n"))
	}
}

func round2(v float64) float64 { return float64(int(v*100+0.5)) / 100 }
