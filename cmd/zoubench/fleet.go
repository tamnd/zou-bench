package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/tamnd/zou-bench/cost"
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
	phases := fs.String("phases", "provision,steady,hold,churn", "which phases to run")
	jobs := fs.Int("jobs", 8, "tenants provisioned at once")
	cpus := fs.String("cpus", "", "cpu list the node is pinned to, for example 0-7")
	httpPort := fs.Int("http", 54321, "http door")
	pgPort := fs.Int("pg", 54322, "postgres door")
	opsPort := fs.Int("ops", 54323, "ops port, where the node's own numbers are read")
	prefix := fs.String("prefix", "t", "tenant ref prefix")
	keep := fs.Bool("keep-node", false, "leave the node running after the run")
	cardsdir := fs.String("cards", "pricecards", "directory of price cards")
	pricecard := fs.String("pricecard", "", "price the hold phase against these cards, comma separated")
	boxUSD := fs.Float64("box-usd-month", 0, "what the node itself costs a month, 0 leaves compute out")
	boxSource := fs.String("box-source", "", "where that price came from, recorded with it")
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
		opsPhaseBefore, statsOK := zoustats.Read(statsPath)
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
		// What this phase alone asked of the store. The whole run's
		// counters are reported below as well, but they carry
		// provisioning, which is a thousand initdbs and drowns out
		// everything a phase did.
		if statsOK == nil {
			if after, err := zoustats.Read(statsPath); err == nil {
				if d, err := zoustats.Diff(opsPhaseBefore, after); err == nil {
					block["store_ops"] = zoustats.Report(d)
				}
			}
		}
		fmt.Printf("%s: %d requests, %d errors, p50 %.1f ms, p99 %.1f ms\n",
			name, out.Requests, out.Errors,
			latency.Percentiles(out.Samples)["p50"], latency.Percentiles(out.Samples)["p99"])
		// A phase that answered wrongly is not a slow phase, and the
		// status it answered with is the whole diagnosis, so it goes to
		// the terminal rather than only into a file nobody opens until
		// the run is over.
		if out.Errors > 0 {
			fmt.Printf("%s: status %v\n", name, out.Status)
			for _, f := range out.Failures {
				fmt.Printf("%s: %s\n", name, f)
			}
		}
		result[name] = block
	}

	// hold asks the node for nothing at all for a while, and watches.
	//
	// Everything else here measures what a node does when it is asked.
	// The long tail bill is the other half: eight hundred projects
	// nobody is using still sit in a bucket, and whatever the node does
	// on its own while they sleep is what they cost forever. That is a
	// rate, so it is sampled rather than totalled, and the gauge is
	// sampled with it because a project the node is holding and a
	// project it has let go are two different prices.
	//
	// The phase deliberately outlasts the node's idle budget, so one
	// window carries both: attached at the start, dormant by the end.
	// Its first `settle` seconds are sampled and then left out, because
	// the node is still writing down the phase before.
	hold := func(name string, seconds, every, settle int) fleet.Idle {
		started := time.Now()
		var samples []fleet.Sample
		take := func() {
			m, errM := fleet.Scrape(opsURL)
			c, errC := zoustats.Read(statsPath)
			if errM != nil || errC != nil {
				return
			}
			t := zoustats.Sum(c)
			samples = append(samples, fleet.Sample{
				Elapsed:  round2(time.Since(started).Seconds()),
				Attached: int(m.Get("zou_tenants_attached")),
				Puts:     t.Puts,
				Gets:     t.Gets,
				Lists:    t.Lists,
				Deletes:  t.Deletes,
			})
		}
		take()
		for time.Since(started) < time.Duration(seconds)*time.Second {
			time.Sleep(time.Duration(every) * time.Second)
			take()
		}
		rates := fleet.Rates(fleet.After(samples, float64(settle)))
		block := map[string]any{
			"seconds":     seconds,
			"every":       every,
			"settle_secs": settle,
			"rates":       rates,
			// Every sample, the settled ones included, because a reader
			// has to be able to see what was left out and how much work
			// was still going on when the window opened.
			"samples": samples,
		}
		if fp, err := storefs.Measure(runtime); err == nil {
			block["runtime_bytes"] = fp.Bytes
		}
		result[name] = block
		fmt.Printf("%s: %.0f s attached and %.0f s dormant, %.1f puts and %.1f gets an hour per held project\n",
			name, rates.AttachedSeconds, rates.DormantSeconds,
			rates.AttachedPerProjectHour.Puts, rates.AttachedPerProjectHour.Gets)
		return rates
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
	var idle fleet.Idle
	if want["hold"] && sc.Hold > 0 {
		idle = hold("hold", sc.Hold, sc.SampleSecs, sc.SettleSecs)
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

	// Dollars, from the hold phase and the store the run left behind.
	//
	// Only the hold phase, because a month of this fleet is a month of
	// nothing happening, and the steady and churn windows are the fleet
	// being hammered. Pricing those as if they ran all month would
	// answer a question nobody asked.
	if *pricecard != "" && idle.Samples > 0 {
		if fp, err := storefs.Measure(*store); err == nil && fp.Objects > 0 {
			tail := cost.Tail{
				Projects:       state.Count(),
				AttachedAtOnce: sc.MaxAttached,
				StorageBytes:   fp.Bytes,
				DormantPerHour: cost.Ops{
					Puts:    idle.DormantPerHour.Puts,
					Gets:    idle.DormantPerHour.Gets,
					Lists:   idle.DormantPerHour.Lists,
					Deletes: idle.DormantPerHour.Deletes,
				},
				AttachedPerProjectHour: cost.Ops{
					Puts:    idle.AttachedPerProjectHour.Puts,
					Gets:    idle.AttachedPerProjectHour.Gets,
					Lists:   idle.AttachedPerProjectHour.Lists,
					Deletes: idle.AttachedPerProjectHour.Deletes,
				},
				BoxUSDMonth: *boxUSD,
				BoxSource:   *boxSource,
			}
			var priced []any
			for _, name := range strings.Split(*pricecard, ",") {
				name = strings.TrimSpace(name)
				if name == "" {
					continue
				}
				card, err := cost.Find(*cardsdir, name)
				die(err)
				block := cost.Monthly(card, tail)
				// A per project rate taken from a window that never
				// went dormant has the node's own housekeeping inside
				// it, so the dollars are an upper bound and the file
				// has to say which it is.
				block["dormant_window_measured"] = idle.SawDormant
				priced = append(priced, block)
				fmt.Printf("cost: %s, %v usd a month for %d projects, %v each\n",
					name, block["total_usd_month"], tail.Projects, block["usd_per_project_month"])
			}
			result["cost"] = priced
		}
	}
	stop()

	die(os.MkdirAll(*outdir, 0o755))
	stamp := strings.NewReplacer(":", "", "-", "").Replace(result["date"].(string))[:15]
	path := filepath.Join(*outdir, fmt.Sprintf("%s-%s-%s.json", sc.Name, *label, stamp))
	var bad []string
	clean := jsonable(result, "", &bad)
	if len(bad) > 0 {
		fmt.Printf("zoubench: not a number, written as null: %s\n", strings.Join(bad, ", "))
	}
	out, err := json.MarshalIndent(clean, "", "  ")
	die(err)
	die(os.WriteFile(path, out, 0o644))
	fmt.Println(path)
}

// jsonable makes a result writable when one number in it is not.
//
// An infinity or a NaN, which is what a rate over a window of no time
// comes to, makes encoding/json refuse the whole document, so a run
// that took an hour is lost to one division. This walks the result,
// writes the offending leaf as null, and returns the paths so the run
// says which number it was rather than leaving it to be guessed at.
func jsonable(v any, path string, bad *[]string) any {
	if _, err := json.Marshal(v); err == nil {
		return v
	}
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = jsonable(val, path+"/"+k, bad)
		}
		return out
	case map[string]float64:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = jsonable(val, path+"/"+k, bad)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = jsonable(val, fmt.Sprintf("%s/%d", path, i), bad)
		}
		return out
	}
	*bad = append(*bad, strings.TrimPrefix(path, "/"))
	return nil
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
// tenant, and the order between the last two is not free: register the
// ref, which writes one small object and starts nothing; then one http
// request, which is what runs initdb, captures the genesis and applies
// the tenant contract, so it is the cold create and it is timed; and
// only then the schema over the postgres door, because the postgres
// door logs in as service_role and that role is one the contract
// creates.
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
				err := register(a, ref)
				created := msSince(t0)
				t1 := time.Now()
				if err == nil {
					err = touch(client, a.base, ref, a.secret)
				}
				touched := msSince(t1)
				if err == nil {
					err = applySetup(a, ref)
				}
				mu.Lock()
				if err != nil {
					if len(failed) < 5 {
						failed = append(failed, ref+": "+err.Error())
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

// register writes the ref into the registry, which costs one small
// object and starts no database.
func register(a provisionArgs, ref string) error {
	out, err := exec.Command(a.zoubin, "tenant", a.store, "create", ref, "--secret", a.secret).CombinedOutput()
	if err != nil && !strings.Contains(string(out), "already") {
		return fmt.Errorf("tenant create: %s", strings.TrimSpace(string(out)))
	}
	return nil
}

// applySetup puts the project's own schema in over the postgres door,
// which is the point at which a registered project becomes a project
// with something in it.
func applySetup(a provisionArgs, ref string) error {
	if a.setup == "" {
		return nil
	}
	// As postgres and not as service_role, because the schema is the
	// project's own DDL and service_role owns nothing, the same split a
	// Supabase project has between its migrations and its api.
	dsn := fmt.Sprintf("host=127.0.0.1 port=%d user=postgres.%s dbname=postgres", a.pgPort, ref)
	cmd := exec.Command(pgbench.Tool("psql"),
		append([]string{"-v", "ON_ERROR_STOP=1", "-q", "-f", a.setup}, pgbench.DSNArgs(dsn)...)...)
	cmd.Env = append(os.Environ(), "PGPASSWORD="+resthttp.KeyToken(a.secret, "postgres"))
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("setup: %s", strings.TrimSpace(string(out)))
	}
	return nil
}

// touch sends the one request that makes the api side of a tenant real,
// which is where the tenant contract, the roles and the auth helpers
// are applied. It asks for the root document rather than for a table,
// because at this point the project has no tables.
func touch(client *http.Client, base, ref, secret string) error {
	url := fleet.URL(base, ref, "/rest/v1", "/")
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
		// The body is where the node says what actually went wrong, and a
		// provisioning failure that only carries a status number is a
		// second run to find out what the first one already knew.
		body, _ := io.ReadAll(io.LimitReader(res.Body, 400))
		return fmt.Errorf("first request answered %s: %s", res.Status, strings.TrimSpace(string(body)))
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
