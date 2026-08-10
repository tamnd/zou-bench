package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tamnd/zou-bench/envinfo"
	"github.com/tamnd/zou-bench/pgbench"
	"github.com/tamnd/zou-bench/pgwire"
	"github.com/tamnd/zou-bench/sampler"
	"github.com/tamnd/zou-bench/scenario"
	"github.com/tamnd/zou-bench/sustain"
	"github.com/tamnd/zou-bench/zoustats"
)

// cmdSustain soaks one store for hours: pgbench load in fixed
// segments, one kill drill somewhere inside each, and recovery
// clocked as the wall time from the kill instant to the first COMMIT
// that returned. Unlike run, which connects to whatever a DSN points
// at, this command owns the zou dev lifecycle for the whole soak,
// because the drills kill the very processes an outside supervisor
// would be trying to keep alive.
func cmdSustain(argv []string) {
	fs := flag.NewFlagSet("sustain", flag.ExitOnError)
	pgbin := fs.String("pgbin", "", "directory holding the postgres binaries")
	zoubin := fs.String("zoubin", "", "path to the zou binary")
	store := fs.String("store", "", "store target zou dev attaches, the thing being soaked")
	workdir := fs.String("workdir", "", "directory for runtimes, dev logs, and the counter file")
	label := fs.String("label", "", "zou-minio, zou-s3, ...")
	outdir := fs.String("outdir", "results", "result directory")
	simulated := fs.String("simulated", "", "mark the run simulated with this sim spec, defaults to ZOU_STORE_SIM or ZOU_STORE_DELAY when either is set")
	var rest []string
	for _, a := range argv {
		if !strings.HasPrefix(a, "-") && strings.HasSuffix(a, ".json") {
			rest = append(rest, a)
		}
	}
	fs.Parse(without(argv, rest))
	if len(rest) != 1 || *pgbin == "" || *zoubin == "" || *store == "" || *workdir == "" || *label == "" {
		usage()
	}
	sc, doc, err := scenario.Load(rest[0])
	die(err)
	if !sc.IsSustain() {
		die(fmt.Errorf("%s is not a sustain scenario", rest[0]))
	}
	if runtime.GOOS == "windows" {
		die(fmt.Errorf("sustain drills kill processes with unix signals, this command does not run on windows"))
	}
	// The cluster superuser zou dev initdb's with, by that project's
	// design: the owner of a database does not depend on which account
	// started the process. Connecting as the OS user only ever worked
	// on stores where someone had created that role by hand, and a
	// fresh store spent the whole readiness window on "role does not
	// exist" before dying.
	pguser := "postgres"

	die(os.MkdirAll(*workdir, 0o755))
	// One counter file for the whole soak. The counters reset with
	// every store boot and Diff refuses shrunken counters, so the
	// harness snapshots per segment and drops the segments a kill
	// landed in, rather than juggling one file per boot.
	statsPath := filepath.Join(*workdir, "store-stats")
	addr := "127.0.0.1:" + strconv.Itoa(sc.Port)
	connArgs := []string{"-h", "127.0.0.1", "-p", strconv.Itoa(sc.Port), "-U", pguser, "postgres"}

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

	runStart := time.Now()
	rtSeq := 0
	dev, err := startDev(*zoubin, *store, *pgbin, sc.Port,
		filepath.Join(*workdir, "rt-0"), filepath.Join(*workdir, "dev-0.log"), statsPath, false)
	die(err)

	// die would leave the node and its postgres tree running, so every
	// fatal exit during setup takes the node down first.
	dieDown := func(err error) {
		if err != nil {
			dev.killNode()
			die(err)
		}
	}

	// A fresh store runs initdb and captures a genesis checkpoint
	// before the first query answers, minutes rather than seconds on
	// a slow store, so the setup deadline is generous. The readiness
	// probe is a write from the very start: select 1 answers while
	// commits still wait behind the zou durable-LSN gate, and a soak
	// that begins before writes flow would charge that wait to the
	// first segment.
	if _, ok := waitWrite(addr, pguser, "create table if not exists zoubench_probe(x int)",
		runStart, time.Now().Add(15*time.Minute)); !ok {
		dieDown(fmt.Errorf("server at %s never took a committed write, see the dev log in %s", addr, *workdir))
	}

	// CHECKPOINT before the bulk load folds the genesis WAL, so init
	// starts against a consolidated store instead of paying the fold
	// in the middle of loading a thousand warehouses.
	_, err = soakQuery(addr, pguser, "checkpoint")
	dieDown(err)
	tInit := time.Now()
	initArgs := []string{"-i", "-q", "-s", strconv.Itoa(sc.Scale)}
	if sc.ServerSideInit {
		initArgs = []string{"-i", "-I", "dtGvp", "-s", strconv.Itoa(sc.Scale)}
	}
	init := exec.Command(filepath.Join(*pgbin, "pgbench"),
		append(initArgs, connArgs...)...)
	init.Stdout, init.Stderr = os.Stderr, os.Stderr
	dieDown(init.Run())
	result["init_seconds"] = round1(time.Since(tInit).Seconds())
	_, err = soakQuery(addr, pguser, "create table if not exists zoubench_ledger(id bigint primary key)")
	dieDown(err)

	led := sustain.NewLedger()
	stopLedger := make(chan struct{})
	go ledgerWriter(addr, pguser, led, stopLedger)

	nSeg := max(sc.Duration/sc.Segment, 1)
	// The balance identity only exists for the tpcb builtin, whose
	// history table records every delta the balances absorbed.
	tpcb := sc.Builtin == "" || sc.Builtin == "tpcb-like"
	var segments []map[string]any
	var drills []sustain.Drill
	var overMu sync.Mutex
	var overs []bool
	violations := 0
	var tpsVals []float64

	for seq := 0; seq < nSeg; seq++ {
		opsBefore, opsErr := zoustats.Read(statsPath)
		var tree *sampler.Tree
		// A new Tree per segment, rooted at whatever postmaster.pid
		// says right now: the drills change the postmaster's pid and
		// sampler.Tree goes silent once its root vanishes, so a tree
		// spanning drills would quietly stop measuring. Per segment
		// trees lose the tail after a mid segment kill, which is the
		// honest gap rather than a hidden one.
		if pid, err := serverPid(filepath.Join(dev.runtime, "pgdata")); err == nil {
			tree = sampler.NewTree(pid)
			tree.Start()
		}
		stopTick := make(chan struct{})
		var ampMu sync.Mutex
		var ampTimeline []map[string]any
		go checkpointLoop(addr, pguser, time.Duration(sc.CheckpointSecs)*time.Second, stopTick)
		go compactLoop(*zoubin, *store, time.Duration(sc.CompactSecs)*time.Second, runStart, stopTick,
			func(t, amp float64, over bool) {
				ampMu.Lock()
				ampTimeline = append(ampTimeline, map[string]any{"t_s": t, "amp_max": amp})
				ampMu.Unlock()
				overMu.Lock()
				overs = append(overs, over)
				overMu.Unlock()
			})

		// -n matters: a plain pgbench run truncates pgbench_history
		// before starting, which would wipe the very rows the balance
		// invariant sums. The zou soak found that the hard way when an
		// iteration read as data loss.
		args := []string{"-n", "-c", strconv.Itoa(sc.Clients), "-j", strconv.Itoa(sc.Threads), "-T", strconv.Itoa(sc.Segment)}
		if sc.Rate > 0 {
			args = append(args, "-R", strconv.Itoa(sc.Rate))
		}
		if sc.Builtin != "" && sc.Builtin != "tpcb-like" {
			args = append(args, "-b", sc.Builtin)
		}
		bench := exec.Command(filepath.Join(*pgbin, "pgbench"), append(args, connArgs...)...)
		var benchOut strings.Builder
		bench.Stdout, bench.Stderr = &benchOut, &benchOut
		benchErr := bench.Start()
		benchDone := make(chan error, 1)
		if benchErr == nil {
			go func() { benchDone <- bench.Wait() }()
		} else {
			benchDone <- benchErr
		}

		// The drill lands at a random point in the middle half of the
		// segment. Killing under load is the point, and the margins
		// keep the kill away from pgbench's own connect and drain
		// phases, which would measure the client instead of the store.
		time.Sleep(time.Duration(float64(sc.Segment)*(0.25+rand.Float64()*0.5)) * time.Second)

		mode := sc.Drills[seq%len(sc.Drills)]
		killT := time.Now()
		drill := sustain.Drill{Seq: seq, Mode: mode, T: round1(killT.Sub(runStart).Seconds())}
		var killErr error
		switch mode {
		case "pusher":
			var pid int
			pid, killErr = pusherPid(filepath.Join(dev.runtime, "pgdata"))
			if killErr == nil {
				killErr = sigkill(pid)
			}
		case "crash":
			var pid int
			pid, killErr = serverPid(filepath.Join(dev.runtime, "pgdata"))
			if killErr == nil {
				killErr = sigkill(pid)
			}
		case "death":
			dev.killNode()
			rtSeq++
			// The fresh node may steal the dead node's lease: the
			// harness just killed the holder, so a live looking lease
			// is a leftover, not a split brain risk. This is the only
			// place ZOU_LEASE_STEAL is ever set.
			dev, killErr = startDev(*zoubin, *store, *pgbin, sc.Port,
				filepath.Join(*workdir, fmt.Sprintf("rt-%d", rtSeq)),
				filepath.Join(*workdir, fmt.Sprintf("dev-%d.log", rtSeq)), statsPath, true)
			if killErr != nil {
				die(fmt.Errorf("segment %d: no node after the death drill: %w", seq, killErr))
			}
		}
		reapSHM()
		if killErr != nil {
			// A drill that could not kill measured nothing, so its rto
			// stays absent rather than becoming a flattering zero.
			drill.KillError = killErr.Error()
			fmt.Fprintf(os.Stderr, "zoubench: segment %d %s drill: %v\n", seq, mode, killErr)
		} else if rto, ok := waitWrite(addr, pguser, "insert into zoubench_probe values (1)",
			killT, killT.Add(time.Hour)); ok {
			drill.RTOms = &rto
		}
		drill.LedgerOK = verifyLedger(addr, pguser, led)
		if !drill.LedgerOK {
			violations++
		}
		if tpcb {
			ok := verifyBalance(addr, pguser)
			drill.BalanceOK = &ok
			if !ok {
				violations++
			}
		}
		drills = append(drills, drill)

		// A pgbench that died mid segment is the drill working, not a
		// failed run; the segment records that it did not complete.
		werr := <-benchDone
		close(stopTick)

		seg := map[string]any{"seq": seq, "completed": benchErr == nil && werr == nil}
		summary := map[string]any{}
		pgbench.ParseSummary(benchOut.String(), summary)
		if tps, ok := summary["tps"].(float64); ok {
			seg["tps"] = tps
			tpsVals = append(tpsVals, tps)
		}
		if opsErr == nil {
			if opsAfter, err := zoustats.Read(statsPath); err == nil {
				// Diff hard-fails when a counter shrank, which means
				// the store restarted inside the window. In a soak
				// full of kills that is routine, so the segment's op
				// counts go absent instead of aborting the run.
				if delta, err := zoustats.Diff(opsBefore, opsAfter); err == nil {
					seg["store_ops"] = zoustats.Report(delta)
				}
			}
		}
		ampMu.Lock()
		if len(ampTimeline) > 0 {
			seg["amp_timeline"] = ampTimeline
		}
		ampMu.Unlock()
		if tree != nil {
			finished := tree.Finish()
			if v, ok := finished["rss_peak_kb"]; ok {
				seg["rss_peak_kb"] = v
			}
			if v, ok := finished["rss_slope_kb_per_min"]; ok {
				seg["rss_slope_kb_per_min"] = v
			}
		}
		segments = append(segments, seg)
	}

	close(stopLedger)
	dev.killNode()

	result["hours"] = round3(time.Since(runStart).Hours())
	result["segments"] = segments
	result["drills"] = drills
	if s := sustain.RTOSummary(drills); len(s) > 0 {
		result["rto_ms"] = s
	}
	overMu.Lock()
	if len(overs) > 0 {
		result["amp_bound_held"] = sustain.BoundHeld(overs)
	}
	overMu.Unlock()
	result["violations"] = violations
	if len(tpsVals) > 0 {
		sum := 0.0
		for _, v := range tpsVals {
			sum += v
		}
		result["tps_mean"] = round1(sum / float64(len(tpsVals)))
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
		case "config", "env", "segments", "drills":
		default:
			brief[k] = v
		}
	}
	pretty, _ := json.MarshalIndent(brief, "", "  ")
	fmt.Println(string(pretty))
}

// devNode is one zou dev supervisor the harness started, with the
// runtime dir it was given, which is also how every process under it
// is found again after the pids stop meaning anything.
type devNode struct {
	cmd     *exec.Cmd
	runtime string
}

// startDev launches one zou dev over the store. stealLease is set
// only for the node started after a death drill, when the harness
// knows the previous holder is dead; against a live holder it would
// be manufacturing the exact split brain the soak exists to rule out.
func startDev(zoubin, store, pgbin string, port int, rtdir, logPath, statsPath string, stealLease bool) (*devNode, error) {
	logf, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(zoubin, "dev", store, "--pg-bin", pgbin,
		"--port", strconv.Itoa(port), "--runtime", rtdir)
	cmd.Stdout, cmd.Stderr = logf, logf
	cmd.Env = append(os.Environ(), "ZOU_STORE_STATS="+statsPath)
	if stealLease {
		cmd.Env = append(cmd.Env, "ZOU_LEASE_STEAL=1")
	}
	err = cmd.Start()
	logf.Close()
	if err != nil {
		return nil, err
	}
	return &devNode{cmd: cmd, runtime: rtdir}, nil
}

// killNode takes the whole node down the hard way: the supervisor
// first so it cannot restart what follows, then everything still
// holding the runtime dir. Matching on the runtime path rather than
// remembered pids is the zou soak script's trick, pids collected
// before a kill cannot be trusted after one.
func (d *devNode) killNode() {
	if d.cmd.Process != nil {
		sigkill(d.cmd.Process.Pid)
	}
	exec.Command("pkill", "-9", "-f", d.runtime).Run()
	d.cmd.Wait()
	reapSHM()
}

// pusherPid finds the postgres background worker running the v2 WAL
// sequencer by its process title, and only among the children of the
// harness's own postmaster. The machine may run other zou instances
// with pushers of their own, and a title-only match would hand the
// drill an innocent victim. The pid is looked up at kill time, never
// cached, because the worker restarts with the postmaster and a
// cached pid could kill an unrelated process that reused it.
func pusherPid(pgdata string) (int, error) {
	postmaster, err := serverPid(pgdata)
	if err != nil {
		return 0, err
	}
	out, err := exec.Command("ps", "-ax", "-o", "pid=,ppid=,command=").Output()
	if err != nil {
		return 0, err
	}
	for line := range strings.SplitSeq(string(out), "\n") {
		if !strings.Contains(line, "zou wal pusher") {
			continue
		}
		f := strings.Fields(strings.TrimSpace(line))
		if len(f) < 3 {
			continue
		}
		ppid, err := strconv.Atoi(f[1])
		if err != nil || ppid != postmaster {
			continue
		}
		return strconv.Atoi(f[0])
	}
	return 0, fmt.Errorf("no \"zou wal pusher\" under postmaster %d", postmaster)
}

// reapSHM removes this user's SysV shared memory segments that have
// nothing attached. kill -9 leaks the postmaster's interlock segment,
// and every fresh attach uses a fresh runtime dir so a new key never
// reclaims an old one; macOS caps the segment table at 32 ids, and a
// zou soak run once died at iteration 17 with shmget ENOSPC on a
// perfectly good store. The column layout differs per platform, hence
// the split: ipcs -ma on darwin, plain ipcs -m on linux.
func reapSHM() {
	me := ""
	if u, err := user.Current(); err == nil {
		me = u.Username
	}
	var out []byte
	var err error
	var idCol, ownerCol, attachCol int
	if runtime.GOOS == "darwin" {
		out, err = exec.Command("ipcs", "-ma").Output()
		idCol, ownerCol, attachCol = 1, 4, 8
	} else {
		out, err = exec.Command("ipcs", "-m").Output()
		idCol, ownerCol, attachCol = 1, 2, 5
	}
	if err != nil {
		return
	}
	for line := range strings.SplitSeq(string(out), "\n") {
		f := strings.Fields(line)
		if len(f) <= attachCol {
			continue
		}
		if runtime.GOOS == "darwin" && f[0] != "m" {
			continue
		}
		if runtime.GOOS != "darwin" && !strings.HasPrefix(f[0], "0x") {
			continue
		}
		if f[ownerCol] != me || f[attachCol] != "0" {
			continue
		}
		exec.Command("ipcrm", "-m", f[idCol]).Run()
	}
}

// waitWrite polls one committing statement until it succeeds and
// returns the milliseconds since from, which for a drill is the kill
// instant, so the number is the RTO a client would have seen. A fresh
// connection every attempt, because a cached one died with the server
// it was talking to. 25 ms is the tick: RTOs worth reporting are tens
// of milliseconds and up, and a 1 ms spin against a recovering server
// is just extra load on the thing being measured.
func waitWrite(addr, user, sql string, from, deadline time.Time) (float64, bool) {
	for time.Now().Before(deadline) {
		conn, err := pgwire.Dial(addr, user, "postgres")
		if err == nil {
			_, qerr := conn.Query(sql)
			conn.Close()
			if qerr == nil {
				return ms(time.Since(from)), true
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	return 0, false
}

// soakQuery runs one statement with retries on a fresh connection
// each time, because a connection blip during verification must not
// read as data loss; only a stable wrong answer counts.
func soakQuery(addr, user, sql string) (string, error) {
	var last error
	for attempt := range 5 {
		if attempt > 0 {
			time.Sleep(2 * time.Second)
		}
		conn, err := pgwire.Dial(addr, user, "postgres")
		if err != nil {
			last = err
			continue
		}
		v, qerr := conn.Query(sql)
		conn.Close()
		if qerr == nil {
			return v, nil
		}
		last = qerr
	}
	return "", last
}

// ledgerWriter is the harness's own slow write stream: one insert
// every 200 ms, the id recorded only after COMMIT returned. The
// connection is kept until it breaks and redialed on the next tick,
// so during a drill the loop degrades into failed attempts and picks
// itself back up, which is exactly the client a recovery promise is
// made to.
func ledgerWriter(addr, user string, led *sustain.Ledger, stop chan struct{}) {
	var conn *pgwire.Conn
	for {
		select {
		case <-stop:
			if conn != nil {
				conn.Close()
			}
			return
		case <-time.After(200 * time.Millisecond):
		}
		if conn == nil {
			c, err := pgwire.Dial(addr, user, "postgres")
			if err != nil {
				continue
			}
			conn = c
		}
		id := led.Next()
		if _, err := conn.Query(fmt.Sprintf("insert into zoubench_ledger(id) values (%d)", id)); err != nil {
			conn.Close()
			conn = nil
			continue
		}
		led.Ack(id)
	}
}

// checkpointLoop issues CHECKPOINT on the scenario cadence. Nothing
// else drives WAL folding and consolidation on a zou dev node, so
// without this the overlay would grow for the whole soak and the amp
// numbers would be about neglect rather than the store. Errors are
// dropped on purpose: during a drill every statement fails, and that
// is the drill working.
func checkpointLoop(addr, user string, every time.Duration, stop chan struct{}) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		if conn, err := pgwire.Dial(addr, user, "postgres"); err == nil {
			conn.Query("checkpoint")
			conn.Close()
		}
	}
}

// compactLoop is the only driver compaction has: run the sweep, then
// read the per shard status for the amp timeline. The status read
// follows the sweep on purpose, because the exit gate forgives a
// sample over the bound only when the sample after a sweep is back
// under, so what gets recorded must be the post sweep state. A failed
// sweep or unreadable status drops the sample, absent beats invented.
func compactLoop(zoubin, store string, every time.Duration, start time.Time, stop chan struct{}, record func(t, amp float64, over bool)) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		// Two workers: the cadence is the driver here, the width only
		// decides how hard the sweep competes with the load.
		exec.Command(zoubin, "compact", store, "local", "--workers", "2").Run()
		out, err := exec.Command(zoubin, "compact", store, "local", "--status").Output()
		if err != nil {
			continue
		}
		shards, err := sustain.ParseCompactStatus(out)
		if err != nil || len(shards) == 0 {
			continue
		}
		amp, over := sustain.MaxAmp(shards)
		record(round1(time.Since(start).Seconds()), round3(amp), over)
	}
}

// verifyLedger checks that every acked id is present after a
// recovery. The wire client only reads the first column of the first
// row, so membership is checked as chunked count queries over
// any-lists: each chunk of acked ids must count exactly its own
// length. Rows in the table that no chunk mentions are ids whose ack
// was lost in a kill and were never recorded; they are outside the
// promise and not checked.
func verifyLedger(addr, user string, led *sustain.Ledger) bool {
	for _, chunk := range led.Chunks(10000) {
		var sb strings.Builder
		for i, id := range chunk {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(strconv.FormatInt(id, 10))
		}
		got, err := soakQuery(addr, user,
			fmt.Sprintf("select count(*) from zoubench_ledger where id = any('{%s}'::bigint[])", sb.String()))
		if err != nil || got != strconv.Itoa(len(chunk)) {
			return false
		}
	}
	return true
}

// verifyBalance is the tpcb identity from the zou soak script: the
// sum of history deltas must equal each of the account, branch, and
// teller balance sums, which proves replay was exact and not merely
// present. It only means something when pgbench ran with -n, since a
// plain run truncates pgbench_history and wipes the left hand side.
func verifyBalance(addr, user string) bool {
	got, err := soakQuery(addr, user,
		"select coalesce(sum(delta),0) = (select sum(abalance) from pgbench_accounts)"+
			" and coalesce(sum(delta),0) = (select sum(bbalance) from pgbench_branches)"+
			" and coalesce(sum(delta),0) = (select sum(tbalance) from pgbench_tellers)"+
			" from pgbench_history")
	return err == nil && got == "t"
}
