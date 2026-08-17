package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/tamnd/zou-bench/envinfo"
	"github.com/tamnd/zou-bench/pgbench"
	"github.com/tamnd/zou-bench/resthttp"
	"github.com/tamnd/zou-bench/sampler"
	"github.com/tamnd/zou-bench/scenario"
	"github.com/tamnd/zou-bench/sockets"
)

// cmdSockets holds a great many realtime sockets against one node and
// commits rows under them, which is the one measurement in this harness
// that cannot be taken with http requests: what a change costs when a
// hundred thousand people are waiting for it.
//
// The generator and the node have to be different boxes for the number
// to mean anything. A hundred thousand sockets is a hundred thousand
// descriptors and a goroutine each on this side, and if that is on the
// node then the node's cpu is partly this program.
func cmdSockets(argv []string) {
	fs := flag.NewFlagSet("sockets", flag.ExitOnError)
	url := fs.String("url", "", "base url of the api, e.g. http://server3:54321")
	label := fs.String("label", "", "zou-server3, supabase-realtime, ...")
	secret := fs.String("jwt-secret", "", "project jwt secret, the api key is minted from it")
	anonKey := fs.String("anon-key", "", "anon api key, when it is not minted from the secret")
	dsn := fs.String("dsn", "", "libpq DSN for the setup file, on the node's postgres")
	writeDSN := fs.String("write-dsn", "", "host:port or socket directory the writer commits rows over, trust auth")
	writeUser := fs.String("write-user", "postgres", "role the writer connects as, postgres.<ref> on a served project")
	writeDB := fs.String("write-db", "postgres", "database the writer connects to")
	writePass := fs.String("write-password", "", "password for the writer, default the postgres role key minted from the secret")
	ops := fs.String("ops", "", "the node's metrics url, e.g. http://server3:9464/metrics")
	local := fs.String("local", "", "comma separated source addresses to open sockets from")
	ports := fs.String("ports", "", "comma separated http ports on the node to spread sockets over, default the one in --url")
	dialers := fs.Int("dialers", 64, "how many handshakes are in flight at once")
	readBuf := fs.Int("read-buf", 2048, "read buffer per socket in bytes")
	datadir := fs.String("datadir", "", "local server datadir for process sampling, when the node is this box")
	nodeNote := fs.String("node-note", "", "what else the node was doing, recorded with the numbers")
	outdir := fs.String("outdir", "results", "result directory")
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
	if !sc.IsSockets() {
		die(fmt.Errorf("%s is not a sockets scenario", rest[0]))
	}

	key := *anonKey
	if key == "" && *secret != "" {
		key = resthttp.KeyToken(*secret, "anon")
	}
	// The writer's password on a served project is the postgres role key,
	// the same one psql needs for the same door, so it is minted here
	// rather than pasted onto a command line. A plain postmaster on trust
	// never asks and this goes unused.
	pass := *writePass
	if pass == "" && *secret != "" {
		pass = resthttp.KeyToken(*secret, "postgres")
	}

	// The table the sockets subscribe to and the writer writes, plus its
	// publication and its grant. Applied every run, because a number
	// measured against whatever was left in the database is not a number
	// about this scenario.
	if sc.Setup != "" {
		if *dsn == "" {
			die(fmt.Errorf("%s has a setup file, so the run needs a --dsn to apply it", rest[0]))
		}
		path := filepath.Join(filepath.Dir(rest[0]), sc.Setup)
		cmd := exec.Command(pgbench.Tool("psql"), append([]string{"-v", "ON_ERROR_STOP=1", "-q", "-f", path}, pgbench.DSNArgs(*dsn)...)...)
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		// The same door the writer uses, so the same password: a served
		// project asks for one in the clear and a bare postmaster on trust
		// ignores it.
		cmd.Env = append(os.Environ(), "PGPASSWORD="+pass)
		die(cmd.Run())
	}

	// One descriptor per socket, plus the writer's connections and the
	// runtime's own, and a margin so the last thousand sockets do not
	// fail for the sake of a round number.
	files, ferr := sockets.RaiseFiles(uint64(sc.Sockets) + 4096)
	if ferr != nil {
		fmt.Fprintf(os.Stderr, "zoubench: the open file limit stopped at %d: %v\n", files, ferr)
	}
	if files < uint64(sc.Sockets) {
		fmt.Fprintf(os.Stderr, "zoubench: this box allows %d open files and the run wants %d sockets, so expect refusals\n", files, sc.Sockets)
	}

	result := map[string]any{
		"scenario":   sc.Name,
		"label":      *label,
		"date":       time.Now().UTC().Format("2006-01-02T15:04:05Z07:00"),
		"config":     doc,
		"env":        envinfo.Capture(),
		"base_url":   *url,
		"file_limit": files,
		// How the sockets were spread over the node's doors, because one
		// port and three are not the same run and the difference does not
		// show anywhere else in here.
		"ports": doors(*url, *ports),
	}
	// What else the box under test was doing. A p99 measured next to
	// somebody else's build is a p99 with that in it, and a number
	// published without saying so is a number that reads as better than
	// it is.
	if *nodeNote != "" {
		result["node_note"] = *nodeNote
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

	// A run that is interrupted still writes what it measured. Holding a
	// hundred thousand sockets for ten minutes and losing the numbers to
	// a ctrl-c is an hour of somebody's day.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	out, err := sockets.Run(ctx, sockets.Options{
		BaseURL:       strings.TrimSuffix(*url, "/"),
		APIKey:        key,
		Sockets:       sc.Sockets,
		Shards:        sc.Shards,
		ConnectRate:   sc.ConnectRate,
		Dialers:       *dialers,
		ReadBuf:       *readBuf,
		Local:         split(*local),
		Ports:         numbers(*ports),
		Table:         sc.Table,
		WriteDSN:      *writeDSN,
		WriteUser:     *writeUser,
		WriteDB:       *writeDB,
		WritePassword: pass,
		Writers:       sc.Writers,
		Rows:          sc.Rows,
		Batch:         sc.Batch,
		Warmup:        time.Duration(sc.Warmup) * time.Second,
		Duration:      time.Duration(sc.Duration) * time.Second,
		Drain:         time.Duration(sc.DrainSecs) * time.Second,
		Heartbeat:     time.Duration(sc.HeartbeatSecs) * time.Second,
		OpsURL:        *ops,
		Say: func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, "zoubench: "+format+"\n", args...)
		},
	})
	die(err)

	blob, err := json.Marshal(out)
	die(err)
	var run map[string]any
	die(json.Unmarshal(blob, &run))
	for k, v := range run {
		result[k] = v
	}
	if tree != nil {
		result["server"] = tree.Finish()
	}
	result["system"] = sys.Finish()

	die(os.MkdirAll(*outdir, 0o755))
	stamp := strings.NewReplacer(":", "", "-", "").Replace(result["date"].(string))[:15]
	path := filepath.Join(*outdir, fmt.Sprintf("%s-%s-%s.json", sc.Name, *label, stamp))
	pretty, err := json.MarshalIndent(result, "", "  ")
	die(err)
	die(os.WriteFile(path, pretty, 0o644))
	fmt.Println(path)

	// The headline in the terminal, and the two things that would make it
	// a lie if they were not zero.
	if out.Missing != 0 {
		fmt.Fprintf(os.Stderr, "zoubench: %d of %d deliveries never arrived, so the percentiles are over what did\n",
			out.Missing, out.Expected)
	}
	if out.Refused != 0 || out.Lost != 0 {
		fmt.Fprintf(os.Stderr, "zoubench: %d sockets refused and %d lost mid run: %s\n",
			out.Refused, out.Lost, strings.Join(out.Failures, "; "))
	}
	brief := map[string]any{}
	for k, v := range result {
		switch k {
		case "config", "env", "system", "server", "timeline":
		default:
			brief[k] = v
		}
	}
	summary, _ := json.MarshalIndent(brief, "", "  ")
	fmt.Println(string(summary))
}

// numbers reads a comma separated port list. A part that is not a
// number is a run that would open every socket to the wrong place, so
// it stops here rather than there.
// Which of the node's http ports the sockets went to. With no list it
// is the one on the url, so a result says the same thing either way
// rather than saying nothing when the flag was left out.
func doors(base, list string) []int {
	if ports := numbers(list); len(ports) > 0 {
		return ports
	}
	u, err := url.Parse(base)
	if err != nil || u.Port() == "" {
		return nil
	}
	return numbers(u.Port())
}

func numbers(list string) []int {
	var out []int
	for _, part := range split(list) {
		n, err := strconv.Atoi(part)
		die(err)
		out = append(out, n)
	}
	return out
}

func split(list string) []string {
	var out []string
	for _, part := range strings.Split(list, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
