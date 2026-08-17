// Package sockets measures what a node does while it is holding a great
// many realtime sockets and a change is committed under them.
//
// The question the milestone asks is one number: a hundred thousand
// concurrent sockets on one node, a thousand changed rows a second,
// delivery p99. Everything here exists to make that number defensible
// rather than plausible.
//
// Three things it does that a load tool usually does not:
//
// One clock. The row carries the microsecond the generator stamped on
// it, and the delivery is timed against the same process's clock, so the
// latency has no ntp skew in it. A run that timed the commit on the
// database's clock and the delivery on the client's would be publishing
// the difference between two boxes' idea of now.
//
// Exact accounting. Every socket asks for one shard, the writer knows
// how many rows it committed for each shard, and the run knows how many
// sockets are on each. Expected deliveries are a multiplication, not an
// estimate, so a p99 measured over the deliveries that arrived is
// published beside the ones that did not.
//
// The node's own view. The scrape off the ops port says how many sockets
// and subscribers the node thinks it has, and how long a change took
// inside it. A client side number that disagrees with the node's is the
// interesting case, and it cannot be seen from one side.
package sockets

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tamnd/zou-bench/fleet"
	"github.com/tamnd/zou-bench/latency"
)

// Options is one socket run.
type Options struct {
	// BaseURL is the project's api root, http or ws, either is read the
	// same way: the socket path is appended to it.
	BaseURL string
	APIKey  string
	// Token is the access token the socket sends on its join, empty to
	// let the api key stand for it.
	Token string

	// Sockets is how many to hold, Shards how many change subscriptions
	// they divide themselves between. Sockets on the same shard all get
	// the same row, which is what makes one committed row into a fan out
	// rather than a delivery.
	Sockets int
	Shards  int
	// ConnectRate is sockets opened per second during the ramp, 0 for as
	// fast as the box will go. A ramp is not politeness: a hundred
	// thousand simultaneous handshakes measures an accept queue, and the
	// run is about what happens after they are all up.
	ConnectRate int
	Dialers     int
	ReadBuf     int
	// Local are source addresses to open sockets from, round robin. One
	// address has one ephemeral port range, so a single address tops out
	// near sixty thousand sockets to the same port no matter what the
	// file descriptor limit says.
	Local []string
	// Ports are the node's http ports to open sockets to, which is the
	// other half of the same arithmetic: a socket is unique by source
	// address, source port, and destination port, so a node serving its
	// api on three ports takes three times as many sockets from one
	// client address. Empty is the port in BaseURL and nothing else.
	Ports []int

	// Schema and Table are what the sockets subscribe to and the writer
	// writes. The table needs a shard column and a sent_us column, which
	// the scenario's setup sql creates.
	Schema string
	Table  string

	// WriteDSN is a host:port or a unix socket directory path for the
	// writer's own connections, over trust auth. Rows are how many to
	// commit per second and Batch how many go in one statement.
	WriteDSN  string
	WriteUser string
	WriteDB   string
	// WritePassword answers a cleartext ask, which is what a served
	// project's postgres door wants: the password there is the project's
	// postgres role key. Empty for a postmaster on trust.
	WritePassword string
	Writers       int
	Rows          int
	Batch         int

	// Warmup runs the whole workload and is thrown away, Duration is the
	// measured window, and Drain is how much longer the sockets are read
	// after the window closes so a row committed in the last instant of
	// it still has somewhere to land.
	Warmup   time.Duration
	Duration time.Duration
	Drain    time.Duration
	// Heartbeat is the cadence realtime-js keeps, and it is part of the
	// load: a hundred thousand clients on a thirty second heartbeat is
	// three thousand frames a second arriving whether or not anything
	// changed.
	Heartbeat time.Duration

	// OpsURL is the node's metrics endpoint, empty to skip the node's
	// own view.
	OpsURL string

	// Say is where progress goes, nil for quiet.
	Say func(format string, args ...any)
}

// Result is what a run publishes.
type Result struct {
	Held      int                `json:"held"`
	Wanted    int                `json:"wanted"`
	Refused   int                `json:"refused"`
	Lost      int                `json:"lost"`
	Failures  []string           `json:"failures,omitempty"`
	RampSecs  float64            `json:"ramp_secs"`
	Elapsed   float64            `json:"elapsed_secs"`
	ConnectMS map[string]float64 `json:"connect_ms"`
	JoinMS    map[string]float64 `json:"join_ms"`

	Rows      int64 `json:"rows_committed"`
	Expected  int64 `json:"deliveries_expected"`
	Delivered int64 `json:"deliveries_seen"`
	Missing   int64 `json:"deliveries_missing"`
	// Stray is deliveries that arrived for a row outside the measured
	// window, which are counted and dropped rather than silently mixed
	// into a percentile they do not belong to.
	Stray int64 `json:"deliveries_outside_window"`

	DeliveryMS map[string]float64 `json:"delivery_ms"`
	CommitMS   map[string]float64 `json:"commit_ms"`
	Timeline   []Second           `json:"timeline"`
	PerShard   ShardSpread        `json:"per_shard"`

	Node map[string]any `json:"node,omitempty"`
}

// Second is one wall clock second of the measured window.
type Second struct {
	T       int64   `json:"t"`
	N       int64   `json:"n"`
	MeanMS  float64 `json:"mean_ms"`
	WorstMS float64 `json:"worst_ms"`
}

// ShardSpread says whether the fan out was even, without printing a
// thousand shards into a result file. An uneven spread is a run where
// one subscription did the work, and the min tells that faster than a
// table would.
type ShardSpread struct {
	Shards   int     `json:"shards"`
	Sockets  int     `json:"sockets_per_shard"`
	MinSeen  int64   `json:"min_seen"`
	MaxSeen  int64   `json:"max_seen"`
	MeanSeen float64 `json:"mean_seen"`
	Empty    int     `json:"shards_with_no_delivery"`
}

// collectors is how many independent tally sheets the sockets share. A
// mutex per socket would be a hundred thousand mutexes, and one mutex
// would be a hundred thousand deliveries a second queueing behind each
// other, which is a measurement of this program rather than of the node.
const collectors = 64

type collector struct {
	mu      sync.Mutex
	hist    *Hist
	seconds map[int64]*Second
	shards  map[int]int64
	stray   int64
}

type run struct {
	opt Options
	// window is the measured span, in stamp microseconds. A delivery is
	// counted when the row's own stamp falls inside it, which is what
	// makes the drain honest.
	from, to atomic.Int64
	cols     [collectors]collector
	held     atomic.Int64
	lost     atomic.Int64
	stop     chan struct{}
	failures failures
}

type failures struct {
	mu   sync.Mutex
	seen map[string]int
	n    int
}

func (f *failures) note(err error) {
	if err == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.seen == nil {
		f.seen = map[string]int{}
	}
	f.n++
	// One line per distinct reason with a count. A run that refused
	// forty thousand sockets refused them for two or three reasons, and
	// forty thousand copies of the same sentence is not a report.
	f.seen[trimError(err.Error())]++
}

func trimError(s string) string {
	// Ephemeral port numbers and socket ids make every message unique,
	// which would defeat the grouping, so the address part goes.
	if i := strings.Index(s, "->"); i > 0 {
		if j := strings.Index(s[i:], ":"); j > 0 {
			return s[:strings.LastIndex(s[:i], " ")+1] + s[i+j+2:]
		}
	}
	return s
}

func (f *failures) list() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for reason, n := range f.seen {
		out = append(out, fmt.Sprintf("%d x %s", n, reason))
	}
	return out
}

// Run holds the sockets, commits the rows, and returns the numbers.
func Run(ctx context.Context, opt Options) (Result, error) {
	if opt.Sockets <= 0 {
		return Result{}, errors.New("a socket run needs sockets")
	}
	if opt.Shards <= 0 {
		opt.Shards = 1
	}
	if opt.Shards > opt.Sockets {
		return Result{}, fmt.Errorf("%d shards and only %d sockets, so some shard would have nobody on it", opt.Shards, opt.Sockets)
	}
	if opt.Dialers <= 0 {
		opt.Dialers = 64
	}
	if opt.ReadBuf <= 0 {
		opt.ReadBuf = 2048
	}
	if opt.Schema == "" {
		opt.Schema = "public"
	}
	if opt.Batch <= 0 {
		opt.Batch = 25
	}
	if opt.Writers <= 0 {
		opt.Writers = 4
	}
	if opt.Heartbeat <= 0 {
		opt.Heartbeat = 30 * time.Second
	}
	if opt.Drain <= 0 {
		opt.Drain = 5 * time.Second
	}
	if opt.Say == nil {
		opt.Say = func(string, ...any) {}
	}

	r := &run{opt: opt, stop: make(chan struct{})}
	for i := range r.cols {
		r.cols[i].hist = NewHist()
		r.cols[i].seconds = map[int64]*Second{}
		r.cols[i].shards = map[int]int64{}
	}

	result := Result{Wanted: opt.Sockets}
	connectMS, joinMS, err := r.ramp(ctx)
	if err != nil {
		r.close()
		return result, err
	}
	result.Held = int(r.held.Load())
	result.Refused = opt.Sockets - result.Held
	result.ConnectMS = latency.Percentiles(connectMS)
	result.JoinMS = latency.Percentiles(joinMS)
	result.RampSecs = round3(rampSecs(connectMS))
	if result.Held == 0 {
		r.close()
		result.Failures = r.failures.list()
		return result, errors.New("not one socket stayed up, so there is nothing to measure")
	}
	opt.Say("holding %d of %d sockets, %d refused", result.Held, opt.Sockets, result.Refused)

	// Only shards that ended up with a socket on them are written to. A
	// shard nobody joined would put rows through the tap that nothing
	// could receive, and the missing count would then be counting the
	// harness rather than the node.
	shards := r.shardsHeld()
	writer := &writer{opt: opt, shards: shards, run: r}
	if opt.Rows > 0 {
		if err := writer.open(); err != nil {
			r.close()
			return result, err
		}
		defer writer.close()
	}

	started := time.Now()
	if opt.Warmup > 0 {
		opt.Say("warmup, %s", opt.Warmup)
		r.window(0, 0)
		writer.push(ctx, opt.Warmup)
	}

	var before fleet.Metrics
	if opt.OpsURL != "" {
		if m, err := fleet.Scrape(opt.OpsURL); err == nil {
			before = m
		} else {
			opt.Say("the node's ops port did not answer: %v", err)
		}
	}

	opt.Say("measuring, %s at %d rows a second over %d shards", opt.Duration, opt.Rows, len(shards))
	from := time.Now()
	r.window(from.UnixMicro(), from.Add(opt.Duration).UnixMicro())
	writer.push(ctx, opt.Duration)
	// The window closes for the writer here and for the readers after
	// the drain, which is the whole point of having a drain.
	opt.Say("draining, %s", opt.Drain)
	select {
	case <-ctx.Done():
	case <-time.After(opt.Drain):
	}
	result.Elapsed = round3(time.Since(started).Seconds())

	var after fleet.Metrics
	if opt.OpsURL != "" && before != nil {
		if m, err := fleet.Scrape(opt.OpsURL); err == nil {
			after = m
		}
	}

	r.close()

	result.Rows = writer.rows.Load()
	result.CommitMS = latency.Percentiles(writer.samples())
	hist, seconds, perShard, stray := r.tally()
	result.Delivered = int64(hist.Count())
	result.Stray = stray
	result.DeliveryMS = hist.Percentiles()
	result.Timeline = seconds
	result.Lost = int(r.lost.Load())
	result.Failures = r.failures.list()

	perSocket := int64(result.Held / opt.Shards)
	expected := int64(0)
	for shard, rows := range writer.perShard() {
		expected += rows * r.socketsOn(shard, result.Held)
	}
	result.Expected = expected
	result.Missing = expected - result.Delivered
	result.PerShard = spread(perShard, len(shards), int(perSocket))

	if after != nil {
		result.Node = nodeView(before, after)
	}
	return result, ctx.Err()
}

func rampSecs(connect []latency.Sample) float64 {
	if len(connect) == 0 {
		return 0
	}
	first, last := connect[0].Epoch, connect[0].Epoch
	for _, s := range connect {
		if s.Epoch < first {
			first = s.Epoch
		}
		if s.Epoch > last {
			last = s.Epoch
		}
	}
	return float64(last - first)
}

// socketsOn is how many sockets a shard's rows were owed to. The sockets
// are handed shards round robin, so a shard's share is the division plus
// one for the remainder it may have caught.
func (r *run) socketsOn(shard, held int) int64 {
	n := int64(held / r.opt.Shards)
	if shard < held%r.opt.Shards {
		n++
	}
	return n
}

func (r *run) shardsHeld() []int {
	held := int(r.held.Load())
	if held > r.opt.Shards {
		held = r.opt.Shards
	}
	out := make([]int, 0, held)
	for i := range held {
		out = append(out, i)
	}
	return out
}

func (r *run) window(from, to int64) {
	r.from.Store(from)
	r.to.Store(to)
}

func (r *run) close() {
	select {
	case <-r.stop:
	default:
		close(r.stop)
	}
}

func (r *run) tally() (*Hist, []Second, map[int]int64, int64) {
	all := NewHist()
	seconds := map[int64]*Second{}
	shards := map[int]int64{}
	var stray int64
	for i := range r.cols {
		c := &r.cols[i]
		c.mu.Lock()
		all.Merge(c.hist)
		stray += c.stray
		for shard, n := range c.shards {
			shards[shard] += n
		}
		for t, s := range c.seconds {
			into, ok := seconds[t]
			if !ok {
				into = &Second{T: t}
				seconds[t] = into
			}
			into.N += s.N
			into.MeanMS += s.MeanMS // a sum until the end, see below
			if s.WorstMS > into.WorstMS {
				into.WorstMS = s.WorstMS
			}
		}
		c.mu.Unlock()
	}
	var times []int64
	for t := range seconds {
		times = append(times, t)
	}
	for i := 1; i < len(times); i++ {
		for j := i; j > 0 && times[j] < times[j-1]; j-- {
			times[j], times[j-1] = times[j-1], times[j]
		}
	}
	out := make([]Second, 0, len(times))
	for _, t := range times {
		s := seconds[t]
		if s.N > 0 {
			s.MeanMS = round3(s.MeanMS / float64(s.N))
		}
		out = append(out, *s)
	}
	// Relative seconds read better than epochs in a result file, and the
	// first measured second is second zero.
	if len(out) > 0 {
		base := out[0].T
		for i := range out {
			out[i].T -= base
		}
	}
	return all, out, shards, stray
}

func spread(seen map[int]int64, shards, perShard int) ShardSpread {
	out := ShardSpread{Shards: shards, Sockets: perShard, MinSeen: -1}
	var total int64
	for i := range shards {
		n := seen[i]
		total += n
		if n == 0 {
			out.Empty++
		}
		if out.MinSeen < 0 || n < out.MinSeen {
			out.MinSeen = n
		}
		if n > out.MaxSeen {
			out.MaxSeen = n
		}
	}
	if shards > 0 {
		out.MeanSeen = round3(float64(total) / float64(shards))
	}
	if out.MinSeen < 0 {
		out.MinSeen = 0
	}
	return out
}

// nodeView is the node's own reading of the same window. The counter
// delta is what it says it delivered, the gauges are what it says it was
// holding at the end, and the histogram is its own share of the latency,
// which is the half a client cannot see.
func nodeView(before, after fleet.Metrics) map[string]any {
	d := fleet.Delta(before, after)
	out := map[string]any{
		"changes_delivered": d["zou_realtime_changes_total"],
		"sockets":           after["zou_realtime_sockets"],
		"subscribers":       after["zou_realtime_subscribers"],
	}
	for _, name := range []string{"zou_realtime_change_seconds", "zou_realtime_commit_to_socket_seconds"} {
		buckets := fleet.Buckets(d, name)
		q := map[string]float64{}
		for label, p := range map[string]float64{"p50": 0.5, "p99": 0.99} {
			if v, ok := fleet.Quantile(buckets, p); ok {
				q[label+"_ms"] = round3(v * 1000)
			}
		}
		if count := d[name+"_count"]; count > 0 {
			q["mean_ms"] = round3(d[name+"_sum"] / count * 1000)
			q["count"] = count
		}
		if len(q) > 0 {
			out[strings.TrimPrefix(name, "zou_realtime_")] = q
		}
	}
	return out
}

// ramp opens every socket and joins a channel on it, at the requested
// rate, and returns once they are all up or refused.
func (r *run) ramp(ctx context.Context) ([]latency.Sample, []latency.Sample, error) {
	socketURLs, err := wsURLs(r.opt.BaseURL, r.opt.APIKey, r.opt.Ports)
	if err != nil {
		return nil, nil, err
	}
	locals, err := localAddrs(r.opt.Local)
	if err != nil {
		return nil, nil, err
	}

	var (
		mu      sync.Mutex
		connect = make([]latency.Sample, 0, r.opt.Sockets)
		joins   = make([]latency.Sample, 0, r.opt.Sockets)
	)
	ids := make(chan int, r.opt.Dialers)
	var wg sync.WaitGroup
	for w := range r.opt.Dialers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for id := range ids {
				// Source address and destination port together are what
				// decide how many sockets fit, so the id walks the
				// addresses and carries into the ports rather than both
				// at once, which on two lists of the same length would
				// only ever pair them up one way.
				var local *net.TCPAddr
				spread := 1
				if len(locals) > 0 {
					local = locals[id%len(locals)]
					spread = len(locals)
				}
				opened := time.Now()
				conn, err := Dial(socketURLs[(id/spread)%len(socketURLs)], local, 30*time.Second, r.opt.ReadBuf)
				if err != nil {
					r.failures.note(err)
					continue
				}
				dialed := time.Now()
				shard := id % r.opt.Shards
				if err := r.join(conn, shard); err != nil {
					r.failures.note(err)
					conn.Close()
					continue
				}
				joined := time.Now()
				mu.Lock()
				connect = append(connect, latency.Sample{MS: msSince(opened, dialed), Epoch: opened.Unix()})
				joins = append(joins, latency.Sample{MS: msSince(dialed, joined), Epoch: dialed.Unix()})
				mu.Unlock()
				r.held.Add(1)
				go r.read(conn, shard, id)
				if r.opt.Heartbeat > 0 {
					go r.beat(conn, id)
				}
			}
		}(w)
	}

	// The pacing is here rather than in the workers so the rate is the
	// run's and not the rate times the worker count.
	var tick <-chan time.Time
	if r.opt.ConnectRate > 0 {
		every := time.Second / time.Duration(r.opt.ConnectRate)
		if every < time.Microsecond {
			every = time.Microsecond
		}
		t := time.NewTicker(every)
		defer t.Stop()
		tick = t.C
	}
	begun := time.Now()
	for id := range r.opt.Sockets {
		if tick != nil {
			select {
			case <-tick:
			case <-ctx.Done():
			}
		}
		if ctx.Err() != nil {
			break
		}
		ids <- id
		if id > 0 && id%10000 == 0 {
			r.opt.Say("%d sockets in, %d up, %s elapsed", id, r.held.Load(), time.Since(begun).Round(time.Second))
		}
	}
	close(ids)
	wg.Wait()
	return connect, joins, ctx.Err()
}

func msSince(from, to time.Time) float64 {
	return round3(float64(to.Sub(from).Microseconds()) / 1000)
}

// wsURLs is the endpoint once per port the node answers on.
//
// One client address has about 64k ports to any one destination port, so
// a run holding more sockets than that has to spread them over more than
// one port on the node, which `zou serve --http a,b,c` is for. The url
// carries the first port and this list is the rest of them.
func wsURLs(base, apikey string, ports []int) ([]string, error) {
	first, err := wsURL(base, apikey)
	if err != nil {
		return nil, err
	}
	if len(ports) == 0 {
		return []string{first}, nil
	}
	u, err := url.Parse(first)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(ports))
	for _, port := range ports {
		if port <= 0 || port > 65535 {
			return nil, fmt.Errorf("%d is not a port a socket can be opened to", port)
		}
		u.Host = net.JoinHostPort(u.Hostname(), strconv.Itoa(port))
		out = append(out, u.String())
	}
	return out, nil
}

// wsURL turns a project base url into the realtime endpoint. Either
// scheme in, ws scheme out, because a caller has the api base on hand
// and should not have to know the path.
func wsURL(base, apikey string) (string, error) {
	if base == "" {
		return "", errors.New("a socket run needs a base url")
	}
	u, err := url.Parse(strings.TrimSuffix(base, "/"))
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "http", "ws":
		u.Scheme = "ws"
	case "https", "wss":
		return "", errors.New("this driver speaks plain websocket only, so a run needs the node's http port")
	default:
		return "", fmt.Errorf("%s is not a url this driver can open", base)
	}
	if !strings.HasSuffix(u.Path, "/realtime/v1/websocket") {
		u.Path += "/realtime/v1/websocket"
	}
	q := u.Query()
	q.Set("vsn", "2.0.0")
	if apikey != "" {
		q.Set("apikey", apikey)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func localAddrs(list []string) ([]*net.TCPAddr, error) {
	var out []*net.TCPAddr
	for _, host := range list {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}
		addr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(host, "0"))
		if err != nil {
			return nil, fmt.Errorf("local address %s: %w", host, err)
		}
		out = append(out, addr)
	}
	return out, nil
}

// join asks for one shard's changes and waits for the reply, which is
// the frame that carries the subscription ids and therefore the frame
// that says the node is actually reading the database for this socket.
func (r *run) join(conn *Conn, shard int) error {
	config := map[string]any{
		"config": map[string]any{
			"postgres_changes": []map[string]any{{
				"event":  "INSERT",
				"schema": r.opt.Schema,
				"table":  r.opt.Table,
				"filter": fmt.Sprintf("shard=eq.%d", shard),
			}},
		},
	}
	if r.opt.Token != "" {
		config["access_token"] = r.opt.Token
	}
	payload, err := json.Marshal(config)
	if err != nil {
		return err
	}
	topic := r.topic(shard)
	frame := fmt.Sprintf(`["1","1",%q,"phx_join",%s]`, topic, payload)
	if err := conn.WriteText([]byte(frame)); err != nil {
		return err
	}
	for {
		msg, err := conn.Read()
		if err != nil {
			return err
		}
		event, payload := eventOf(msg)
		switch event {
		case "phx_reply":
			if status, _ := payload["status"].(string); status != "ok" {
				return fmt.Errorf("join refused: %s", compact(payload))
			}
			return nil
		case "phx_error":
			return fmt.Errorf("join failed: %s", compact(payload))
		}
		// Anything else on the way is the server's business, not the
		// join's, and the reply is what is being waited for.
	}
}

func (r *run) topic(shard int) string {
	return fmt.Sprintf("realtime:shard-%d", shard)
}

// read is one socket's whole life after its join: count what arrives,
// and say so if it stops arriving because the socket went away.
func (r *run) read(conn *Conn, shard, id int) {
	defer conn.Close()
	col := &r.cols[id%collectors]
	for {
		msg, err := conn.Read()
		if err != nil {
			select {
			case <-r.stop:
			default:
				r.lost.Add(1)
				r.held.Add(-1)
				r.failures.note(fmt.Errorf("socket dropped mid run: %w", err))
			}
			return
		}
		event, payload := eventOf(msg)
		if event != "postgres_changes" {
			continue
		}
		stamp, ok := sentUS(payload)
		if !ok {
			continue
		}
		now := time.Now()
		from, to := r.from.Load(), r.to.Load()
		col.mu.Lock()
		switch {
		case from == 0 || stamp < from || stamp > to:
			col.stray++
		default:
			us := now.UnixMicro() - stamp
			col.hist.Add(us)
			col.shards[shard]++
			second := (stamp - from) / 1_000_000
			s, ok := col.seconds[second]
			if !ok {
				s = &Second{T: second}
				col.seconds[second] = s
			}
			s.N++
			latencyMS := float64(us) / 1000
			s.MeanMS += latencyMS
			if latencyMS > s.WorstMS {
				s.WorstMS = latencyMS
			}
		}
		col.mu.Unlock()
	}
}

// beat keeps the heartbeat realtime-js keeps. The reply lands in the
// read loop with an event nothing counts, which is where it belongs.
func (r *run) beat(conn *Conn, id int) {
	// The phase is spread across the interval so a hundred thousand
	// clients do not all beat in the same millisecond, which is what a
	// real fleet looks like and also what keeps the generator from
	// making its own spike.
	stagger := time.Duration(id%1000) * r.opt.Heartbeat / 1000
	select {
	case <-time.After(stagger):
	case <-r.stop:
		return
	}
	ticker := time.NewTicker(r.opt.Heartbeat)
	defer ticker.Stop()
	ref := 0
	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
			ref++
			frame := fmt.Sprintf(`[null,"%d","phoenix","heartbeat",{}]`, ref)
			if err := conn.WriteText([]byte(frame)); err != nil {
				return
			}
		}
	}
}

// eventOf reads the event name and payload out of a v2 frame, which is
// the array form: join ref, ref, topic, event, payload.
func eventOf(msg []byte) (string, map[string]any) {
	var frame []json.RawMessage
	if err := json.Unmarshal(msg, &frame); err != nil || len(frame) < 5 {
		return "", nil
	}
	var event string
	if err := json.Unmarshal(frame[3], &event); err != nil {
		return "", nil
	}
	var payload map[string]any
	if err := json.Unmarshal(frame[4], &payload); err != nil {
		return event, nil
	}
	return event, payload
}

// sentUS digs the stamp out of a change payload. The record's values
// arrive as strings for a bigint, which is postgres being careful about
// what a json number can hold, so both forms are read.
func sentUS(payload map[string]any) (int64, bool) {
	data, ok := payload["data"].(map[string]any)
	if !ok {
		return 0, false
	}
	record, ok := data["record"].(map[string]any)
	if !ok {
		return 0, false
	}
	switch v := record["sent_us"].(type) {
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		return n, err == nil
	case float64:
		return int64(v), true
	case json.Number:
		n, err := v.Int64()
		return n, err == nil
	}
	return 0, false
}

func compact(payload map[string]any) string {
	blob, err := json.Marshal(payload)
	if err != nil {
		return "unreadable payload"
	}
	if len(blob) > 200 {
		return string(blob[:200])
	}
	return string(blob)
}
