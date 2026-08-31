// Package tpcb runs pgbench's tpcb-like transaction from Go so that
// every statement inside it is timed on its own.
//
// pgbench can report per statement latencies with -r, but only as
// means, and it logs per transaction totals with -l, which is where
// every percentile in this book comes from. Neither of those answers a
// question about one statement's tail: the accounts update at scale 100
// with eight writers has a 0.352 ms mean and the transaction it sits in
// has a 36.8 ms p99, and a bound on the statement cannot be read off
// either number. pgbench has no flag that would give it, so the choice
// is between changing the shape, by running the statement as its own
// transaction, and driving the same shape from here.
//
// This drives the same shape. The five statements are pgbench's own,
// in pgbench's order, with pgbench's variable draws, sent as simple
// queries over the same wire protocol pgbench uses by default, one
// connection per client, no prepared statements and no pipelining. A
// run through here should reach the same rate as a pgbench run of the
// same scenario, and when it does not, the difference is the harness
// and the numbers say so rather than hiding it.
package tpcb

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/tamnd/zou-bench/latency"
	"github.com/tamnd/zou-bench/pgwire"
)

// Statements are the seven round trips of one tpcb-like transaction,
// named the way the tables read. BEGIN and END are timed too: END is
// where the commit lands, and on a store backed server that is the
// statement the freshness barrier and the wal push are paid in, so
// leaving it out would drop the most interesting line in the run.
var Statements = []string{
	"begin",
	"accounts update",
	"accounts select",
	"tellers update",
	"branches update",
	"history insert",
	"end",
}

// Txn is one completed transaction: its total and the seven statement
// times inside it, in Statements order.
type Txn struct {
	LatencyMS float64
	Epoch     int64
	StmtMS    [7]float64
}

// Run drives clients connections against addr for d, each looping the
// tpcb-like transaction with variables drawn the way pgbench draws
// them at this scale. It returns every transaction that committed.
//
// A statement that errors ends that transaction and the client rolls
// back and carries on, and the transaction is counted as failed rather
// than timed, which is what pgbench does with a serialization failure
// it cannot retry. A connection that dies takes its client with it,
// because a run whose clients quietly halve is a run reporting a rate
// nobody asked for.
func Run(addr, user, database string, scale, clients int, d time.Duration) ([]Txn, int, error) {
	if clients < 1 {
		return nil, 0, fmt.Errorf("tpcb: %d clients", clients)
	}
	conns := make([]*pgwire.Conn, clients)
	for i := range conns {
		c, err := pgwire.Dial(addr, user, database)
		if err != nil {
			for _, open := range conns[:i] {
				open.Close()
			}
			return nil, 0, fmt.Errorf("tpcb: client %d: %w", i, err)
		}
		conns[i] = c
	}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()

	// Every client is given its own generator seeded from its index, so
	// a run is reproducible in shape without every client drawing the
	// same rows, which would turn scale 100 into scale 1.
	var wg sync.WaitGroup
	out := make([][]Txn, clients)
	failed := make([]int, clients)
	deadline := time.Now().Add(d)
	for i := range conns {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out[i], failed[i] = loop(conns[i], rand.New(rand.NewSource(int64(i)+1)), scale, deadline)
		}(i)
	}
	wg.Wait()

	var txns []Txn
	total := 0
	for i := range out {
		txns = append(txns, out[i]...)
		total += failed[i]
	}
	return txns, total, nil
}

// loop is one client until the deadline.
func loop(c *pgwire.Conn, rng *rand.Rand, scale int, deadline time.Time) ([]Txn, int) {
	var txns []Txn
	failed := 0
	for time.Now().Before(deadline) {
		aid := rng.Intn(100000*scale) + 1
		bid := rng.Intn(scale) + 1
		tid := rng.Intn(10*scale) + 1
		delta := rng.Intn(10001) - 5000
		sql := [7]string{
			"BEGIN",
			fmt.Sprintf("UPDATE pgbench_accounts SET abalance = abalance + %d WHERE aid = %d", delta, aid),
			fmt.Sprintf("SELECT abalance FROM pgbench_accounts WHERE aid = %d", aid),
			fmt.Sprintf("UPDATE pgbench_tellers SET tbalance = tbalance + %d WHERE tid = %d", delta, tid),
			fmt.Sprintf("UPDATE pgbench_branches SET bbalance = bbalance + %d WHERE bid = %d", delta, bid),
			fmt.Sprintf("INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (%d, %d, %d, %d, CURRENT_TIMESTAMP)", tid, bid, aid, delta),
			"END",
		}
		var txn Txn
		start := time.Now()
		broke := false
		for k, s := range sql {
			at := time.Now()
			if _, err := c.Query(s); err != nil {
				broke = true
				break
			}
			txn.StmtMS[k] = float64(time.Since(at).Nanoseconds()) / 1e6
		}
		if broke {
			failed++
			// The transaction is open and the connection is fine, since
			// Query only returns after ReadyForQuery. Roll back so the
			// next round starts where the last one did, and give up on
			// the client if even that does not answer.
			if _, err := c.Query("ROLLBACK"); err != nil {
				return txns, failed
			}
			continue
		}
		done := time.Now()
		txn.LatencyMS = float64(done.Sub(start).Nanoseconds()) / 1e6
		txn.Epoch = done.Unix()
		txns = append(txns, txn)
	}
	return txns, failed
}

// Percentiles returns the distribution of each statement, keyed by the
// names in Statements, beside the distribution of the whole
// transaction under "transaction". Every entry is the same shape the
// rest of the book's latency tables are, so a statement line and a
// transaction line can be read against each other.
func Percentiles(txns []Txn) map[string]map[string]float64 {
	out := map[string]map[string]float64{}
	if len(txns) == 0 {
		return out
	}
	whole := make([]latency.Sample, len(txns))
	for i, t := range txns {
		whole[i] = latency.Sample{MS: t.LatencyMS, Epoch: t.Epoch}
	}
	out["transaction"] = latency.Percentiles(whole)
	for k, name := range Statements {
		s := make([]latency.Sample, len(txns))
		for i, t := range txns {
			s[i] = latency.Sample{MS: t.StmtMS[k], Epoch: t.Epoch}
		}
		out[name] = latency.Percentiles(s)
	}
	return out
}
