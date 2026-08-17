package sockets

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tamnd/zou-bench/latency"
	"github.com/tamnd/zou-bench/pgwire"
)

// writer commits the rows the sockets are waiting for.
//
// Each row carries the microsecond it was stamped at, on this process's
// clock, and this process is also the one that reads the frame, so the
// delivery latency has one clock in it and no ntp skew. The stamp goes
// on before the insert is sent, which makes the latency a ceiling: it
// contains the round trip to the database and the commit as well as the
// node's own work. commit_ms is that head published separately, and the
// node's commit_to_socket histogram is the same span read from the
// database's commit timestamp instead, so all three are in the result and
// nobody has to take one of them on faith.
type writer struct {
	opt    Options
	shards []int
	run    *run

	rows  atomic.Int64
	conns []*pgwire.Conn

	mu     sync.Mutex
	sample []latency.Sample
	shard  map[int]int64
}

func (w *writer) open() error {
	w.shard = map[int]int64{}
	if w.opt.WriteDSN == "" {
		return fmt.Errorf("a run that commits %d rows a second needs a write dsn", w.opt.Rows)
	}
	user, db := w.opt.WriteUser, w.opt.WriteDB
	if user == "" {
		user = "postgres"
	}
	if db == "" {
		db = "postgres"
	}
	for range w.opt.Writers {
		conn, err := pgwire.DialPassword(w.opt.WriteDSN, user, db, w.opt.WritePassword)
		if err != nil {
			w.close()
			return fmt.Errorf("writer connection: %w", err)
		}
		w.conns = append(w.conns, conn)
	}
	return nil
}

func (w *writer) close() {
	for _, conn := range w.conns {
		conn.Close()
	}
	w.conns = nil
}

// push runs the write load for a span and returns when the span is over.
// A statement in flight when the span ends is finished rather than cut,
// because a half sent insert would leave the accounting owing rows that
// were never committed.
func (w *writer) push(ctx context.Context, span time.Duration) {
	if w.opt.Rows <= 0 || len(w.conns) == 0 {
		select {
		case <-ctx.Done():
		case <-time.After(span):
		}
		return
	}
	deadline := time.Now().Add(span)
	var wg sync.WaitGroup
	for i, conn := range w.conns {
		wg.Add(1)
		go func(i int, conn *pgwire.Conn) {
			defer wg.Done()
			w.pump(ctx, i, conn, deadline)
		}(i, conn)
	}
	wg.Wait()
}

func (w *writer) pump(ctx context.Context, index int, conn *pgwire.Conn, deadline time.Time) {
	// Rows a second is the run's number, so each writer takes its share
	// of it and the batch size decides how often it says anything.
	share := w.opt.Rows / len(w.conns)
	if index < w.opt.Rows%len(w.conns) {
		share++
	}
	if share <= 0 {
		return
	}
	batch := w.opt.Batch
	if batch > share {
		batch = share
	}
	every := time.Duration(float64(time.Second) * float64(batch) / float64(share))
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	// Every writer starts on the same shard offset it will keep walking,
	// so the shards fill evenly across a second rather than all the rows
	// of one second landing on one subscription.
	next := index * batch
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if time.Now().After(deadline) {
			return
		}
		var (
			sql    strings.Builder
			shards = make([]int, 0, batch)
		)
		sql.WriteString("insert into ")
		sql.WriteString(w.opt.Schema)
		sql.WriteString(".")
		sql.WriteString(w.opt.Table)
		sql.WriteString(" (shard, sent_us) values ")
		stamp := time.Now()
		for j := range batch {
			shard := w.shards[(next+j)%len(w.shards)]
			shards = append(shards, shard)
			if j > 0 {
				sql.WriteString(",")
			}
			fmt.Fprintf(&sql, "(%d,%d)", shard, stamp.UnixMicro())
		}
		next += batch
		sent := time.Now()
		if _, err := conn.Query(sql.String()); err != nil {
			w.run.failures.note(fmt.Errorf("insert: %w", err))
			// A writer whose connection is gone is done: reconnecting
			// mid window would hide a database that fell over.
			return
		}
		done := time.Now()
		// Counted only when the stamp is inside the measured window, so
		// warmup rows are written and owed to nobody.
		from, to := w.run.from.Load(), w.run.to.Load()
		if from != 0 && stamp.UnixMicro() >= from && stamp.UnixMicro() <= to {
			w.rows.Add(int64(len(shards)))
			w.mu.Lock()
			for _, shard := range shards {
				w.shard[shard]++
			}
			w.sample = append(w.sample, latency.Sample{MS: msSince(sent, done), Epoch: sent.Unix()})
			w.mu.Unlock()
		}
	}
}

func (w *writer) samples() []latency.Sample {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]latency.Sample(nil), w.sample...)
}

func (w *writer) perShard() map[int]int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := map[int]int64{}
	for shard, n := range w.shard {
		out[shard] = n
	}
	return out
}
