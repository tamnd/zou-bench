package tpcb

import (
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// A fake backend that answers every simple query, so the driver can be
// exercised without a postgres. It speaks the same four messages the
// client reads: an auth ok, a ready, a command complete and a ready.
func fakeServer(t *testing.T, answer func(sql string) []byte) (string, func() []string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "tpcb")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, ".s.PGSQL.5432")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	var mu sync.Mutex
	var seen []string
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				if _, err := readBody(c, 4); err != nil {
					return
				}
				c.Write(append(msg('R', 0, 0, 0, 0), msg('Z', 'I')...))
				for {
					var kind [1]byte
					if _, err := io.ReadFull(c, kind[:]); err != nil {
						return
					}
					body, err := readBody(c, 4)
					if err != nil {
						return
					}
					sql := strings.TrimRight(string(body), "\x00")
					mu.Lock()
					seen = append(seen, sql)
					mu.Unlock()
					if _, err := c.Write(answer(sql)); err != nil {
						return
					}
				}
			}(c)
		}
	}()
	return sock, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
}

func msg(kind byte, payload ...byte) []byte {
	out := make([]byte, 5+len(payload))
	out[0] = kind
	binary.BigEndian.PutUint32(out[1:], uint32(4+len(payload)))
	copy(out[5:], payload)
	return out
}

// readBody reads a length prefixed body, the prefix counting itself.
func readBody(c net.Conn, prefix int) ([]byte, error) {
	head := make([]byte, prefix)
	if _, err := io.ReadFull(c, head); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint32(head)) - prefix
	body := make([]byte, n)
	_, err := io.ReadFull(c, body)
	return body, err
}

func ok() []byte { return append(msg('C', 'O', 'K', 0), msg('Z', 'I')...) }

func errorAt(code, message string) []byte {
	payload := append([]byte{'C'}, code...)
	payload = append(payload, 0, 'M')
	payload = append(payload, message...)
	payload = append(payload, 0, 0)
	return append(msg('E', payload...), msg('Z', 'E')...)
}

func TestOneTransactionIsSevenStatementsInPgbenchsOrder(t *testing.T) {
	sock, seen := fakeServer(t, func(string) []byte { return ok() })
	txns, failed, err := Run(sock, "bench", "postgres", 10, 1, 40*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if failed != 0 {
		t.Fatalf("%d failed against a server that answers everything", failed)
	}
	if len(txns) == 0 {
		t.Fatal("no transaction committed in 40 ms")
	}
	got := seen()
	if len(got) < 7 {
		t.Fatalf("only %d statements sent", len(got))
	}
	want := []string{"BEGIN", "UPDATE pgbench_accounts", "SELECT abalance",
		"UPDATE pgbench_tellers", "UPDATE pgbench_branches",
		"INSERT INTO pgbench_history", "END"}
	for i, prefix := range want {
		if !strings.HasPrefix(got[i], prefix) {
			t.Errorf("statement %d is %q, wanted one starting %q", i, got[i], prefix)
		}
	}
	// The seventh is where the next transaction starts, which is what
	// makes this a loop rather than a single pass.
	if len(got) > 7 && got[7] != "BEGIN" {
		t.Errorf("the eighth statement is %q rather than the next BEGIN", got[7])
	}
}

func TestTheVariablesAreDrawnInsidePgbenchsRanges(t *testing.T) {
	const scale = 3
	sock, seen := fakeServer(t, func(string) []byte { return ok() })
	if _, _, err := Run(sock, "bench", "postgres", scale, 2, 60*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	branches := 0
	for _, sql := range seen() {
		if !strings.HasPrefix(sql, "UPDATE pgbench_branches") {
			continue
		}
		branches++
		// bid is drawn from 1 to the scale, which is the row count of
		// pgbench_branches. A draw outside it updates nothing and the
		// run reads as a rate against a table it never touched.
		bid := sql[strings.LastIndex(sql, "= ")+2:]
		switch bid {
		case "1", "2", "3":
		default:
			t.Fatalf("bid %s outside 1 to %d", bid, scale)
		}
	}
	if branches == 0 {
		t.Fatal("no branch update sent")
	}
}

func TestAFailedStatementRollsBackAndIsNotTimed(t *testing.T) {
	var mu sync.Mutex
	first := true
	sock, seen := fakeServer(t, func(sql string) []byte {
		if strings.HasPrefix(sql, "UPDATE pgbench_tellers") {
			mu.Lock()
			defer mu.Unlock()
			if first {
				first = false
				return errorAt("40001", "could not serialize access")
			}
		}
		return ok()
	})
	txns, failed, err := Run(sock, "bench", "postgres", 10, 1, 40*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if failed != 1 {
		t.Fatalf("%d failed, wanted the one the server refused", failed)
	}
	rollbacks := 0
	for _, sql := range seen() {
		if sql == "ROLLBACK" {
			rollbacks++
		}
	}
	if rollbacks != 1 {
		t.Fatalf("%d rollbacks after one refusal", rollbacks)
	}
	// The refused transaction is not in the timings, because a
	// transaction that stopped a third of the way through is not a
	// sample of what a transaction costs.
	for _, tx := range txns {
		if tx.StmtMS[4] == 0 && tx.StmtMS[6] == 0 {
			t.Fatal("a transaction that never reached END was timed")
		}
	}
}

func TestPercentilesNameEveryStatementAndTheWhole(t *testing.T) {
	sock, _ := fakeServer(t, func(string) []byte { return ok() })
	txns, _, err := Run(sock, "bench", "postgres", 10, 1, 40*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	p := Percentiles(txns)
	for _, name := range append([]string{"transaction"}, Statements...) {
		if _, ok := p[name]; !ok {
			t.Errorf("no distribution for %q", name)
		}
	}
	if len(p["accounts update"]) == 0 || p["accounts update"]["p99"] < 0 {
		t.Error("the accounts update has no p99, which is the line this driver exists for")
	}
}

func TestNothingMeasuredIsNotZeroMeasured(t *testing.T) {
	if got := Percentiles(nil); len(got) != 0 {
		t.Fatalf("an empty run gave %v rather than nothing", got)
	}
}

func TestAServerThatWillNotTakeAClientIsAnError(t *testing.T) {
	if _, _, err := Run("/nonexistent/.s.PGSQL.5432", "bench", "postgres", 10, 1, time.Millisecond); err == nil {
		t.Fatal("dialing nothing succeeded")
	}
}
