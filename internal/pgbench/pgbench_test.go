package pgbench

import (
	"os"
	"path/filepath"
	"testing"
)

const summary = `pgbench (18.4)
transaction type: <builtin: TPC-B (sort of)>
scaling factor: 100
number of clients: 8
number of threads: 4
number of transactions actually processed: 10543
number of failed transactions: 0 (0.000%)
latency average = 91.057 ms
latency stddev = 184.496 ms
initial connection time = 82.138 ms
tps = 87.788855 (without initial connection time)
statement latencies in milliseconds and failures:
         0.567           0  BEGIN;
        45.123           0  UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
`

func TestParseSummaryReadsTheHeadlineNumbersAndStatements(t *testing.T) {
	result := map[string]any{}
	ParseSummary(summary, result)
	if result["tps"].(float64) != 87.788855 {
		t.Fatalf("tps = %v", result["tps"])
	}
	if result["latency_stddev_ms"].(float64) != 184.496 {
		t.Fatalf("stddev = %v", result["latency_stddev_ms"])
	}
	if result["transactions"].(int) != 10543 {
		t.Fatalf("transactions = %v", result["transactions"])
	}
	stmts := result["statement_latency_ms"].(map[string]float64)
	if stmts["BEGIN;"] != 0.567 {
		t.Fatalf("statements = %v", stmts)
	}
}

func TestParseInitPhasesSplitsTheDoneLine(t *testing.T) {
	text := "done in 3621.55 s (drop tables 0.00 s, create tables 0.02 s, client-side generate 1200.31 s, vacuum 800.10 s, primary keys 1621.12 s)."
	total, phases := ParseInitPhases(text)
	if total != 3621.55 {
		t.Fatalf("total = %v", total)
	}
	if phases["vacuum"] != 800.10 || phases["primary keys"] != 1621.12 {
		t.Fatalf("phases = %v", phases)
	}
}

func TestTxnLogsBecomePercentilesAndBuckets(t *testing.T) {
	dir := t.TempDir()
	// client txn latency_us script epoch epoch_us
	log := ""
	for i := 0; i < 100; i++ {
		epoch := int64(1700000000 + i/10) // ten txns per second
		log += "0 1 " + itoa(1000*(i+1)) + " 0 " + itoa64(epoch) + " 0\n"
	}
	if err := os.WriteFile(filepath.Join(dir, "pgbench_log.1"), []byte(log), 0o644); err != nil {
		t.Fatal(err)
	}
	txns := ParseTxnLogs(dir)
	if len(txns) != 100 {
		t.Fatalf("parsed %d txns", len(txns))
	}
	p := Percentiles(txns)
	if p["p50"] != 51 || p["max"] != 100 {
		t.Fatalf("percentiles = %v", p)
	}
	if p["p999"] != 100 {
		t.Fatalf("p999 = %v", p["p999"])
	}
	buckets := Buckets(txns, 5)
	if len(buckets) != 2 {
		t.Fatalf("buckets = %v", buckets)
	}
	if buckets[0].Txns != 50 || buckets[0].TPS != 10 {
		t.Fatalf("first bucket = %+v", buckets[0])
	}
	if buckets[1].P99 <= buckets[0].P99 {
		t.Fatalf("later latencies should dominate: %+v", buckets)
	}
}

func TestDSNArgsMapsFieldsAndDefaultsTheDatabase(t *testing.T) {
	args := DSNArgs("host=10.0.0.1 port=5490 user=zou")
	want := []string{"-h", "10.0.0.1", "-p", "5490", "-U", "zou", "postgres"}
	if len(args) != len(want) {
		t.Fatalf("args = %v", args)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Fatalf("args = %v", args)
		}
	}
}

func itoa(n int) string { return itoa64(int64(n)) }
func itoa64(n int64) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}
