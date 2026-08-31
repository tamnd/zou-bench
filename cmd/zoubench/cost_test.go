package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/tamnd/zou-bench/cost"
)

// A result file, as the run command writes one, cut down to the parts a
// price depends on. Written as json text and parsed rather than built as
// a map, because the thing under test is reading a file back and every
// number in one arrives as a float64 no matter what was written.
const recordedRun = `{
  "scenario": "tpcb-scale100",
  "label": "zou-minio",
  "date": "2026-08-31T09:12:00Z",
  "transactions": 50000,
  "pg_delta": {"database": {"xact_commit": 50120}},
  "store": {"bytes_after": 10737418240, "bytes_delta": 1073741824},
  "store_ops": {
    "ops": {
      "get":          {"count": 900000, "bytes": 7340032000},
      "get_range":    {"count": 100000, "bytes": 819200000},
      "put":          {"count": 400000, "bytes": 3276800000},
      "put_if_match": {"count": 100000, "bytes": 819200000},
      "delete":       {"count": 20000,  "bytes": 0},
      "list":         {"count": 500000, "bytes": 0}
    }
  }
}`

func parse(t *testing.T, text string) map[string]any {
	t.Helper()
	var r map[string]any
	if err := json.Unmarshal([]byte(text), &r); err != nil {
		t.Fatal(err)
	}
	return r
}

// The grouping a bill uses is not the grouping the op names suggest, and
// getting it wrong moves the biggest line on most cards: a range get is
// a read, a conditional put is a write, and a list is billed with the
// writes even though everybody thinks of it as a read.
func TestARecordedRunIsRebuiltWithTheGroupingABillUses(t *testing.T) {
	u, ok := runUsage(parse(t, recordedRun), false)
	if !ok {
		t.Fatal("a run with a million gets in it read as unmeasured")
	}
	if u.Gets != 1000000 || u.Puts != 500000 {
		t.Fatalf("gets %d puts %d", u.Gets, u.Puts)
	}
	if u.Lists != 500000 || u.Deletes != 20000 {
		t.Fatalf("lists %d deletes %d", u.Lists, u.Deletes)
	}
	if u.DownloadBytes != 8159232000 || u.UploadBytes != 4096000000 {
		t.Fatalf("down %d up %d", u.DownloadBytes, u.UploadBytes)
	}
	if u.StorageBytes != 10737418240 || u.IngestedBytes != 1073741824 {
		t.Fatalf("stored %d ingested %d", u.StorageBytes, u.IngestedBytes)
	}
	// Two divisors, close together and not the same, and the file says
	// which is which so the table can too.
	if u.Txns != 50000 || u.Commits != 50120 {
		t.Fatalf("txns %d commits %d", u.Txns, u.Commits)
	}
}

// A store that shrank over the window folded more than it wrote. That is
// a real thing for this engine to do and it is not an ingest of a
// negative number of bytes, which would come back out of the arithmetic
// as a negative dollars per GB.
func TestAStoreThatShrankIngestedNothing(t *testing.T) {
	r := parse(t, recordedRun)
	r["store"].(map[string]any)["bytes_delta"] = float64(-2048)
	u, _ := runUsage(r, false)
	if u.IngestedBytes != 0 {
		t.Fatalf("ingested %d", u.IngestedBytes)
	}
	if got := cost.Compute(cost.Card{Name: "c", GBBytes: 1 << 30, ClassAPerMillion: 5}, u); got["usd_per_gb_ingested"] != nil {
		t.Fatalf("priced a GB that was never ingested: %v", got["usd_per_gb_ingested"])
	}
}

// A result with no counters in it is the ordinary state of a run against
// a store that does not report any, and pricing that at zero would put a
// free row in a table of dollars.
func TestAResultWithNoCountersIsNotPricedAtZero(t *testing.T) {
	if _, ok := runUsage(parse(t, `{"label": "pg18", "transactions": 5}`), false); ok {
		t.Fatal("a run with no store counters read as measured")
	}
}

// The fleet shape, cut down the same way. The two rates are per hour and
// per project hour, and the second is the one that multiplies by the
// ceiling rather than by the fleet.
const recordedFleet = `{
  "scenario": "fleet-800-idle",
  "label": "zou-fleet",
  "date": "2026-08-11T23:20:22Z",
  "tenants_ready": 800,
  "node": {"max_attached": 100},
  "store": {"bytes": 34359738368, "objects": 120000},
  "hold": {
    "rates": {
      "saw_dormant": true,
      "dormant_ops_per_hour":          {"puts": 0, "gets": 0, "lists": 0, "deletes": 0},
      "attached_ops_per_project_hour": {"puts": 742.9, "gets": 762.9, "lists": 0, "deletes": 56}
    }
  }
}`

func TestAFleetResultIsRepricedFromItsHoldWindow(t *testing.T) {
	tail, ok := fleetTail(parse(t, recordedFleet), 0, "")
	if !ok {
		t.Fatal("a fleet result read as something else")
	}
	if tail.Projects != 800 || tail.AttachedAtOnce != 100 {
		t.Fatalf("projects %d attached %d", tail.Projects, tail.AttachedAtOnce)
	}
	if tail.StorageBytes != 34359738368 {
		t.Fatalf("stored %d", tail.StorageBytes)
	}
	if tail.AttachedPerProjectHour.Puts != 742.9 || tail.AttachedPerProjectHour.Gets != 762.9 {
		t.Fatalf("rate %+v", tail.AttachedPerProjectHour)
	}
	// The hundred held projects at 742.9 puts an hour each, over 730
	// hours, at five dollars a million, is the class A line and it is
	// the one that dominates this scenario.
	card := cost.Card{Name: "c", AsOf: "2026-08-29", GBBytes: 1 << 30, ClassAPerMillion: 5, ClassBPerMillion: 0.4}
	block := cost.Monthly(card, tail)
	want := (742.9*100*730/1e6)*5 + (762.9*100*730/1e6)*0.4
	if got := block["ops_usd_month"].(float64); got < want-0.01 || got > want+0.01 {
		t.Fatalf("ops = %v, want about %v", got, want)
	}
	if block["usd_per_project_month"] == nil {
		t.Fatal("no per project line on a fleet of eight hundred")
	}
}

// The two shapes have to be told apart by what is in them rather than by
// a name, since both carry a scenario, a label and a store block.
func TestARunIsNotMistakenForAFleet(t *testing.T) {
	if _, ok := fleetTail(parse(t, recordedRun), 0, ""); ok {
		t.Fatal("a run priced as a month of idle projects")
	}
	if _, ok := runUsage(parse(t, recordedFleet), false); ok {
		t.Fatal("a fleet run priced as a workload")
	}
}

// A card with no prices in it is what tamnd/zou exports for a machine
// you own, and it refuses to guess. A table that quietly showed a box as
// the cheapest store on the page would be exactly the failure that
// refusal exists to prevent, so the card is left out loudly.
func TestACardWithNoPricesIsLeftOutRatherThanShownAsFree(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name+".json"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("aa-priced", `{"name":"aa-priced","as_of":"2026-08-29","source":"a page",
	                     "gb_bytes":1073741824,"storage_per_gb_month":0.02}`)
	write("zz-needs-box", `{"name":"zz-needs-box","as_of":"2026-08-29","source":"your own invoice",
	                        "gb_bytes":1073741824,"needs_box":true}`)
	cards, err := cardsIn(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(cards) != 1 || cards[0].Name != "aa-priced" {
		t.Fatalf("cards = %v", cards)
	}
}

// Rows have to line up across runs of this, so the order is the card
// name and not whatever the directory hands back.
func TestCardsComeOutInAStableOrder(t *testing.T) {
	cards, err := cardsIn("../../pricecards", "wasabi,aws-s3-standard,cloudflare-r2")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, c := range cards {
		names = append(names, c.Name)
	}
	want := []string{"aws-s3-standard", "cloudflare-r2", "wasabi"}
	if len(names) != len(want) {
		t.Fatalf("names = %v", names)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("names = %v, want %v", names, want)
		}
	}
}

// The blocks put a sentence where a number would go when a line was not
// counted, and a sentence under a column headed with a dollar sign reads
// as a price.
func TestAnUncountedLineIsAnEmptyCellAndNotASentence(t *testing.T) {
	if got := usd("not counted, the reader is in the region of the bucket"); got != "" {
		t.Fatalf("usd = %q", got)
	}
	if got := usd(1.5); got != "1.5000" {
		t.Fatalf("usd = %q", got)
	}
	// Free on three of the seven cards, and it should read as free
	// rather than as four decimal places of a rounding.
	if got := usd(0.0); got != "0" {
		t.Fatalf("usd = %q", got)
	}
}
