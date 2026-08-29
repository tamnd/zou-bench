package cost

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const gb = 1024 * 1024 * 1024

func retail() Card {
	return Card{
		Name:              "test-retail",
		AsOf:              "2026-08-29",
		Source:            "a page",
		GBBytes:           gb,
		StoragePerGBMonth: 0.02,
		ClassAPerMillion:  5,
		ClassBPerMillion:  0.4,
		EgressPerGB:       0.09,
	}
}

func TestRetailComputePricesStorageAndMeasuredOps(t *testing.T) {
	got := Compute(retail(), Usage{
		StorageBytes:  10 * gb,
		Puts:          1_000_000,
		Gets:          10_000_000,
		DownloadBytes: 2 * gb,
		Egress:        true,
		Measured:      true,
	})
	if got["storage_usd_month"].(float64) != 0.2 {
		t.Fatalf("storage = %v", got["storage_usd_month"])
	}
	if got["ops_usd"].(float64) != 9.0 {
		t.Fatalf("ops = %v", got["ops_usd"])
	}
	if got["egress_usd"].(float64) != 0.18 {
		t.Fatalf("egress = %v", got["egress_usd"])
	}
	if _, present := got["usd_per_million_txns"]; present {
		t.Fatal("per txn cost should be absent when no transactions were counted")
	}
}

// A LIST is class A everywhere, priced with the PUTs rather than with
// the GETs, and that grouping is most of why a freshness barrier over a
// long wal tail is expensive.
func TestAListIsClassAAndCostsWhatAPutCosts(t *testing.T) {
	puts := Compute(retail(), Usage{Puts: 1_000_000, Measured: true})
	lists := Compute(retail(), Usage{Lists: 1_000_000, Measured: true})
	if puts["ops_usd"].(float64) != lists["ops_usd"].(float64) {
		t.Fatalf("a list costs %v and a put costs %v", lists["ops_usd"], puts["ops_usd"])
	}
}

// Egress prices bytes going out to the internet and the reader in every
// scenario this harness runs is a postgres in the region of the bucket,
// where transfer is free. Charging it by default would produce a number
// an order of magnitude high while looking authoritative.
func TestEgressIsOffUntilItIsAskedFor(t *testing.T) {
	quiet := Compute(retail(), Usage{DownloadBytes: 100 * gb, Measured: true})
	if quiet["egress_usd"] == nil {
		t.Fatal("the egress line should say why it is not a number")
	}
	if _, priced := quiet["egress_usd"].(float64); priced {
		t.Fatalf("egress = %v with nobody asking for it", quiet["egress_usd"])
	}
	asked := Compute(retail(), Usage{DownloadBytes: 100 * gb, Egress: true, Measured: true})
	if asked["egress_usd"].(float64) != 9.0 {
		t.Fatalf("egress = %v", asked["egress_usd"])
	}
}

// Backblaze gives away egress up to three times what is stored, which
// is not a constant number of GB and has to be computed against the
// footprint of the run it is applied to.
func TestFreeEgressCanDependOnTheFootprint(t *testing.T) {
	card := retail()
	card.EgressFreeTimesStorage = 3
	card.EgressPerGB = 0.01
	got := Compute(card, Usage{
		StorageBytes:  10 * gb,
		DownloadBytes: 40 * gb,
		Egress:        true,
		Measured:      true,
	})
	// Thirty of the forty GB are free, the other ten are a cent each.
	if got["egress_usd"].(float64) != 0.1 {
		t.Fatalf("egress = %v", got["egress_usd"])
	}
}

// Express charges for the bytes on top of the request, which is the
// line that makes a store of many small objects behave differently
// there than on Standard, and the field the old card shape had no room
// for.
func TestTransferIsChargedOnTopOfTheRequestInRegion(t *testing.T) {
	card := retail()
	card.UploadPerGB = 0.0032
	card.RetrievalPerGB = 0.0006
	got := Compute(card, Usage{
		UploadBytes:   1000 * gb,
		DownloadBytes: 1000 * gb,
		Measured:      true,
	})
	if got["transfer_usd"].(float64) != 3.8 {
		t.Fatalf("transfer = %v", got["transfer_usd"])
	}
	// And it is not egress, which is still off.
	if _, priced := got["egress_usd"].(float64); priced {
		t.Fatalf("egress = %v, a retrieval fee is not an egress fee", got["egress_usd"])
	}
}

func TestTxnsTurnOpsCostIntoDollarsPerMillionCommits(t *testing.T) {
	got := Compute(retail(), Usage{
		Puts:     100_000,
		Gets:     1_000_000,
		Measured: true,
		Txns:     50_000,
	})
	// 0.5 + 0.4 = 0.9 dollars over 50k txns is 18 dollars per million.
	if got["usd_per_million_txns"].(float64) != 18.0 {
		t.Fatalf("per million txns = %v", got["usd_per_million_txns"])
	}
}

// The other per unit line M1b asks for, and it is the store's growth
// rather than everything written to it, since a page rewritten ten
// times was ingested once.
func TestIngestedBytesTurnOpsCostIntoDollarsPerGB(t *testing.T) {
	got := Compute(retail(), Usage{
		Puts:          200_000,
		Measured:      true,
		IngestedBytes: 10 * gb,
	})
	// One dollar of puts over ten gigabytes.
	if got["usd_per_gb_ingested"].(float64) != 0.1 {
		t.Fatalf("per gb = %v", got["usd_per_gb_ingested"])
	}
}

// Each per unit line is left out rather than divided by an assumption
// when its divisor was not measured.
func TestAMissingDivisorLeavesItsLineOut(t *testing.T) {
	got := Compute(retail(), Usage{Puts: 1_000_000, Measured: true})
	if _, present := got["usd_per_million_txns"]; present {
		t.Fatal("dollars per transaction for a run with no transactions counted")
	}
	if _, present := got["usd_per_gb_ingested"]; present {
		t.Fatal("dollars per GB for a run with no ingest measured")
	}
}

func TestUnmeasuredOpsSayUnmeasuredInsteadOfGuessing(t *testing.T) {
	got := Compute(retail(), Usage{StorageBytes: gb})
	if got["ops_usd"] != "unmeasured" {
		t.Fatalf("ops = %v", got["ops_usd"])
	}
	if _, present := got["egress_usd"]; present {
		t.Fatal("egress should be absent when unmeasured")
	}
}

// A machine has no list price, so zou's self hosted card ships with no
// numbers in it at all and will not guess. Loading one here is a
// mistake rather than a free store, and the only self hosted card in
// this directory is one that has had a real box price folded in.
func TestACardWithNoPricesInItIsRefused(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.json")
	body := `{"name":"empty","as_of":"2026-08-29","source":"nowhere","gb_bytes":1073741824,"needs_box":true}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("a card with no prices in it loaded as if it had some")
	}
}

func TestEveryCheckedInCardLoads(t *testing.T) {
	dir := "../pricecards"
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("no price cards")
	}
	for _, e := range entries {
		// The probe drops its calibration files in here too, which are
		// latency curves and not price cards.
		if strings.HasSuffix(e.Name(), ".calibration.json") {
			continue
		}
		card, err := Load(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if card.AsOf == "" || card.Source == "" {
			t.Fatalf("%s: cards must carry as_of and source", card.Name)
		}
		if card.Note == "" {
			t.Fatalf("%s: cards must carry a note", card.Name)
		}
		if card.StoragePerGBMonth <= 0 {
			t.Fatalf("%s: stores for free", card.Name)
		}
	}
}
