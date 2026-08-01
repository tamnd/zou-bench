package cost

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRetailComputePricesStorageAndMeasuredOps(t *testing.T) {
	card := Card{
		Name:           "test-retail",
		Kind:           "retail",
		StorageGBMonth: 0.02,
		PutPer1000:     0.005,
		GetPer1000:     0.0004,
		EgressPerGB:    0.09,
	}
	got := Compute(card, Usage{
		StorageBytes: 10 * gb,
		Puts:         1_000_000,
		Gets:         10_000_000,
		EgressBytes:  2 * gb,
		Measured:     true,
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

func TestTxnsTurnOpsCostIntoDollarsPerMillionCommits(t *testing.T) {
	card := Card{Name: "c", Kind: "retail", PutPer1000: 0.005, GetPer1000: 0.0004}
	got := Compute(card, Usage{
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

func TestUnmeasuredOpsSayUnmeasuredInsteadOfGuessing(t *testing.T) {
	got := Compute(Card{Name: "c", Kind: "retail", StorageGBMonth: 0.02}, Usage{StorageBytes: gb})
	if got["ops_usd"] != "unmeasured" {
		t.Fatalf("ops = %v", got["ops_usd"])
	}
	if _, present := got["egress_usd"]; present {
		t.Fatal("egress should be absent when unmeasured")
	}
}

func TestSelfhostedAmortizesTheBoxOverItsDisk(t *testing.T) {
	card := Card{Name: "box", Kind: "selfhosted", BoxUSDMonth: 15, CapacityGB: 300}
	got := Compute(card, Usage{StorageBytes: 30 * gb})
	if got["storage_usd_month"].(float64) != 1.5 {
		t.Fatalf("storage = %v", got["storage_usd_month"])
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
		card, err := Load(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if card.Dated == "" || card.Source == "" {
			t.Fatalf("%s: cards must carry dated and source", card.Name)
		}
		if card.Kind == "selfhosted" && card.CapacityGB == 0 {
			t.Fatalf("%s: selfhosted needs capacity", card.Name)
		}
	}
}
