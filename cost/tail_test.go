package cost

import "testing"

func card() Card {
	return Card{
		Name:           "test-retail",
		Kind:           "retail",
		StorageGBMonth: 0.02,
		PutPer1000:     0.005,
		GetPer1000:     0.0004,
		ListPer1000:    0.005,
	}
}

// The two rates are charged differently on purpose: the node's own
// housekeeping is one node whatever the fleet size, and a held project
// multiplies by the ceiling.
func TestAMonthIsHousekeepingPlusWhatTheHeldProjectsCost(t *testing.T) {
	got := Monthly(card(), Tail{
		Projects:               800,
		AttachedAtOnce:         100,
		StorageBytes:           100 * gb,
		DormantPerHour:         Ops{Puts: 10},
		AttachedPerProjectHour: Ops{Puts: 2},
	})
	// 10 + 100*2 = 210 puts an hour, 730 hours, 153300 puts a month at
	// five dollars a million.
	ops := got["ops_month"].(Ops)
	if ops.Puts != 153300 {
		t.Fatalf("puts a month = %v", ops.Puts)
	}
	if got["ops_usd_month"].(float64) != 0.7665 {
		t.Fatalf("ops = %v", got["ops_usd_month"])
	}
	if got["storage_usd_month"].(float64) != 2.0 {
		t.Fatalf("storage = %v", got["storage_usd_month"])
	}
	if got["total_usd_month"].(float64) != 2.7665 {
		t.Fatalf("total = %v", got["total_usd_month"])
	}
	if got["usd_per_project_month"].(float64) != 0.0035 {
		t.Fatalf("per project = %v", got["usd_per_project_month"])
	}
}

// A store bill is not the cost of running the databases, and a run that
// had no price for the box says so instead of implying the box is free.
func TestComputeIsNamedNotPricedRatherThanZero(t *testing.T) {
	got := Monthly(card(), Tail{Projects: 10, StorageBytes: gb})
	if got["compute_usd_month"] != "not priced" {
		t.Fatalf("compute = %v", got["compute_usd_month"])
	}
	priced := Monthly(card(), Tail{
		Projects: 10, StorageBytes: gb, BoxUSDMonth: 50, BoxSource: "an invoice",
	})
	if priced["compute_usd_month"].(float64) != 50 {
		t.Fatalf("compute = %v", priced["compute_usd_month"])
	}
	// 0.02 for the gigabyte plus the box, over ten projects.
	if priced["usd_per_project_month"].(float64) != 5.002 {
		t.Fatalf("per project = %v", priced["usd_per_project_month"])
	}
	if priced["store_usd_per_project_month"].(float64) != 0.002 {
		t.Fatalf("store share = %v", priced["store_usd_per_project_month"])
	}
}

// A self hosted card has no per gigabyte price, it has a box and a
// disk, and the storage line is the share of that box the bytes take.
func TestASelfHostedCardAmortizesItsBox(t *testing.T) {
	got := Monthly(Card{
		Name: "ours", Kind: "selfhosted", BoxUSDMonth: 20, CapacityGB: 400,
	}, Tail{Projects: 800, StorageBytes: 40 * gb})
	if got["storage_usd_month"].(float64) != 2.0 {
		t.Fatalf("storage = %v", got["storage_usd_month"])
	}
}
