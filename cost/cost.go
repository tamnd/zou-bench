// Package cost turns measured usage into dollars against a price
// card. The rule that keeps this honest: every dollar derives from a
// measured byte or a measured op count. When op counts were not
// captured for a run, the ops line says unmeasured instead of
// estimating, because an estimated request rate is exactly the kind of
// number this harness exists to kill.
package cost

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Card is one provider's retail price card, checked in as a dated
// json file with its source url. Self hosted cards amortize the box
// instead: BoxUSDMonth over CapacityGB is the storage price and ops
// are free, which is how a MinIO on our own hardware gets honest
// dollars next to AWS.
type Card struct {
	Name           string  `json:"name"`
	Source         string  `json:"source"`
	Dated          string  `json:"dated"`
	Kind           string  `json:"kind"` // retail or selfhosted
	StorageGBMonth float64 `json:"storage_gb_month_usd"`
	PutPer1000     float64 `json:"put_per_1000_usd"`
	GetPer1000     float64 `json:"get_per_1000_usd"`
	ListPer1000    float64 `json:"list_per_1000_usd"`
	DeletePer1000  float64 `json:"delete_per_1000_usd"`
	EgressPerGB    float64 `json:"egress_per_gb_usd"`
	MinStorageDays int     `json:"min_storage_days"`
	BoxUSDMonth    float64 `json:"box_usd_month"`
	CapacityGB     float64 `json:"capacity_gb"`
	Notes          string  `json:"notes"`
}

// Usage is what a run measured. Ops are only meaningful when Measured
// is true, storage bytes are always real because the harness stats the
// store itself.
type Usage struct {
	StorageBytes int64
	Puts         int64
	Gets         int64
	Lists        int64
	Deletes      int64
	EgressBytes  int64
	Measured     bool
	// Txns is how many transactions the workload processed while the
	// ops above accumulated, so the report can quote dollars per
	// million transactions, the number that decides whether a design
	// is cheap.
	Txns int64
}

func Load(path string) (Card, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Card{}, err
	}
	var c Card
	if err := json.Unmarshal(raw, &c); err != nil {
		return Card{}, fmt.Errorf("%s: %w", path, err)
	}
	if c.Name == "" {
		return Card{}, fmt.Errorf("%s: card has no name", path)
	}
	return c, nil
}

// Find loads a card by name from a directory of cards.
func Find(dir, name string) (Card, error) {
	path := filepath.Join(dir, name+".json")
	return Load(path)
}

const gb = 1024 * 1024 * 1024

// Compute prices the usage. The storage line is dollars per month at
// the measured size. The ops line is the one time cost of the measured
// requests, present only when they were measured.
func Compute(c Card, u Usage) map[string]any {
	out := map[string]any{
		"card":       c.Name,
		"card_dated": c.Dated,
	}
	storageGB := float64(u.StorageBytes) / gb
	switch c.Kind {
	case "selfhosted":
		if c.CapacityGB > 0 {
			out["storage_usd_month"] = round4(storageGB / c.CapacityGB * c.BoxUSDMonth)
		}
	default:
		out["storage_usd_month"] = round4(storageGB * c.StorageGBMonth)
	}
	if !u.Measured {
		out["ops_usd"] = "unmeasured"
		return out
	}
	ops := float64(u.Puts)/1000*c.PutPer1000 +
		float64(u.Gets)/1000*c.GetPer1000 +
		float64(u.Lists)/1000*c.ListPer1000 +
		float64(u.Deletes)/1000*c.DeletePer1000
	out["ops_usd"] = round4(ops)
	out["egress_usd"] = round4(float64(u.EgressBytes) / gb * c.EgressPerGB)
	// Request cost per million transactions. Egress stays out because
	// it prices readback traffic, not the workload's commits, and
	// storage stays out because it is a monthly rate, not a per run
	// spend.
	if u.Txns > 0 {
		out["usd_per_million_txns"] = round4(ops / float64(u.Txns) * 1e6)
	}
	return out
}

func round4(v float64) float64 { return float64(int(v*10000+0.5)) / 10000 }
