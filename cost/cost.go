// Package cost turns measured usage into dollars against a price
// card. The rule that keeps this honest: every dollar derives from a
// measured byte or a measured op count. When op counts were not
// captured for a run, the ops line says unmeasured instead of
// estimating, because an estimated request rate is exactly the kind of
// number this harness exists to kill.
//
// The cards themselves are not owned here. They live in tamnd/zou, in
// crates/zou/src/cost.rs, and `zou cost --export-cards pricecards`
// regenerates the directory next door. That is deliberate: the same
// seven providers used to be priced twice, once there and once here,
// and two copies of a price list drift the first time a vendor changes
// a number and only one side reads the page. Then the same run has two
// dollar figures and nothing says which is current. So the shape below
// is the exported shape, field for field, and a card that goes stale
// goes stale once.
package cost

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Card is one provider's retail price card, checked in as a dated json
// file with the page it was read from.
//
// Everything is a rate per unit rather than a total, so nothing in here
// depends on the size of the workload it is applied to, and a card can
// be checked against the vendor's page without knowing anything about
// the run it will be used on.
type Card struct {
	Name string `json:"name"`
	// The day the numbers were read off the vendor, carried into every
	// result, because a cost from a card that is two years old is a
	// cost from two years ago and should say so.
	AsOf   string `json:"as_of"`
	Source string `json:"source"`
	Note   string `json:"note"`
	// What the vendor means by a GB when it bills one. AWS defines it
	// as 2^30 bytes and most other pages write GB without defining it
	// at all, so every card uses the binary one, which is the
	// conservative reading. It is a field rather than a constant
	// because the difference is seven percent of the storage line.
	GBBytes           float64 `json:"gb_bytes"`
	StoragePerGBMonth float64 `json:"storage_per_gb_month"`
	// PUT, COPY, POST and LIST, the expensive class everywhere.
	ClassAPerMillion float64 `json:"class_a_per_million"`
	// GET, HEAD and everything else.
	ClassBPerMillion float64 `json:"class_b_per_million"`
	DeletePerMillion float64 `json:"delete_per_million"`
	// Bytes written, charged on top of the request. Only S3 Express
	// One Zone does this, and it is the line that makes a store of many
	// small objects behave differently there than on Standard.
	UploadPerGB float64 `json:"upload_per_gb"`
	// Bytes read, charged on top of the request, and not the same thing
	// as egress: a retrieval fee is paid even when the reader sits in
	// the region of the bucket.
	RetrievalPerGB float64 `json:"retrieval_per_gb"`
	// Bytes out to the internet, which is off unless the caller asks
	// for it. See Usage.Egress.
	EgressPerGB float64 `json:"egress_per_gb"`
	// Backblaze gives free egress up to a multiple of what is stored,
	// which is not a constant number of GB and cannot be written as one.
	EgressFreeGBMonth      float64 `json:"egress_free_gb_month"`
	EgressFreeTimesStorage float64 `json:"egress_free_times_storage"`
	// A card with no prices in it, which is what a machine you own is
	// until somebody says what it cost. A self hosted card that has had
	// its box price folded into StoragePerGBMonth clears this and says
	// in its note where the number came from.
	NeedsBox bool `json:"needs_box"`
}

// Usage is what a run measured. Ops are only meaningful when Measured
// is true; storage bytes are always real because the harness stats the
// store itself.
type Usage struct {
	StorageBytes int64
	Puts         int64
	Gets         int64
	Lists        int64
	Deletes      int64
	// Bytes the run wrote to and read from the store. Both are charged
	// on the cards that bill for transfer on top of the request, which
	// today is only Express.
	UploadBytes   int64
	DownloadBytes int64
	Measured      bool
	// Egress asks for the internet transfer line. It is off by default
	// because the reader in every scenario this harness runs is a
	// postgres in the region of the bucket, where transfer is free, and
	// charging a page read at internet rates would produce a number an
	// order of magnitude high while looking authoritative.
	Egress bool
	// Txns is how many transactions the workload processed while the
	// ops above accumulated, so the report can quote dollars per
	// million transactions, the number that decides whether a design
	// is cheap.
	Txns int64
	// Bytes the workload ingested, for the dollars per GB ingested line
	// M1b asks for. This is the store's growth over the run rather than
	// everything written to it, since a page rewritten ten times was
	// ingested once.
	IngestedBytes int64
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
	if c.GBBytes <= 0 {
		return Card{}, fmt.Errorf("%s: card bills in a GB of no bytes", path)
	}
	if c.NeedsBox {
		return Card{}, fmt.Errorf("%s: card has no prices in it, see its note", path)
	}
	return c, nil
}

// Find loads a card by name from a directory of cards.
func Find(dir, name string) (Card, error) {
	path := filepath.Join(dir, name+".json")
	return Load(path)
}

// requests is the request half of a bill, which is the half a design
// decision moves. A LIST is class A everywhere, priced with the PUTs
// rather than with the GETs, and that grouping is most of why a
// freshness barrier is expensive.
func requests(c Card, puts, gets, lists, deletes int64) float64 {
	classA := float64(puts+lists) / 1e6 * c.ClassAPerMillion
	classB := float64(gets) / 1e6 * c.ClassBPerMillion
	return classA + classB + float64(deletes)/1e6*c.DeletePerMillion
}

// Compute prices the usage. The storage line is dollars per month at
// the measured size. The ops line is the one time cost of the measured
// requests, present only when they were measured.
func Compute(c Card, u Usage) map[string]any {
	out := map[string]any{
		"card":        c.Name,
		"card_dated":  c.AsOf,
		"card_source": c.Source,
	}
	storageGB := float64(u.StorageBytes) / c.GBBytes
	out["storage_usd_month"] = round4(storageGB * c.StoragePerGBMonth)
	if !u.Measured {
		out["ops_usd"] = "unmeasured"
		return out
	}
	ops := requests(c, u.Puts, u.Gets, u.Lists, u.Deletes)
	// Transfer charged on top of the request, which is a different
	// thing from egress: a retrieval fee is paid in region.
	transfer := float64(u.UploadBytes)/c.GBBytes*c.UploadPerGB +
		float64(u.DownloadBytes)/c.GBBytes*c.RetrievalPerGB
	out["ops_usd"] = round4(ops)
	if transfer > 0 {
		out["transfer_usd"] = round4(transfer)
	}
	work := ops + transfer
	if u.Egress {
		free := c.EgressFreeGBMonth + storageGB*c.EgressFreeTimesStorage
		billable := float64(u.DownloadBytes)/c.GBBytes - free
		if billable < 0 {
			billable = 0
		}
		egress := billable * c.EgressPerGB
		out["egress_usd"] = round4(egress)
		work += egress
	} else {
		out["egress_usd"] = "not counted, the reader is in the region of the bucket"
	}
	out["workload_usd"] = round4(work)
	// Storage stays out of the workload total because it is a rate per
	// month and the run was not a month. It prints beside the total
	// rather than inside it.
	//
	// The two per unit lines below are each omitted rather than printed
	// as zero when their divisor was not measured, since dollars per
	// transaction for a run whose transactions nobody counted is a
	// division by an assumption.
	if u.Txns > 0 {
		out["usd_per_million_txns"] = round4(work / float64(u.Txns) * 1e6)
	}
	if u.IngestedBytes > 0 {
		out["usd_per_gb_ingested"] = round4(work / (float64(u.IngestedBytes) / c.GBBytes))
	}
	return out
}

func round4(v float64) float64 { return float64(int(v*10000+0.5)) / 10000 }
