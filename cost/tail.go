package cost

// The long tail: many projects, almost all of them asleep.
//
// This is the shape the per tenant price arguments are about. Nobody
// asks what a database costs while it is running flat out, they ask
// what eight hundred of them cost when nobody is using seven hundred of
// them, and the answer for a design that keeps everything on object
// storage is almost entirely made of two numbers: the bytes it left
// there, and the operations it goes on paying for while nothing
// happens.
//
// Both of those are measured elsewhere and arrive here as rates. The
// only thing this file does is multiply, and it is separate from the
// per run pricing above because the question is different: that one
// prices a run that happened, this one prices a month that has not, out
// of rates that were watched long enough to be rates.

// HoursPerMonth is the hours a month is billed as, the same 730 every
// provider's calculator uses, so a number here compares with the number
// on their page.
const HoursPerMonth = 730.0

// Ops is a count of store operations by the kinds a card charges for.
type Ops struct {
	Puts    float64 `json:"puts"`
	Gets    float64 `json:"gets"`
	Lists   float64 `json:"lists"`
	Deletes float64 `json:"deletes"`
}

func (o Ops) scale(by float64) Ops {
	return Ops{o.Puts * by, o.Gets * by, o.Lists * by, o.Deletes * by}
}

func (o Ops) add(b Ops) Ops {
	return Ops{o.Puts + b.Puts, o.Gets + b.Gets, o.Lists + b.Lists, o.Deletes + b.Deletes}
}

func (o Ops) usd(c Card) float64 {
	return o.Puts/1000*c.PutPer1000 +
		o.Gets/1000*c.GetPer1000 +
		o.Lists/1000*c.ListPer1000 +
		o.Deletes/1000*c.DeletePer1000
}

// Tail is a deployment to price: how many projects there are, how many
// of them the node holds at once, what they left in the store, and the
// two idle rates measured over a window with no traffic in it.
//
// AttachedAtOnce is the node's own ceiling rather than a guess about
// how busy the fleet is. Pricing it as if every held slot is full for
// the whole month is the expensive reading of the measurement, which is
// the right way round for a number that goes in an argument.
type Tail struct {
	Projects       int
	AttachedAtOnce int
	StorageBytes   int64
	// Node wide operations an hour with nothing attached.
	DormantPerHour Ops
	// Operations an hour one attached project costs on top of that.
	AttachedPerProjectHour Ops
	// What the box costs a month, when the caller has a price for it.
	// Zero leaves compute out of the total rather than assuming it is
	// free, and the caller says which of those it meant.
	BoxUSDMonth float64
	BoxSource   string
}

// Monthly prices a month of that against one card.
//
// The ops line is the node's own housekeeping plus the held projects,
// both scaled from an hour to a month. Nothing here divides work by how
// often a project is used, because that is a property of somebody
// else's users rather than of this deployment, and a number that
// depends on an assumed request rate is the kind of number this harness
// exists to avoid.
func Monthly(c Card, t Tail) map[string]any {
	ops := t.DormantPerHour.
		add(t.AttachedPerProjectHour.scale(float64(t.AttachedAtOnce))).
		scale(HoursPerMonth)
	storageGB := float64(t.StorageBytes) / gb
	storage := storageGB * c.StorageGBMonth
	if c.Kind == "selfhosted" && c.CapacityGB > 0 {
		storage = storageGB / c.CapacityGB * c.BoxUSDMonth
	}
	opsUSD := ops.usd(c)
	total := storage + opsUSD + t.BoxUSDMonth
	out := map[string]any{
		"card":              c.Name,
		"card_dated":        c.Dated,
		"projects":          t.Projects,
		"attached_at_once":  t.AttachedAtOnce,
		"storage_gb":        round4(storageGB),
		"storage_usd_month": round4(storage),
		"ops_month":         ops,
		"ops_usd_month":     round4(opsUSD),
		"total_usd_month":   round4(total),
	}
	if t.BoxUSDMonth > 0 {
		out["compute_usd_month"] = round4(t.BoxUSDMonth)
		out["compute_source"] = t.BoxSource
	} else {
		// A total with no compute in it is a store bill, and calling it
		// the cost of running eight hundred databases would be a lie of
		// omission.
		out["compute_usd_month"] = "not priced"
	}
	if t.Projects > 0 {
		out["usd_per_project_month"] = round4(total / float64(t.Projects))
		out["store_usd_per_project_month"] = round4((storage + opsUSD) / float64(t.Projects))
	}
	return out
}
