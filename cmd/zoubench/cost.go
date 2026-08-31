package main

// Pricing a result that has already been recorded, against every card
// rather than the one the run happened to be handed.
//
// A run is priced once, when it finishes, on whichever card the
// invocation named. That answers what the run cost. It does not answer
// what M1b asks, which is what the same workload would cost on seven
// different stores, and getting there by rerunning would be seven
// benchmark runs spent on arithmetic over counters already written down,
// producing seven runs that are not even comparable with each other
// because each one is its own sample.
//
// So this reads the counters back out of the result file and multiplies.
// Nothing here measures anything and nothing here can. A file with no op
// counts in it is reported as having none rather than priced at zero,
// which is the rule the run path already follows and the reason a
// scoreboard of these is worth reading.
//
// Both shapes this harness writes are priced, because the two questions
// the cost line asks are in different files: a run says what a workload
// costs while it happens, a fleet run says what a month of mostly idle
// projects costs when nothing happens.

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/tamnd/zou-bench/cost"
)

func cmdCost(argv []string) {
	fs := flag.NewFlagSet("cost", flag.ExitOnError)
	cardsdir := fs.String("cardsdir", "pricecards", "price card directory")
	only := fs.String("cards", "", "comma separated card names, default every card in the directory")
	egress := fs.Bool("egress", false, "price bytes read as internet transfer, off because the reader is in the region of the bucket")
	boxUSD := fs.Float64("box-usd-month", 0, "what the box costs a month, for the compute line of a fleet result")
	boxSource := fs.String("box-source", "", "where that box price came from")
	asJSON := fs.Bool("json", false, "write the priced blocks as json instead of markdown")
	var files []string
	for _, a := range argv {
		if !strings.HasPrefix(a, "-") && strings.HasSuffix(a, ".json") {
			files = append(files, a)
		}
	}
	fs.Parse(without(argv, files))
	if len(files) == 0 {
		usage()
	}

	cards, err := cardsIn(*cardsdir, *only)
	die(err)
	if len(cards) == 0 {
		die(fmt.Errorf("%s: no price cards with prices in them", *cardsdir))
	}

	for _, path := range files {
		raw, err := os.ReadFile(path)
		die(err)
		var r map[string]any
		die(json.Unmarshal(raw, &r))
		if *asJSON {
			die(priceJSON(r, cards, *egress, *boxUSD, *boxSource))
			continue
		}
		die(priceMarkdown(path, r, cards, *egress, *boxUSD, *boxSource))
	}
}

// cardsIn loads the cards to price against, sorted by name so two runs
// of this produce tables whose rows line up.
//
// A card that has no prices in it is skipped with a line saying so
// rather than dropped quietly or priced as free. That is the self hosted
// card as tamnd/zou exports it, which refuses to guess what a machine
// cost, and a table that silently showed a box as the cheapest store on
// the page would be the exact failure the refusal exists to prevent.
func cardsIn(dir, only string) ([]cost.Card, error) {
	var names []string
	if strings.TrimSpace(only) != "" {
		for _, n := range strings.Split(only, ",") {
			if n = strings.TrimSpace(n); n != "" {
				names = append(names, n)
			}
		}
	} else {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
				names = append(names, strings.TrimSuffix(e.Name(), ".json"))
			}
		}
	}
	sort.Strings(names)
	var cards []cost.Card
	for _, name := range names {
		card, err := cost.Find(dir, name)
		if err != nil {
			fmt.Fprintf(os.Stderr, "skipping %s: %v\n", name, err)
			continue
		}
		cards = append(cards, card)
	}
	return cards, nil
}

// runUsage rebuilds what a run measured out of the file it wrote.
//
// The grouping is the one the store counters use and the one a bill
// uses: a range get is a get, a conditional put is a put, and a list is
// neither of those and is billed with the writes. Getting that wrong
// would move the largest line on most of these cards.
func runUsage(r map[string]any, egress bool) (cost.Usage, bool) {
	u := cost.Usage{Egress: egress}
	ops, ok := mapOf(mapAt(r, "store_ops"), "ops")
	if !ok {
		return u, false
	}
	kind := func(name string) (count, bytes int64) {
		m, ok := mapOf(ops, name)
		if !ok {
			return 0, 0
		}
		return intOf(m["count"]), intOf(m["bytes"])
	}
	get, getBytes := kind("get")
	rng, rngBytes := kind("get_range")
	put, putBytes := kind("put")
	cas, casBytes := kind("put_if_match")
	del, _ := kind("delete")
	list, _ := kind("list")
	u.Gets, u.DownloadBytes = get+rng, getBytes+rngBytes
	u.Puts, u.UploadBytes = put+cas, putBytes+casBytes
	u.Deletes, u.Lists = del, list
	u.Measured = u.Gets+u.Puts+u.Deletes+u.Lists > 0
	if store, ok := mapOf(r, "store"); ok {
		u.StorageBytes = intOf(store["bytes_after"])
		// The store's growth, not everything written to it, since a
		// page rewritten ten times was ingested once. A phase that
		// folded more than it wrote shrank, and a negative ingest is
		// not a thing to divide by.
		if grew := intOf(store["bytes_delta"]); grew > 0 {
			u.IngestedBytes = grew
		}
	}
	u.Txns = intOf(r["transactions"])
	if db, ok := mapOf(mapAt(r, "pg_delta"), "database"); ok {
		u.Commits = intOf(db["xact_commit"])
	}
	return u, u.Measured
}

// fleetTail rebuilds the long tail a fleet run measured: the projects it
// made, the ceiling it held them under, the bytes they left, and the two
// idle rates from the hold window.
//
// Only the hold window, the same as the fleet command itself uses, since
// a month of this deployment is a month of nothing happening and the
// steady and churn phases are the fleet being hammered.
func fleetTail(r map[string]any, boxUSD float64, boxSource string) (cost.Tail, bool) {
	rates, ok := mapOf(mapAt(r, "hold"), "rates")
	if !ok {
		return cost.Tail{}, false
	}
	t := cost.Tail{
		Projects:               int(intOf(r["tenants_ready"])),
		DormantPerHour:         opsRate(rates, "dormant_ops_per_hour"),
		AttachedPerProjectHour: opsRate(rates, "attached_ops_per_project_hour"),
		BoxUSDMonth:            boxUSD,
		BoxSource:              boxSource,
	}
	if node, ok := mapOf(r, "node"); ok {
		t.AttachedAtOnce = int(intOf(node["max_attached"]))
	}
	if store, ok := mapOf(r, "store"); ok {
		t.StorageBytes = intOf(store["bytes"])
	}
	return t, t.Projects > 0 && t.StorageBytes > 0
}

func opsRate(rates map[string]any, key string) cost.Ops {
	m, ok := mapOf(rates, key)
	if !ok {
		return cost.Ops{}
	}
	f := func(name string) float64 {
		v, _ := m[name].(float64)
		return v
	}
	return cost.Ops{Puts: f("puts"), Gets: f("gets"), Lists: f("lists"), Deletes: f("deletes")}
}

func priceMarkdown(path string, r map[string]any, cards []cost.Card, egress bool, boxUSD float64, boxSource string) error {
	// Both shapes are read before anything is printed, so a file with
	// nothing priceable in it produces one sentence rather than a
	// heading followed by an apology.
	tail, isFleet := fleetTail(r, boxUSD, boxSource)
	usage, isRun := runUsage(r, egress)
	if !isFleet && !isRun {
		return fmt.Errorf("%s: no store op counts in it, so there is nothing here to price", path)
	}
	head := strings.TrimSpace(str(r["scenario"]) + " " + str(r["label"]))
	if head == "" {
		head = filepath.Base(path)
	}
	if d := str(r["date"]); len(d) >= 10 {
		head += ", " + d[:10]
	}
	if r["simulated"] != nil {
		// The latency was simulated and the op counts were not, so the
		// dollars are real and the run they came from is not. M1b says
		// a simulated number wears the marker in every table it lands
		// in, and a cost table is a table.
		head += " (sim)"
	}
	fmt.Printf("\n## %s\n\n", head)

	if isFleet {
		fmt.Printf("%d projects, %d held at once, %s in the store, priced from the hold window's idle rates.\n\n",
			tail.Projects, tail.AttachedAtOnce, gib(tail.StorageBytes))
		cols := []string{"card", "dated", "storage", "requests", "compute", "total a month", "per project"}
		row(cols)
		rule(len(cols))
		for _, c := range cards {
			b := cost.Monthly(c, tail)
			row([]string{
				c.Name, c.AsOf,
				usd(b["storage_usd_month"]), usd(b["ops_usd_month"]), usd(b["compute_usd_month"]),
				usd(b["total_usd_month"]), usd(b["usd_per_project_month"]),
			})
		}
		return nil
	}

	fmt.Printf("%d gets, %d puts, %d lists and %d deletes measured, %s up and %s down.\n\n",
		usage.Gets, usage.Puts, usage.Lists, usage.Deletes, gib(usage.UploadBytes), gib(usage.DownloadBytes))
	cols := []string{"card", "dated", "requests", "transfer", "workload", "$/M txns", "$/M commits", "$/GB in", "storage a month"}
	row(cols)
	rule(len(cols))
	for _, c := range cards {
		b := cost.Compute(c, usage)
		row([]string{
			c.Name, c.AsOf,
			usd(b["ops_usd"]), usd(b["transfer_usd"]), usd(b["workload_usd"]),
			usd(b["usd_per_million_txns"]), usd(b["usd_per_million_commits"]),
			usd(b["usd_per_gb_ingested"]), usd(b["storage_usd_month"]),
		})
	}
	return nil
}

func priceJSON(r map[string]any, cards []cost.Card, egress bool, boxUSD float64, boxSource string) error {
	out := map[string]any{"scenario": r["scenario"], "label": r["label"], "date": r["date"]}
	var priced []any
	if tail, ok := fleetTail(r, boxUSD, boxSource); ok {
		out["shape"] = "fleet"
		for _, c := range cards {
			priced = append(priced, cost.Monthly(c, tail))
		}
	} else {
		usage, ok := runUsage(r, egress)
		if !ok {
			return fmt.Errorf("%s: no store op counts in it, so there is nothing here to price", str(r["label"]))
		}
		out["shape"] = "run"
		for _, c := range cards {
			priced = append(priced, cost.Compute(c, usage))
		}
	}
	out["cost"] = priced
	text, err := json.Marshal(out)
	if err != nil {
		return err
	}
	fmt.Println(string(text))
	return nil
}

func row(cells []string) {
	fmt.Print("|")
	for _, c := range cells {
		fmt.Printf(" %s |", c)
	}
	fmt.Println()
}

func rule(n int) {
	fmt.Print("|")
	for i := 0; i < n; i++ {
		fmt.Print("---|")
	}
	fmt.Println()
}

// usd renders a priced cell. The blocks put a sentence in the place of a
// number when a line was not counted, and a sentence in a table column
// headed with a dollar sign is worse than an empty cell, so a cell that
// is not a number is empty and the prose stays in the json.
//
// Free is written as a bare zero rather than four decimal places of it.
// Three of the seven cards charge nothing for a request at all, so a
// column of 0.0000 is most of the table, and it should read as free
// rather than as a rounding.
func usd(v any) string {
	f, ok := v.(float64)
	if !ok {
		return ""
	}
	if f == 0 {
		return "0"
	}
	return fmt.Sprintf("%.4f", f)
}

func gib(b int64) string {
	if b <= 0 {
		return "0 B"
	}
	const unit = 1024
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	size, i := float64(b), 0
	for size >= unit && i+1 < len(units) {
		size /= unit
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%d B", b)
	}
	return fmt.Sprintf("%.1f %s", size, units[i])
}

func intOf(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	}
	return 0
}

func mapOf(m map[string]any, key string) (map[string]any, bool) {
	if m == nil {
		return nil, false
	}
	inner, ok := m[key].(map[string]any)
	return inner, ok
}

func mapAt(m map[string]any, key string) map[string]any {
	inner, _ := mapOf(m, key)
	return inner
}
