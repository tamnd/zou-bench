package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tamnd/zou-bench/dashboard"
)

// cmdDashboard merges result files into the page saying which published
// claims the benchmarks have earned.
//
// It takes result files rather than finding them, because they live on
// whichever machine ran them and half of them are never on the machine
// the page is written from. What crosses machines is the book, which is
// small, committed, and merged into rather than replaced, so a run of
// one scenario updates its own rows and leaves everyone else's alone.
//
// With no result files it re-renders the book, which is what editing
// the targets file wants: a claim can be added, reworded or given a
// tighter line without rerunning anything.
func cmdDashboard(argv []string) {
	fs := flag.NewFlagSet("dashboard", flag.ExitOnError)
	targetsPath := fs.String("targets", "docs/targets.json", "the claims and their lines")
	bookPath := fs.String("book", "docs/dashboard.json", "readings, merged run by run and committed")
	out := fs.String("out", "docs/dashboard.md", "the page")
	var results []string
	for _, a := range argv {
		if len(a) > 0 && a[0] != '-' {
			results = append(results, a)
		}
	}
	fs.Parse(without(argv, results))

	targets, err := dashboard.LoadTargets(*targetsPath)
	die(err)
	book, err := dashboard.LoadBook(*bookPath)
	die(err)

	for _, path := range results {
		raw, err := os.ReadFile(path)
		die(err)
		var doc map[string]any
		if err := json.Unmarshal(raw, &doc); err != nil {
			die(fmt.Errorf("%s: %w", path, err))
		}
		fresh := dashboard.Take(targets, doc, filepath.Base(path))
		// A result file that answered nothing is worth saying out loud.
		// Usually it is a scenario renamed on one side only, and the
		// quiet version of that is a page that stops moving while the
		// runs keep happening.
		if len(fresh) == 0 {
			fmt.Printf("%s: no target reads anything out of this\n", filepath.Base(path))
			continue
		}
		for id := range fresh {
			fmt.Printf("%s: %s\n", filepath.Base(path), id)
		}
		book.Merge(fresh)
	}

	die(os.MkdirAll(filepath.Dir(*out), 0o755))
	die(os.WriteFile(*out, []byte(dashboard.Render(targets, book)), 0o644))
	die(book.Save(*bookPath))

	t := dashboard.Tally(targets, book)
	fmt.Printf("%s: %d met, %d missed, %d simulated, %d not measured, %d reported\n",
		*out, t[dashboard.Met], t[dashboard.Missed], t[dashboard.Simulated], t[dashboard.NotMeasured], t[dashboard.Reported])
}
