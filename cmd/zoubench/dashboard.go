package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

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
	flags, results := splitDashboardArgs(argv)
	fs.Parse(flags)

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

// splitDashboardArgs separates the result files from the flags, so the
// files can be given before the flags as well as after.
//
// Every flag this subcommand takes has a json or markdown path for a
// value, which is the same shape a result file has, so a walk that only
// asks whether an argument starts with a dash eats docs/targets.json as
// a result and hands the flag package a -targets with nothing after it.
// Skipping the value after a flag that takes one is the difference.
func splitDashboardArgs(argv []string) (flags, results []string) {
	takesValue := map[string]bool{"targets": true, "book": true, "out": true}
	for i := 0; i < len(argv); i++ {
		a := argv[i]
		if !strings.HasPrefix(a, "-") {
			results = append(results, a)
			continue
		}
		flags = append(flags, a)
		name := strings.TrimLeft(a, "-")
		if !strings.Contains(a, "=") && takesValue[name] && i+1 < len(argv) {
			i++
			flags = append(flags, argv[i])
		}
	}
	return flags, results
}
