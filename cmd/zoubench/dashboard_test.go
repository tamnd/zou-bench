package main

import (
	"slices"
	"testing"
)

// The command in the README writes the flags first and every one of
// their values is a path ending in json or md, which is what a result
// file looks like too. Telling them apart by shape loses either the
// flag's value or the result file, and both failures are silent enough
// to publish a stale page.
func TestTheFlagsKeepTheirValuesAndTheResultsStayResults(t *testing.T) {
	for _, c := range []struct {
		name    string
		argv    []string
		flags   []string
		results []string
	}{
		{
			name:    "flags first, the documented order",
			argv:    []string{"--targets", "docs/targets.json", "--book", "docs/dashboard.json", "--out", "docs/dashboard.md", "a.json", "b.json"},
			flags:   []string{"--targets", "docs/targets.json", "--book", "docs/dashboard.json", "--out", "docs/dashboard.md"},
			results: []string{"a.json", "b.json"},
		},
		{
			name:    "results first",
			argv:    []string{"a.json", "--out", "elsewhere.md"},
			flags:   []string{"--out", "elsewhere.md"},
			results: []string{"a.json"},
		},
		{
			name:    "a value joined by an equals sign keeps the next argument",
			argv:    []string{"--out=docs/dashboard.md", "a.json"},
			flags:   []string{"--out=docs/dashboard.md"},
			results: []string{"a.json"},
		},
		{
			name:    "no results at all re-renders the book",
			argv:    []string{"-targets", "docs/targets.json"},
			flags:   []string{"-targets", "docs/targets.json"},
			results: nil,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			flags, results := splitDashboardArgs(c.argv)
			if !slices.Equal(flags, c.flags) {
				t.Errorf("flags = %v, want %v", flags, c.flags)
			}
			if !slices.Equal(results, c.results) {
				t.Errorf("results = %v, want %v", results, c.results)
			}
		})
	}
}
