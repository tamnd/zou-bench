// Package dashboard turns result files into a page saying which
// published claims the benchmarks have actually earned.
//
// A result file answers one run's question and nothing else. A claim
// like "cold attach under 30 ms" spans runs, machines and months, and
// the usual way such a claim gets tracked is somebody copying a number
// into a markdown table and then not copying the next one. So the
// claims live in a targets file naming what to read out of which
// scenario, and the page is generated from whatever result files are
// handed to it.
//
// The readings are kept in a book of their own because the raw json
// stays out of git and each run happens on the machine that can run it:
// cold attach on the box with the MinIO next to it, density on the box
// with the cores. Merging into a book lets one page carry a number from
// last week on one machine next to one from this morning on another,
// without either run having to know about the other.
package dashboard

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Target is one published claim: where the number comes from, which
// side of the line it has to land on, and which milestone line it earns
// by landing there.
type Target struct {
	ID    string `json:"id"`
	Group string `json:"group"`
	Claim string `json:"claim"`
	// Scenario and Label say which result files can answer this. An
	// empty label takes the number from any run of the scenario, which
	// is what a claim measured on one machine only wants.
	Scenario string `json:"scenario"`
	Label    string `json:"label"`
	// Path is a dotted path into the result file, see Value.
	Path string `json:"path"`
	// DivideBy converts what the result file stores into what the claim
	// is written in, kilobytes into gigabytes and the like. Zero means
	// one, so a target needing no conversion says nothing.
	DivideBy float64 `json:"divide_by"`
	Unit     string  `json:"unit"`
	// Compare is <= or >=, and empty for a headline number with no line
	// under it yet. Those are reported rather than judged, because a
	// budget invented to make a table look finished is worse than a
	// table admitting which of its numbers are context.
	Compare string  `json:"compare"`
	Target  float64 `json:"target"`
	// Earns names the milestone line this row is evidence for, so a met
	// row can be traced to the box it ticks.
	Earns string `json:"earns"`
	Note  string `json:"note"`
}

// Reading is one measurement of one target, with enough of the run
// attached to find it again.
type Reading struct {
	Value     float64 `json:"value"`
	Label     string  `json:"label"`
	Host      string  `json:"host"`
	Date      string  `json:"date"`
	Source    string  `json:"source"`
	Simulated string  `json:"simulated,omitempty"`
}

// Book is the readings the page was rendered from, by target id. It is
// committed next to the page, so regenerating after an edit to the
// targets file does not need the raw json back.
type Book struct {
	Readings map[string]Reading `json:"readings"`
}

// LoadTargets reads the targets file.
func LoadTargets(path string) ([]Target, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []Target
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	seen := map[string]bool{}
	for _, t := range out {
		if t.ID == "" {
			return nil, fmt.Errorf("%s: a target with no id", path)
		}
		if seen[t.ID] {
			return nil, fmt.Errorf("%s: two targets called %s", path, t.ID)
		}
		seen[t.ID] = true
		switch t.Compare {
		case "", "<=", ">=":
		default:
			return nil, fmt.Errorf("%s: %s compares with %q, which is neither <= nor >=", path, t.ID, t.Compare)
		}
	}
	return out, nil
}

// LoadBook reads the book, and an absent one is an empty one so the
// first run of a new dashboard works like every later one.
func LoadBook(path string) (Book, error) {
	b := Book{Readings: map[string]Reading{}}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return b, nil
	}
	if err != nil {
		return b, err
	}
	if err := json.Unmarshal(raw, &b); err != nil {
		return b, fmt.Errorf("%s: %w", path, err)
	}
	if b.Readings == nil {
		b.Readings = map[string]Reading{}
	}
	return b, nil
}

// Save writes the book, which marshals its map in key order, so a run
// that moved one number leaves a diff of one number.
func (b Book) Save(path string) error {
	out, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(out, '\n'), 0o644)
}

// Take reads every target this result file can answer.
//
// A file carrying the scenario but not the path answers nothing rather
// than answering zero: a phase that did not run and a phase that
// measured zero are different things and only one of them belongs on a
// page.
func Take(targets []Target, result map[string]any, source string) map[string]Reading {
	out := map[string]Reading{}
	scenario, _ := result["scenario"].(string)
	label, _ := result["label"].(string)
	host := ""
	if env, ok := result["env"].(map[string]any); ok {
		host, _ = env["hostname"].(string)
	}
	date, _ := result["date"].(string)
	sim, _ := result["simulated"].(string)
	for _, t := range targets {
		if t.Scenario != scenario {
			continue
		}
		if t.Label != "" && t.Label != label {
			continue
		}
		v, ok := Value(result, t.Path)
		if !ok {
			continue
		}
		if t.DivideBy != 0 {
			v /= t.DivideBy
		}
		out[t.ID] = Reading{Value: v, Label: label, Host: host, Date: date, Source: source, Simulated: sim}
	}
	return out
}

// Merge folds fresh readings into a book, the newest run winning.
//
// By the run's date rather than by the order the files were handed
// over, because a rerun of last week's scenario usually arrives
// alongside this week's and the page should not depend on which came
// first on the command line.
func (b Book) Merge(fresh map[string]Reading) {
	for id, r := range fresh {
		if old, ok := b.Readings[id]; ok && old.Date > r.Date {
			continue
		}
		b.Readings[id] = r
	}
}

// Value walks a dotted path into a decoded result file.
//
// A segment is a key, an array index, or `field=value`, which picks the
// element of an array whose field reads that way. That last one is for
// lists whose order is an accident of the command line, like the priced
// cards, where an index would quietly point a target at a different
// price card the day somebody reorders a flag.
func Value(doc any, path string) (float64, bool) {
	cur := doc
	for seg := range strings.SplitSeq(path, ".") {
		if seg == "" {
			return 0, false
		}
		switch node := cur.(type) {
		case map[string]any:
			v, ok := node[seg]
			if !ok {
				return 0, false
			}
			cur = v
		case []any:
			if key, want, found := strings.Cut(seg, "="); found {
				cur = nil
				for _, e := range node {
					m, ok := e.(map[string]any)
					if !ok {
						continue
					}
					if s, ok := m[key].(string); ok && s == want {
						cur = m
						break
					}
				}
				if cur == nil {
					return 0, false
				}
				continue
			}
			i, err := strconv.Atoi(seg)
			if err != nil || i < 0 || i >= len(node) {
				return 0, false
			}
			cur = node[i]
		default:
			return 0, false
		}
	}
	switch v := cur.(type) {
	case float64:
		return v, true
	case bool:
		// An invariant recorded as a boolean is still something a line
		// can be drawn under, one for held and zero for did not.
		if v {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

// Verdict is what a row says about itself.
type Verdict string

const (
	Met         Verdict = "met"
	Missed      Verdict = "missed"
	Simulated   Verdict = "simulated"
	Reported    Verdict = "reported"
	NotMeasured Verdict = "not measured"
)

// Judge decides a row.
//
// A simulated store is never met whatever the number says, per the M1b
// rule that a simulated number holds a place until a real bucket run
// replaces it. It is not called missed either, because the claim has
// not been tested rather than tested and found wanting.
func Judge(t Target, r Reading, ok bool) Verdict {
	if !ok {
		return NotMeasured
	}
	if r.Simulated != "" {
		return Simulated
	}
	if t.Compare == "" {
		return Reported
	}
	if t.Compare == "<=" && r.Value <= t.Target {
		return Met
	}
	if t.Compare == ">=" && r.Value >= t.Target {
		return Met
	}
	return Missed
}

// Tally counts the verdicts, for the line at the top saying where the
// whole board stands.
func Tally(targets []Target, b Book) map[Verdict]int {
	out := map[Verdict]int{}
	for _, t := range targets {
		r, ok := b.Readings[t.ID]
		out[Judge(t, r, ok)]++
	}
	return out
}

// Render writes the page.
//
// Groups appear in the order the targets file lists them, because that
// file is the one a person edits and the order in it is the order they
// meant. Nothing in the output is stamped with the time it was
// generated: the page changes when a number changes, and every row
// already carries the date of the run its number came from.
func Render(targets []Target, b Book) string {
	var out strings.Builder
	out.WriteString("# Benchmark dashboard\n\n")
	out.WriteString("Generated by `zoubench dashboard` out of the result files the runs wrote, so every number here is one a run measured rather than one somebody remembered to copy over.\n")
	out.WriteString("The raw json stays out of git and each run happens on whichever machine can run it, so the readings are kept in `dashboard.json` next to this page and merged run by run.\n")
	out.WriteString("Regenerate with `zoubench dashboard --targets docs/targets.json --book docs/dashboard.json --out docs/dashboard.md <result.json>...` on the machine holding the json, and commit both files.\n\n")
	out.WriteString("A row is met when the measured number is on the right side of the line, and nothing else counts.\n")
	out.WriteString("A simulated store is never met, because a simulated number only holds a place until a real bucket replaces it.\n")
	out.WriteString("A claim nothing has measured yet says so instead of being left off, and a headline number with no line under it yet is reported rather than judged.\n\n")

	t := Tally(targets, b)
	out.WriteString("## Where it stands\n\n")
	fmt.Fprintf(&out, "%d of %d claims met, %d missed, %d simulated, %d not measured yet, and %d numbers reported without a line.\n\n",
		t[Met], t[Met]+t[Missed]+t[Simulated]+t[NotMeasured], t[Missed], t[Simulated], t[NotMeasured], t[Reported])

	for _, group := range groups(targets) {
		out.WriteString("## " + group + "\n\n")
		out.WriteString("| claim | line | measured | | where | when | earns |\n")
		out.WriteString("| --- | --- | ---: | --- | --- | --- | --- |\n")
		for _, tg := range targets {
			if tg.Group != group {
				continue
			}
			r, ok := b.Readings[tg.ID]
			fmt.Fprintf(&out, "| %s | %s | %s | %s | %s | %s | %s |\n",
				tg.Claim, line(tg), measured(tg, r, ok), Judge(tg, r, ok), where(r, ok), when(r, ok), tg.Earns)
		}
		out.WriteString("\n")
		for _, tg := range targets {
			if tg.Group == group && tg.Note != "" {
				out.WriteString(tg.Note + "\n")
			}
		}
		out.WriteString("\n")
	}

	out.WriteString("## Where the numbers came from\n\n")
	var files []string
	seen := map[string]bool{}
	for _, r := range b.Readings {
		if r.Source != "" && !seen[r.Source] {
			seen[r.Source] = true
			files = append(files, r.Source)
		}
	}
	sort.Strings(files)
	if len(files) == 0 {
		out.WriteString("Nothing has been measured into this book yet.\n")
		return out.String()
	}
	for _, f := range files {
		out.WriteString("- `" + f + "`\n")
	}
	return out.String()
}

func groups(targets []Target) []string {
	var out []string
	seen := map[string]bool{}
	for _, t := range targets {
		if !seen[t.Group] {
			seen[t.Group] = true
			out = append(out, t.Group)
		}
	}
	return out
}

func line(t Target) string {
	if t.Compare == "" {
		return "no line"
	}
	return fmt.Sprintf("%s %s %s", t.Compare, trim(t.Target), t.Unit)
}

func measured(t Target, r Reading, ok bool) string {
	if !ok {
		return ""
	}
	return trim(r.Value) + " " + t.Unit
}

func where(r Reading, ok bool) string {
	if !ok {
		return ""
	}
	w := r.Label
	if r.Host != "" && !strings.Contains(w, r.Host) {
		w += " on " + r.Host
	}
	return w
}

func when(r Reading, ok bool) string {
	if !ok || len(r.Date) < 10 {
		return ""
	}
	return r.Date[:10]
}

// trim prints a number the way somebody would write it down: three
// decimals on a millisecond, none on eighteen seconds of it, and no
// trailing zeroes or exponents anywhere. Digits past the third
// significant one are noise a page should not carry, and a churn p99
// printed as 18239.516 ms invites a reader to believe the last two.
func trim(v float64) string {
	digits := 3
	switch a := math.Abs(v); {
	case a >= 1000:
		digits = 0
	case a >= 100:
		digits = 1
	case a >= 10:
		digits = 2
	}
	s := strconv.FormatFloat(v, 'f', digits, 64)
	if !strings.Contains(s, ".") {
		return s
	}
	s = strings.TrimRight(s, "0")
	return strings.TrimSuffix(s, ".")
}
