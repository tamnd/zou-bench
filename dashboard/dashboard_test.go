package dashboard

import (
	"encoding/json"
	"strings"
	"testing"
)

func decode(t *testing.T, doc string) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal([]byte(doc), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// The paths a target points with are the shapes the result files
// actually have: a nested percentile, a card picked out of a list by
// name rather than by position, and an invariant written as a boolean.
func TestValueWalksTheShapesResultFilesHave(t *testing.T) {
	doc := decode(t, `{
		"steady": {"latency_ms": {"p99": 1.298}},
		"cost": [
			{"card": "cloudflare-r2", "total_usd_month": 12.5},
			{"card": "aws-s3-standard", "total_usd_month": 61.4}
		],
		"amp_bound_held": true
	}`)
	if v, ok := Value(doc, "steady.latency_ms.p99"); !ok || v != 1.298 {
		t.Fatalf("p99 = %v %v", v, ok)
	}
	if v, ok := Value(doc, "cost.card=aws-s3-standard.total_usd_month"); !ok || v != 61.4 {
		t.Fatalf("priced by name = %v %v", v, ok)
	}
	if v, ok := Value(doc, "cost.1.total_usd_month"); !ok || v != 61.4 {
		t.Fatalf("priced by index = %v %v", v, ok)
	}
	if v, ok := Value(doc, "amp_bound_held"); !ok || v != 1 {
		t.Fatalf("bool = %v %v", v, ok)
	}
}

// A phase that did not run is not a phase that measured zero, and a
// page that cannot tell them apart publishes a zero nobody measured.
func TestAMissingPathAnswersNothingRatherThanZero(t *testing.T) {
	doc := decode(t, `{"steady": {"latency_ms": {"p50": 0.7}}}`)
	for _, path := range []string{"churn.latency_ms.p99", "steady.latency_ms.p99", "steady.latency_ms", "cost.card=nobody.total_usd_month", "cost.0.x"} {
		if v, ok := Value(doc, path); ok {
			t.Fatalf("%s answered %v", path, v)
		}
	}
}

func targets() []Target {
	return []Target{
		{ID: "cold-attach", Group: "Cold attach", Claim: "attach to first row", Scenario: "cold-attach",
			Label: "zou-minio-server3", Path: "attach_ms.p50", Unit: "ms", Compare: "<=", Target: 500},
		{ID: "rss", Group: "Density", Claim: "peak rss", Scenario: "fleet-1000-warm",
			Path: "process.rss_peak_kb", DivideBy: 1000000, Unit: "GB", Compare: "<=", Target: 16},
		{ID: "tenants", Group: "Density", Claim: "tenants on the node", Scenario: "fleet-1000-warm",
			Path: "tenants_ready", Unit: "tenants", Compare: ">=", Target: 1000},
	}
}

func TestTakeReadsOnlyTheTargetsAFileCanAnswer(t *testing.T) {
	doc := decode(t, `{
		"scenario": "cold-attach", "label": "zou-minio-server3", "date": "2026-08-06T21:59:55Z",
		"env": {"hostname": "server3"},
		"attach_ms": {"p50": 245.7}
	}`)
	got := Take(targets(), doc, "cold-attach-zou-minio-server3.json")
	if len(got) != 1 {
		t.Fatalf("read %d targets out of an attach file", len(got))
	}
	r := got["cold-attach"]
	if r.Value != 245.7 || r.Host != "server3" || r.Source == "" {
		t.Fatalf("%+v", r)
	}
}

// A run of the right scenario under the wrong label is somebody else's
// number, and the two cold attach rows on the page are exactly that
// distinction.
func TestTheWrongLabelIsNotTheSameClaim(t *testing.T) {
	doc := decode(t, `{"scenario": "cold-attach", "label": "zou-localfs-gamingpc", "attach_ms": {"p50": 20.5}}`)
	if got := Take(targets(), doc, "x.json"); len(got) != 0 {
		t.Fatalf("%+v", got)
	}
}

func TestDivideByPutsTheNumberInTheUnitsTheClaimIsWrittenIn(t *testing.T) {
	doc := decode(t, `{"scenario": "fleet-1000-warm", "label": "zou-fleet", "process": {"rss_peak_kb": 15738000}, "tenants_ready": 1000}`)
	got := Take(targets(), doc, "x.json")
	if v := got["rss"].Value; v != 15.738 {
		t.Fatalf("rss in GB = %v", v)
	}
}

// Three decimals on a millisecond and none on eighteen seconds of it.
// A churn p99 written as 18239.516 ms invites a reader to believe the
// last two digits, and nothing measured over a network earns them.
func TestNumbersArePrintedToTheDigitsTheyAreWorth(t *testing.T) {
	cases := map[float64]string{
		1.298:     "1.298",
		20.53:     "20.53",
		245.736:   "245.7",
		18239.516: "18240",
		1000:      "1000",
		0:         "0",
		15.738:    "15.74",
	}
	for v, want := range cases {
		if got := trim(v); got != want {
			t.Errorf("trim(%v) = %s, wanted %s", v, got, want)
		}
	}
}

// A book carries numbers from several machines and several days, so a
// rerun replaces the older reading and an older file handed over later
// does not undo the newer one.
func TestTheNewerRunWinsWhicheverOrderTheFilesArrive(t *testing.T) {
	b := Book{Readings: map[string]Reading{}}
	b.Merge(map[string]Reading{"cold-attach": {Value: 245.7, Date: "2026-08-06T21:59:55Z"}})
	b.Merge(map[string]Reading{"cold-attach": {Value: 3000, Date: "2026-08-01T00:00:00Z"}})
	if b.Readings["cold-attach"].Value != 245.7 {
		t.Fatalf("an older run overwrote a newer one: %+v", b.Readings["cold-attach"])
	}
	b.Merge(map[string]Reading{"cold-attach": {Value: 120, Date: "2026-08-09T00:00:00Z"}})
	if b.Readings["cold-attach"].Value != 120 {
		t.Fatalf("a newer run did not land: %+v", b.Readings["cold-attach"])
	}
}

func TestJudge(t *testing.T) {
	under := Target{Compare: "<=", Target: 30}
	over := Target{Compare: ">=", Target: 1000}
	none := Target{}
	cases := []struct {
		name string
		t    Target
		r    Reading
		ok   bool
		want Verdict
	}{
		{"under the line", under, Reading{Value: 20.5}, true, Met},
		{"on the line", under, Reading{Value: 30}, true, Met},
		{"over the line", under, Reading{Value: 245.7}, true, Missed},
		{"at least", over, Reading{Value: 1000}, true, Met},
		{"short", over, Reading{Value: 800}, true, Missed},
		{"nothing measured", under, Reading{}, false, NotMeasured},
		{"no line under it", none, Reading{Value: 7}, true, Reported},
		// A simulated store passing the line is not the line being
		// passed, whichever side of it the number lands on.
		{"simulated", under, Reading{Value: 1, Simulated: "s3-standard"}, true, Simulated},
	}
	for _, c := range cases {
		if got := Judge(c.t, c.r, c.ok); got != c.want {
			t.Errorf("%s: %s, wanted %s", c.name, got, c.want)
		}
	}
}

func TestRenderSaysWhatIsMissingAsPlainlyAsWhatIsMet(t *testing.T) {
	b := Book{Readings: map[string]Reading{
		"cold-attach": {Value: 245.7, Label: "zou-minio-server3", Host: "server3", Date: "2026-08-06T21:59:55Z", Source: "cold-attach.json"},
	}}
	page := Render(targets(), b)
	if !strings.Contains(page, "1 of 3 claims met") {
		t.Errorf("the tally is wrong:\n%s", page)
	}
	if !strings.Contains(page, "245.7 ms") || !strings.Contains(page, "<= 500 ms") {
		t.Errorf("the measured row is wrong:\n%s", page)
	}
	if strings.Count(page, "not measured") < 2 {
		t.Errorf("the two unmeasured claims are not both on the page:\n%s", page)
	}
	if !strings.Contains(page, "`cold-attach.json`") {
		t.Errorf("the page does not say where its number came from:\n%s", page)
	}
	// The host is already in the label of most runs, and repeating it
	// reads like two machines.
	if strings.Contains(page, "server3 on server3") {
		t.Errorf("the host is doubled:\n%s", page)
	}
}

func TestGroupsKeepTheOrderTheTargetsFileWroteThem(t *testing.T) {
	page := Render(targets(), Book{Readings: map[string]Reading{}})
	if strings.Index(page, "## Cold attach") > strings.Index(page, "## Density") {
		t.Fatalf("groups reordered themselves:\n%s", page)
	}
}
