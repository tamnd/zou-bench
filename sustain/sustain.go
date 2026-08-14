// Package sustain holds the hermetic pieces of the soak driver: the
// compaction status parser, the amp bound gate, the RTO summary, and
// the acked write ledger. They live outside cmd/zoubench so the
// arithmetic that decides whether a soak passed can be tested without
// spawning a server or waiting hours.
package sustain

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/tamnd/zou-bench/latency"
)

// ShardStatus is one entry of `zou compact <target> local --status`,
// a read only snapshot of one shard's compaction debt and read
// amplification against its configured bound.
type ShardStatus struct {
	Shard int     `json:"shard"`
	Debt  int64   `json:"debt"`
	Amp   float64 `json:"amp"`
	Bound float64 `json:"bound"`
}

// ParseCompactStatus decodes the --status json array. Anything that
// is not the array is an error rather than an empty sample, because a
// sample recorded from garbage output would look like a healthy store.
func ParseCompactStatus(raw []byte) ([]ShardStatus, error) {
	var shards []ShardStatus
	if err := json.Unmarshal(raw, &shards); err != nil {
		return nil, fmt.Errorf("compact status: %w", err)
	}
	return shards, nil
}

// MaxAmp reduces one status sample to the worst shard's amplification
// and whether any shard sits over its own bound. Each shard is
// compared against its own bound rather than a global one, so a store
// with mixed bounds cannot hide a violating shard behind a laxer
// neighbor.
func MaxAmp(shards []ShardStatus) (amp float64, over bool) {
	for _, s := range shards {
		if s.Amp > amp {
			amp = s.Amp
		}
		if s.Bound > 0 && s.Amp > s.Bound {
			over = true
		}
	}
	return amp, over
}

// BoundHeld is the amp exit gate over the per sample over-bound
// flags, taken in time order. One sample over the bound is transient
// debt the next sweep pays down and is forgiven; two consecutive
// samples over means the sweep ran and the shard was still over, so
// compaction is losing to the load and the gate fails.
func BoundHeld(over []bool) bool {
	for i := 1; i < len(over); i++ {
		if over[i] && over[i-1] {
			return false
		}
	}
	return true
}

// Drill is one executed kill drill. RTOms is nil when the server
// never took a committed write before the probe gave up, absent
// rather than zero because a zero would read as instant recovery, the
// exact opposite of what happened. BalanceOK is nil when the workload
// was not tpcb shaped and the identity had nothing to say.
//
// CheckError is what an identity check hit when it could not run at
// all. The ok flags are false either way, since a database that will
// not answer after a drill has failed the promise, but the two are
// different failures: one says writes went missing, the other says
// the reads did. Without the text the result file makes every read
// failure look like data loss.
type Drill struct {
	Seq        int      `json:"seq"`
	Mode       string   `json:"mode"`
	T          float64  `json:"t_s"`
	RTOms      *float64 `json:"rto_ms,omitempty"`
	LedgerOK   bool     `json:"ledger_ok"`
	BalanceOK  *bool    `json:"balance_ok,omitempty"`
	KillError  string   `json:"kill_error,omitempty"`
	CheckError string   `json:"check_error,omitempty"`
}

// RTOSummary groups the measured recovery times by drill mode and
// summarizes each group, reusing the latency package so the numbers
// are computed the same way every other table in the repo is. Drills
// that never recovered are excluded, their absence is visible in the
// count against the drill list.
func RTOSummary(drills []Drill) map[string]any {
	byMode := map[string][]latency.Sample{}
	for _, d := range drills {
		if d.RTOms == nil {
			continue
		}
		byMode[d.Mode] = append(byMode[d.Mode], latency.Sample{MS: *d.RTOms})
	}
	out := map[string]any{}
	for mode, samples := range byMode {
		p := latency.Percentiles(samples)
		out[mode] = map[string]any{
			"p50":   p["p50"],
			"p95":   p["p95"],
			"max":   p["max"],
			"count": len(samples),
		}
	}
	return out
}

// Ledger records the ids of writes the server acked. An id is handed
// out once and recorded only after COMMIT returned. A failed ack is
// never retried, because the insert may have committed with the ack
// lost in the kill, and retrying the id would hit the primary key;
// the id is simply skipped. The table can therefore hold rows the
// ledger never checks, which is fine: the invariant is that every
// acked id is present, extra rows only mean an ack was lost in
// flight, exactly the crash loop protocol the zou soak script runs.
type Ledger struct {
	mu    sync.Mutex
	next  int64
	acked []int64
}

func NewLedger() *Ledger { return &Ledger{next: 1} }

// Next hands out the id the writer should try, one id per attempt.
func (l *Ledger) Next() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	id := l.next
	l.next++
	return id
}

// Ack records an id whose COMMIT returned.
func (l *Ledger) Ack(id int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.acked = append(l.acked, id)
}

// Acked returns how many ids have been acked.
func (l *Ledger) Acked() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.acked)
}

// MaxAcked returns the highest acked id, 0 when nothing was acked.
func (l *Ledger) MaxAcked() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.acked) == 0 {
		return 0
	}
	return l.acked[len(l.acked)-1]
}

// Chunks copies the acked ids into slices of at most size ids. The
// verification client can only read the first column of the first
// row, so membership is checked as count queries over any-lists, and
// chunking keeps each statement's text bounded no matter how long the
// soak ran.
func (l *Ledger) Chunks(size int) [][]int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out [][]int64
	for lo := 0; lo < len(l.acked); lo += size {
		hi := min(lo+size, len(l.acked))
		out = append(out, append([]int64(nil), l.acked[lo:hi]...))
	}
	return out
}

// FoldShard is one shard's outcome from a merge fold, decoded from
// zou compact --horizon --json. BytesBefore and BytesAfter are the
// shard's whole layer footprint either side of the cut, not the bytes
// the pass touched, so a soak can plot the thing the fold exists to
// hold down.
type FoldShard struct {
	Shard       int    `json:"shard"`
	Horizon     string `json:"horizon"`
	Retired     int    `json:"retired"`
	Outputs     int    `json:"outputs"`
	Imaged      int    `json:"imaged"`
	Unbased     int    `json:"unbased"`
	Pinned      int    `json:"pinned"`
	BytesBefore int64  `json:"bytes_before"`
	BytesAfter  int64  `json:"bytes_after"`
}

// FoldReport is one fold across every shard of a tenant. Shards is
// empty when no shard had anything below the horizon, which is the
// ordinary answer on a young store and not a failure.
type FoldReport struct {
	Horizon string      `json:"horizon"`
	Shards  []FoldShard `json:"shards"`
}

// ParseFoldReport decodes the --horizon --json object. Anything that
// is not the object is an error rather than an empty report, for the
// same reason a garbage status sample is: a fold recorded from noise
// would read as a fold that ran and found nothing to do, which is the
// one outcome that looks identical to a healthy store.
func ParseFoldReport(raw []byte) (FoldReport, error) {
	var rep FoldReport
	if err := json.Unmarshal(raw, &rep); err != nil {
		return FoldReport{}, fmt.Errorf("fold report: %w", err)
	}
	if rep.Horizon == "" {
		return FoldReport{}, fmt.Errorf("fold report: no horizon")
	}
	return rep, nil
}

// FoldTotals reduces one report to the numbers a timeline sample
// carries: the footprint either side of the fold summed over every
// shard that moved, and how many layers went away to get there. A
// report with no shards sums to zeros, which is the honest reading of
// a fold that was allowed to cut nowhere.
func (r FoldReport) FoldTotals() (before, after int64, retired int) {
	for _, s := range r.Shards {
		before += s.BytesBefore
		after += s.BytesAfter
		retired += s.Retired
	}
	return before, after, retired
}

// GcSummary is one collection sweep, read back off the line zou gc
// prints when it is done: so many tenants pinned, so many objects
// deleted, so many stamped and waiting out the safety window.
//
// Deleting takes two sweeps by construction, one to stamp a key and a
// later one to delete it if it is still garbage, so the two counts
// belong to different sweeps: Candidates is what this run stamped and
// Deleted is what an earlier run stamped and this one still found
// unreferenced. A soak that keeps only the totals cannot tell a store
// that collected nothing from one that never ran a sweep, and the
// missing layer hunt in zou #388 was a day of reading mtimes because
// the sweeps left no record of themselves at all.
type GcSummary struct {
	Tenants    int
	Deleted    int
	Candidates int
}

// ParseGcSummary reads that line off the end of a sweep's output. A
// dry run prints a would-delete line per doomed key first, so the
// summary is the last line rather than the only one, and it is the
// last line that has to parse: noise anywhere above it is somebody
// else's log, noise where the summary should be is a sweep whose
// outcome nobody knows.
func ParseGcSummary(raw []byte) (GcSummary, error) {
	line := ""
	for _, l := range strings.Split(string(raw), "\n") {
		if l = strings.TrimSpace(l); l != "" {
			line = l
		}
	}
	f := strings.Fields(line)
	if len(f) < 6 || f[len(f)-1] != "window" {
		return GcSummary{}, fmt.Errorf("gc summary: %q is not a sweep summary", line)
	}
	var s GcSummary
	var err error
	if s.Tenants, err = countBefore(f, "tenants,"); err != nil {
		return GcSummary{}, err
	}
	if s.Deleted, err = countBefore(f, "objects,"); err != nil {
		return GcSummary{}, err
	}
	if s.Candidates, err = countBefore(f, "waiting"); err != nil {
		return GcSummary{}, err
	}
	return s, nil
}

// countBefore reads the number sitting in front of a word, which is
// how every count in that line is written. Going by the word rather
// than by position is what lets the same parser read a real sweep and
// a dry run, where "deleted" becomes "would delete" and everything
// after it shifts along by one.
func countBefore(fields []string, word string) (int, error) {
	for i, f := range fields {
		if f == word && i > 0 {
			n, err := strconv.Atoi(strings.TrimSuffix(fields[i-1], ","))
			if err != nil {
				return 0, fmt.Errorf("gc summary: %q before %q is not a number", fields[i-1], word)
			}
			return n, nil
		}
	}
	return 0, fmt.Errorf("gc summary: nothing before %q", word)
}
