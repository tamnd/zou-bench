// Package zoustats reads the store op counter file zou keeps when
// ZOU_STORE_STATS is set, format 8 from crates/zou-store/src/stats.rs.
//
// The file is little endian u64 slots behind a magic and format
// header: count and bytes per op kind and key class, a power of two
// microsecond latency histogram per op kind, io errors per kind, one
// CAS conflict counter, per read tier the smgr call count, page count,
// and call latency histogram, per page service phase a sample count
// and its own histogram, per commit path step the same pair, a count
// per park cause, one histogram of how much wal a park was waiting on,
// a count per retry reason, and raw and stored bytes per compressor
// caller. Counters accumulate for the life of one zou boot, so the
// harness snapshots the file before and after a run and works on the
// difference. Reading is a plain cold file read, it never disturbs the
// live counters.
//
// The format number is checked rather than tolerated. A layout that
// grew a section in the middle decodes to plausible garbage under an
// older reader, and a wrong latency histogram is worse than no
// histogram, so a file from a binary this reader does not know is
// refused and the run says so.
//
// The tiers say where a page came from, the phases say what the wait
// was made of. A read the page service answered is one tier sample at
// the backend and, on the service side, a park sample for the time it
// spent waiting on ingest plus a read sample for the read itself. The
// ingest phase is the serve loop doing something other than answering,
// which is every queued reader's latency.
//
// The park causes say which of two opposite problems a park was. A
// backend asks for a page at the position its own wal pusher has made
// durable, which is a position for the whole tenant, so a read either
// waited for wal that wrote the pages it asked for, which only ingest
// speed fixes, or for wal that wrote something else, which wants a per
// block position instead and no amount of ingest speed helps. The park
// gap histogram is the other half of that: park latency says how long
// the wait was, the gap says how many wal bytes it was for, and a
// service that is behind and a service that is idle and being asked
// for a position nothing has reached have the same park latency.
//
// The retry counts say why a request went out again, which the op
// latency histogram cannot: a put that took two seconds because the
// bucket asked for less traffic and a put that took two seconds
// because the object was large are the same bucket and two different
// problems. They are counted inside the backend, under the wrapper
// that counts ops, so one logical op that was retried four times is
// one op and four retries, which is also why the cost line bills the
// op and reports the retries as unbilled.
//
// The compressor counts are the one thing about the store nothing
// outside it can measure. A block that does not compress is stored
// raw, so the object sizes a bucket reports are stored sizes and
// nothing out there knows what those bytes would have been, which
// means a space amplification figure computed from the outside quietly
// credits the engine for compression it may not be getting.
//
// The commit steps say what a commit's wait was made of, from the
// pusher picking WAL up to the durable watermark passing it: push is
// the gap between one chunk reaching the pipeline and the next, stage
// is the append call itself, window is how long the batch a chunk
// joined stayed open, dispatch is the wait from that batch closing to
// a worker taking it, put is the store call, ack is the wait from the
// put finishing to the ack resolving in chain order, and durable is
// the whole of it end to end. Push, stage, and durable are sampled per
// chunk, the other four per batch.
package zoustats

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
)

var opNames = [6]string{"get", "get_range", "put_if_match", "put", "delete", "list"}
var classNames = [7]string{"manifest", "wal", "chk", "shards", "page", "file", "other"}
var tierNames = [4]string{"cache", "local", "store", "service"}
var phaseNames = [4]string{"park", "read", "ingest", "queue"}
var stepNames = [7]string{"push", "stage", "window", "dispatch", "put", "ack", "durable"}
var causeNames = [3]string{"touched", "untouched", "unclear"}
var retryNames = [4]string{"throttle", "server", "transport", "exhausted"}
var packNames = [3]string{"layer", "wal", "file"}

const (
	kinds        = 6
	classes      = 7
	tiers        = 4
	phases       = 4
	steps        = 7
	causes       = 3
	retries      = 4
	packs        = 3
	buckets      = 32
	header       = 2
	bucketBase   = header + kinds*classes*2
	errorBase    = bucketBase + kinds*buckets
	conflictSlot = errorBase + kinds
	tierBase     = conflictSlot + 1
	phaseBase    = tierBase + tiers*(2+buckets)
	stepBase     = phaseBase + phases*(1+buckets)
	causeBase    = stepBase + steps*(1+buckets)
	gapBase      = causeBase + causes
	retryBase    = gapBase + 1 + buckets
	packBase     = retryBase + retries
	slots        = packBase + packs*2
	format       = 8
)

var magic = binary.LittleEndian.Uint64([]byte("ZOUSTATS"))

// Counters is one decoded counter file, or the difference of two.
type Counters [slots]uint64

func countSlot(kind, class int) int { return header + (kind*classes+class)*2 }
func tierSlot(tier int) int         { return tierBase + tier*(2+buckets) }
func phaseSlot(phase int) int       { return phaseBase + phase*(1+buckets) }
func stepSlot(step int) int         { return stepBase + step*(1+buckets) }
func packSlot(pack int) int         { return packBase + pack*2 }

// Read decodes a counter file, refusing anything that is not format 8.
func Read(path string) (Counters, error) {
	var c Counters
	raw, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	if len(raw) < slots*8 {
		return c, fmt.Errorf("%s: not a zou counter file", path)
	}
	for i := range c {
		c[i] = binary.LittleEndian.Uint64(raw[i*8 : i*8+8])
	}
	if c[0] != magic || c[1] != format {
		return c, fmt.Errorf("%s: not a format %d zou counter file", path, format)
	}
	return c, nil
}

// Diff subtracts a before snapshot from an after snapshot slot by
// slot. Counters only grow within one boot, a slot that shrank means
// the store restarted mid run and the delta is meaningless, so that
// errors instead of going negative.
func Diff(before, after Counters) (Counters, error) {
	var d Counters
	d[0], d[1] = after[0], after[1]
	for i := header; i < slots; i++ {
		if after[i] < before[i] {
			return d, fmt.Errorf("counter %d shrank from %d to %d, store restarted mid run", i, before[i], after[i])
		}
		d[i] = after[i] - before[i]
	}
	return d, nil
}

// Report renders counters into the shape the result json carries: one
// block per op kind that saw traffic, with count, bytes, errors,
// latency percentiles as bucket upper bounds in microseconds, and the
// per key class split.
func Report(c Counters) map[string]any {
	ops := map[string]any{}
	for kind, name := range opNames {
		count, bytes := uint64(0), uint64(0)
		byClass := map[string]any{}
		for class, cname := range classNames {
			n := c[countSlot(kind, class)]
			if n == 0 {
				continue
			}
			b := c[countSlot(kind, class)+1]
			count += n
			bytes += b
			byClass[cname] = map[string]uint64{"count": n, "bytes": b}
		}
		if count == 0 {
			continue
		}
		hist := c[bucketBase+kind*buckets : bucketBase+(kind+1)*buckets]
		ops[name] = map[string]any{
			"count":    count,
			"bytes":    bytes,
			"errors":   c[errorBase+kind],
			"p50_us":   percentile(hist, count, 0.50),
			"p95_us":   percentile(hist, count, 0.95),
			"p99_us":   percentile(hist, count, 0.99),
			"by_class": byClass,
		}
	}
	reads := map[string]any{}
	for tier, name := range tierNames {
		calls := c[tierSlot(tier)]
		if calls == 0 {
			continue
		}
		hist := c[tierSlot(tier)+2 : tierSlot(tier)+2+buckets]
		reads[name] = map[string]any{
			"calls":  calls,
			"pages":  c[tierSlot(tier)+1],
			"p50_us": percentile(hist, calls, 0.50),
			"p95_us": percentile(hist, calls, 0.95),
			"p99_us": percentile(hist, calls, 0.99),
		}
	}
	pagesvc := map[string]any{}
	for phase, name := range phaseNames {
		calls := c[phaseSlot(phase)]
		if calls == 0 {
			continue
		}
		hist := c[phaseSlot(phase)+1 : phaseSlot(phase)+1+buckets]
		pagesvc[name] = map[string]any{
			"calls":  calls,
			"p50_us": percentile(hist, calls, 0.50),
			"p95_us": percentile(hist, calls, 0.95),
			"p99_us": percentile(hist, calls, 0.99),
		}
	}
	commit := map[string]any{}
	for step, name := range stepNames {
		samples := c[stepSlot(step)]
		if samples == 0 {
			continue
		}
		hist := c[stepSlot(step)+1 : stepSlot(step)+1+buckets]
		commit[name] = map[string]any{
			"samples": samples,
			"p50_us":  percentile(hist, samples, 0.50),
			"p95_us":  percentile(hist, samples, 0.95),
			"p99_us":  percentile(hist, samples, 0.99),
		}
	}
	parkCause := map[string]any{}
	for cause, name := range causeNames {
		if n := c[causeBase+cause]; n > 0 {
			parkCause[name] = n
		}
	}
	// The gap histogram is wal bytes rather than microseconds, so its
	// percentiles are bucket upper bounds in bytes and are named that
	// way. Same buckets, different unit, and a column headed us that
	// holds bytes is how a report starts lying.
	var parkGap map[string]any
	if samples := c[gapBase]; samples > 0 {
		hist := c[gapBase+1 : gapBase+1+buckets]
		parkGap = map[string]any{
			"samples":   samples,
			"p50_bytes": percentile(hist, samples, 0.50),
			"p95_bytes": percentile(hist, samples, 0.95),
			"p99_bytes": percentile(hist, samples, 0.99),
		}
	}
	retry := map[string]any{}
	for kind, name := range retryNames {
		if n := c[retryBase+kind]; n > 0 {
			retry[name] = n
		}
	}
	packed := map[string]any{}
	for pack, name := range packNames {
		raw := c[packSlot(pack)]
		if raw == 0 {
			continue
		}
		stored := c[packSlot(pack)+1]
		block := map[string]any{"raw_bytes": raw, "stored_bytes": stored}
		if stored > 0 {
			block["ratio"] = round3(float64(raw) / float64(stored))
		}
		packed[name] = block
	}

	out := map[string]any{"ops": ops, "cas_conflicts": c[conflictSlot]}
	if len(reads) > 0 {
		out["reads"] = reads
	}
	if len(pagesvc) > 0 {
		out["pagesvc"] = pagesvc
	}
	if len(commit) > 0 {
		out["commit"] = commit
	}
	if len(parkCause) > 0 {
		out["park_cause"] = parkCause
	}
	if parkGap != nil {
		out["park_gap"] = parkGap
	}
	if len(retry) > 0 {
		out["retries"] = retry
	}
	if len(packed) > 0 {
		out["packed"] = packed
	}
	return out
}

func round3(v float64) float64 { return float64(int(v*1000+0.5)) / 1000 }

// Totals sums the counters into the op classes a price card bills:
// every put kind is a billable write, get and get_range are billable
// reads, and get bytes are what egress meters when the store is
// remote.
type Totals struct {
	Puts       int64
	Gets       int64
	Lists      int64
	Deletes    int64
	GetBytes   int64
	PutBytes   int64
	Conflicts  int64
	AnyTraffic bool
}

func Sum(c Counters) Totals {
	var t Totals
	kindCount := func(kind int) (n, b int64) {
		for class := range classes {
			n += int64(c[countSlot(kind, class)])
			b += int64(c[countSlot(kind, class)+1])
		}
		return
	}
	gn, gb := kindCount(0)
	rn, rb := kindCount(1)
	pn, pb := kindCount(2)
	un, ub := kindCount(3)
	dn, _ := kindCount(4)
	ln, _ := kindCount(5)
	t.Gets, t.GetBytes = gn+rn, gb+rb
	t.Puts, t.PutBytes = pn+un, pb+ub
	t.Deletes, t.Lists = dn, ln
	t.Conflicts = int64(c[conflictSlot])
	t.AnyTraffic = t.Gets+t.Puts+t.Deletes+t.Lists > 0
	return t
}

func percentile(hist []uint64, total uint64, q float64) uint64 {
	if total == 0 {
		return 0
	}
	want := uint64(math.Ceil(float64(total) * q))
	seen := uint64(0)
	for b, n := range hist {
		seen += n
		if seen >= want {
			return 1 << (b + 1)
		}
	}
	return 1 << buckets
}
