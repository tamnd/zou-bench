// Package zoustats reads the store op counter file zou keeps when
// ZOU_STORE_STATS is set, format 1 from crates/zou-store/src/stats.rs.
//
// The file is 273 little endian u64 slots behind a magic and format
// header: count and bytes per op kind and key class, a power of two
// microsecond latency histogram per op kind, io errors per kind, and
// one CAS conflict counter. Counters accumulate for the life of one
// zou boot, so the harness snapshots the file before and after a run
// and works on the difference. Reading is a plain cold file read, it
// never disturbs the live counters.
package zoustats

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
)

var opNames = [6]string{"get", "get_range", "put_if_match", "put", "delete", "list"}
var classNames = [6]string{"manifest", "wal", "chk", "page", "file", "other"}

const (
	kinds        = 6
	classes      = 6
	buckets      = 32
	header       = 2
	bucketBase   = header + kinds*classes*2
	errorBase    = bucketBase + kinds*buckets
	conflictSlot = errorBase + kinds
	slots        = conflictSlot + 1
	format       = 1
)

var magic = binary.LittleEndian.Uint64([]byte("ZOUSTATS"))

// Counters is one decoded counter file, or the difference of two.
type Counters [slots]uint64

func countSlot(kind, class int) int { return header + (kind*classes+class)*2 }

// Read decodes a counter file, refusing anything that is not format 1.
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
	return map[string]any{"ops": ops, "cas_conflicts": c[conflictSlot]}
}

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
