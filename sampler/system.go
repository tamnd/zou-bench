package sampler

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// System samples whole box counters once a second on Linux: disk bytes
// and IOPS per physical device, network bytes, page cache size, and
// swap activity. On other platforms Finish returns supported false and
// every field null, never a fabricated zero.
type System struct {
	samples []sysSample
	stop    chan struct{}
	done    chan struct{}
}

type sysSample struct {
	at time.Time
	disk
	netRx, netTx int64
	cachedKB     int64
	swapIn       int64
	swapOut      int64
}

type disk struct {
	readBytes  int64
	writeBytes int64
	reads      int64
	writes     int64
	ioTicksMS  int64
}

func NewSystem() *System {
	return &System{stop: make(chan struct{}), done: make(chan struct{})}
}

func (s *System) Start() {
	go func() {
		defer close(s.done)
		for {
			if sample, ok := sampleSystem(); ok {
				s.samples = append(s.samples, sample)
			}
			select {
			case <-s.stop:
				return
			case <-time.After(time.Second):
			}
		}
	}()
}

func sampleSystem() (sysSample, bool) {
	raw, err := os.ReadFile("/proc/diskstats")
	if err != nil {
		return sysSample{}, false
	}
	sample := sysSample{at: time.Now()}
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Fields(line)
		if len(f) < 14 {
			continue
		}
		name := f[2]
		// Whole disks only: partitions double count and loop, ram, and
		// cd devices are noise. A whole disk has a /sys/block entry.
		if strings.HasPrefix(name, "loop") || strings.HasPrefix(name, "ram") ||
			strings.HasPrefix(name, "sr") || strings.HasPrefix(name, "zram") {
			continue
		}
		if _, err := os.Stat("/sys/block/" + name); err != nil {
			continue
		}
		reads, _ := strconv.ParseInt(f[3], 10, 64)
		sectorsRead, _ := strconv.ParseInt(f[5], 10, 64)
		writes, _ := strconv.ParseInt(f[7], 10, 64)
		sectorsWritten, _ := strconv.ParseInt(f[9], 10, 64)
		ioTicks, _ := strconv.ParseInt(f[12], 10, 64)
		sample.reads += reads
		sample.writes += writes
		sample.readBytes += sectorsRead * 512
		sample.writeBytes += sectorsWritten * 512
		sample.ioTicksMS += ioTicks
	}
	if raw, err := os.ReadFile("/proc/net/dev"); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			name, rest, ok := strings.Cut(line, ":")
			if !ok || strings.TrimSpace(name) == "lo" {
				continue
			}
			f := strings.Fields(rest)
			if len(f) < 9 {
				continue
			}
			rx, _ := strconv.ParseInt(f[0], 10, 64)
			tx, _ := strconv.ParseInt(f[8], 10, 64)
			sample.netRx += rx
			sample.netTx += tx
		}
	}
	if raw, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			if v, ok := strings.CutPrefix(line, "Cached:"); ok {
				n, _ := strconv.ParseInt(strings.Fields(v)[0], 10, 64)
				sample.cachedKB = n
			}
		}
	}
	if raw, err := os.ReadFile("/proc/vmstat"); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			if v, ok := strings.CutPrefix(line, "pswpin "); ok {
				sample.swapIn, _ = strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			}
			if v, ok := strings.CutPrefix(line, "pswpout "); ok {
				sample.swapOut, _ = strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			}
		}
	}
	return sample, true
}

// Finish stops sampling and returns deltas over the run plus a 10 s
// disk write timeline. Rates divide by the sampled wall time.
func (s *System) Finish() map[string]any {
	close(s.stop)
	<-s.done
	if len(s.samples) < 2 {
		return map[string]any{"supported": false}
	}
	first, last := s.samples[0], s.samples[len(s.samples)-1]
	seconds := last.at.Sub(first.at).Seconds()
	if seconds <= 0 {
		return map[string]any{"supported": false}
	}
	out := map[string]any{
		"supported":        true,
		"seconds":          round1(seconds),
		"disk_read_bytes":  last.readBytes - first.readBytes,
		"disk_write_bytes": last.writeBytes - first.writeBytes,
		"disk_reads":       last.reads - first.reads,
		"disk_writes":      last.writes - first.writes,
		"disk_util_pct":    round1(float64(last.ioTicksMS-first.ioTicksMS) / (seconds * 10)),
		"net_rx_bytes":     last.netRx - first.netRx,
		"net_tx_bytes":     last.netTx - first.netTx,
		"page_cache_delta_kb": last.cachedKB -
			first.cachedKB,
		"swap_in_pages":  last.swapIn - first.swapIn,
		"swap_out_pages": last.swapOut - first.swapOut,
	}
	var wtl []int64
	width := 10 * time.Second
	current := int64(-1)
	var base int64
	for _, sm := range s.samples {
		w := int64(sm.at.Sub(first.at) / width)
		if w != current {
			if current >= 0 {
				wtl = append(wtl, sm.writeBytes-base)
			}
			base = sm.writeBytes
			current = w
		}
	}
	out["disk_write_timeline_bytes"] = wtl
	return out
}
