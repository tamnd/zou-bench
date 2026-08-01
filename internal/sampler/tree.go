// Package sampler watches the system while a benchmark runs: the
// server process tree for RSS and CPU, and on Linux the whole box for
// disk, network, page cache, and swap. Everything is sampled, deltaed,
// and kept as a coarse timeline, because a single end to end number
// cannot show a leak or a mid run stall.
package sampler

import (
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Tree samples RSS and cumulative CPU of a process tree twice a
// second. The tree is rediscovered from ps on every sample, so backends
// that appear mid run are counted from their first sample. Disk io
// bytes come from /proc/<pid>/io when it exists, so they are Linux only
// and reported as null elsewhere.
type Tree struct {
	root    int
	samples []treeSample
	stop    chan struct{}
	done    chan struct{}
}

type treeSample struct {
	at        time.Time
	procs     int
	rssKB     int64
	cpuS      float64
	majflt    *int64
	diskRead  *int64
	diskWrite *int64
}

func NewTree(rootPid int) *Tree {
	return &Tree{root: rootPid, stop: make(chan struct{}), done: make(chan struct{})}
}

func (s *Tree) Start() {
	go func() {
		defer close(s.done)
		for {
			if sample, ok := s.sampleOnce(); ok {
				s.samples = append(s.samples, sample)
			}
			select {
			case <-s.stop:
				return
			case <-time.After(500 * time.Millisecond):
			}
		}
	}()
}

func (s *Tree) treePids() []int {
	out, err := exec.Command("ps", "-ax", "-o", "pid=,ppid=").Output()
	if err != nil {
		return nil
	}
	children := map[int][]int{}
	for _, line := range strings.Split(string(out), "\n") {
		parts := strings.Fields(line)
		if len(parts) != 2 {
			continue
		}
		pid, err1 := strconv.Atoi(parts[0])
		ppid, err2 := strconv.Atoi(parts[1])
		if err1 != nil || err2 != nil {
			continue
		}
		children[ppid] = append(children[ppid], pid)
	}
	seen := map[int]bool{}
	queue := []int{s.root}
	var pids []int
	for len(queue) > 0 {
		pid := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if seen[pid] {
			continue
		}
		seen[pid] = true
		pids = append(pids, pid)
		queue = append(queue, children[pid]...)
	}
	return pids
}

func (s *Tree) sampleOnce() (treeSample, bool) {
	pids := s.treePids()
	if len(pids) == 0 {
		return treeSample{}, false
	}
	specs := make([]string, len(pids))
	for i, p := range pids {
		specs[i] = strconv.Itoa(p)
	}
	out, err := exec.Command("ps", "-o", "rss=,time=", "-p", strings.Join(specs, ",")).Output()
	if err != nil {
		return treeSample{}, false
	}
	sample := treeSample{at: time.Now(), procs: len(pids)}
	for _, line := range strings.Split(string(out), "\n") {
		parts := strings.Fields(line)
		if len(parts) != 2 {
			continue
		}
		rss, _ := strconv.ParseInt(parts[0], 10, 64)
		sample.rssKB += rss
		sample.cpuS += parsePsTime(parts[1])
	}
	var read, write, faults int64
	haveIO := true
	for _, pid := range pids {
		raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/io")
		if err != nil {
			haveIO = false
			break
		}
		for _, line := range strings.Split(string(raw), "\n") {
			if k, v, ok := strings.Cut(line, ": "); ok {
				n, _ := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
				switch k {
				case "read_bytes":
					read += n
				case "write_bytes":
					write += n
				}
			}
		}
		if stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat"); err == nil {
			// Field 12 (1-based) after the comm field is majflt. comm can
			// hold spaces, so cut at the closing paren first.
			if _, rest, ok := strings.Cut(string(stat), ") "); ok {
				fields := strings.Fields(rest)
				if len(fields) > 9 {
					n, _ := strconv.ParseInt(fields[9], 10, 64)
					faults += n
				}
			}
		}
	}
	if haveIO {
		sample.diskRead, sample.diskWrite = &read, &write
		sample.majflt = &faults
	}
	return sample, true
}

// parsePsTime parses ps TIME values: MM:SS.hh, HH:MM:SS, or D-HH:MM:SS.
func parsePsTime(text string) float64 {
	days := 0.0
	if d, rest, ok := strings.Cut(text, "-"); ok {
		if n, err := strconv.Atoi(d); err == nil {
			days = float64(n)
			text = rest
		}
	}
	var parts []float64
	for _, p := range strings.Split(text, ":") {
		f, _ := strconv.ParseFloat(p, 64)
		parts = append(parts, f)
	}
	for len(parts) < 3 {
		parts = append([]float64{0}, parts...)
	}
	return days*86400 + parts[0]*3600 + parts[1]*60 + parts[2]
}

// Finish stops sampling and reduces the samples to peaks, medians,
// deltas, a 10 s RSS timeline, and an RSS slope over the run. A flat
// slope on a long run is the leak check.
func (s *Tree) Finish() map[string]any {
	close(s.stop)
	<-s.done
	if len(s.samples) == 0 {
		return map[string]any{}
	}
	rss := make([]int64, len(s.samples))
	procsMax := 0
	var rssPeak int64
	for i, sm := range s.samples {
		rss[i] = sm.rssKB
		if sm.rssKB > rssPeak {
			rssPeak = sm.rssKB
		}
		if sm.procs > procsMax {
			procsMax = sm.procs
		}
	}
	sort.Slice(rss, func(i, j int) bool { return rss[i] < rss[j] })
	first, last := s.samples[0], s.samples[len(s.samples)-1]
	out := map[string]any{
		"samples":       len(s.samples),
		"procs_max":     procsMax,
		"rss_peak_kb":   rssPeak,
		"rss_median_kb": rss[len(rss)/2],
		"cpu_s_total":   round1(last.cpuS - first.cpuS),
	}
	if first.diskRead != nil && last.diskRead != nil {
		out["disk_read_bytes"] = *last.diskRead - *first.diskRead
		out["disk_write_bytes"] = *last.diskWrite - *first.diskWrite
		out["major_faults"] = *last.majflt - *first.majflt
	} else {
		out["disk_read_bytes"] = nil
		out["disk_write_bytes"] = nil
		out["major_faults"] = nil
	}
	out["rss_timeline_kb"] = timeline(s.samples, 10*time.Second)
	out["rss_slope_kb_per_min"] = slope(s.samples)
	return out
}

// timeline reduces samples to one RSS value per width, the last sample
// in each window.
func timeline(samples []treeSample, width time.Duration) []int64 {
	if len(samples) == 0 {
		return nil
	}
	start := samples[0].at
	var out []int64
	current := int64(-1)
	for _, sm := range samples {
		w := int64(sm.at.Sub(start) / width)
		if w != current {
			out = append(out, sm.rssKB)
			current = w
		} else {
			out[len(out)-1] = sm.rssKB
		}
	}
	return out
}

// slope compares the median RSS of the first and last quarter of the
// run, in KB per minute. Warmup growth lands in the first quarter, so
// a leak shows as a positive slope that persists across quarters.
func slope(samples []treeSample) float64 {
	if len(samples) < 8 {
		return 0
	}
	quarter := len(samples) / 4
	head := medianRSS(samples[:quarter])
	tail := medianRSS(samples[len(samples)-quarter:])
	minutes := samples[len(samples)-1].at.Sub(samples[0].at).Minutes() * 3 / 4
	if minutes <= 0 {
		return 0
	}
	return round1(float64(tail-head) / minutes)
}

func medianRSS(samples []treeSample) int64 {
	vals := make([]int64, len(samples))
	for i, sm := range samples {
		vals[i] = sm.rssKB
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	return vals[len(vals)/2]
}

func round1(v float64) float64 { return float64(int(v*10+0.5)) / 10 }
