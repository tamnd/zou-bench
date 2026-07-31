package main

import (
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"
)

// treeSampler samples RSS and cumulative CPU of a process tree twice a
// second. The tree is rediscovered from ps on every sample, so backends
// that appear mid run are counted from their first sample. Disk io
// bytes come from /proc/<pid>/io when it exists, so they are Linux only
// and reported as null elsewhere.
type treeSampler struct {
	root    int
	samples []treeSample
	stop    chan struct{}
	done    chan struct{}
}

type treeSample struct {
	procs     int
	rssKB     int64
	cpuS      float64
	diskRead  *int64
	diskWrite *int64
}

func newTreeSampler(rootPid int) *treeSampler {
	return &treeSampler{root: rootPid, stop: make(chan struct{}), done: make(chan struct{})}
}

func (s *treeSampler) start() {
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

func (s *treeSampler) treePids() []int {
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

func (s *treeSampler) sampleOnce() (treeSample, bool) {
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
	sample := treeSample{procs: len(pids)}
	for _, line := range strings.Split(string(out), "\n") {
		parts := strings.Fields(line)
		if len(parts) != 2 {
			continue
		}
		rss, _ := strconv.ParseInt(parts[0], 10, 64)
		sample.rssKB += rss
		sample.cpuS += parsePsTime(parts[1])
	}
	var read, write int64
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
	}
	if haveIO {
		sample.diskRead, sample.diskWrite = &read, &write
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

func (s *treeSampler) finish() map[string]any {
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
	} else {
		out["disk_read_bytes"] = nil
		out["disk_write_bytes"] = nil
	}
	return out
}
