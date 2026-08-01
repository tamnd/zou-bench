// Package envinfo captures the machine a benchmark ran on. A number
// without its hardware is not reproducible, so every result file
// carries this block. Capture is best effort per platform, a field the
// platform cannot answer is absent, never guessed.
package envinfo

import (
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

func Capture() map[string]any {
	host, _ := os.Hostname()
	out := map[string]any{
		"hostname": host,
		"os":       runtime.GOOS,
		"arch":     runtime.GOARCH,
		"cpus":     runtime.NumCPU(),
	}
	switch runtime.GOOS {
	case "linux":
		if raw, err := os.ReadFile("/proc/cpuinfo"); err == nil {
			for _, line := range strings.Split(string(raw), "\n") {
				if _, v, ok := strings.Cut(line, "model name"); ok {
					out["cpu_model"] = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(v), ":"))
					break
				}
			}
		}
		if raw, err := os.ReadFile("/proc/meminfo"); err == nil {
			for _, line := range strings.Split(string(raw), "\n") {
				if v, ok := strings.CutPrefix(line, "MemTotal:"); ok {
					if kb, err := strconv.ParseInt(strings.Fields(v)[0], 10, 64); err == nil {
						out["mem_total_mb"] = kb / 1024
					}
				}
			}
		}
		if raw, err := exec.Command("uname", "-r").Output(); err == nil {
			out["kernel"] = strings.TrimSpace(string(raw))
		}
		if raw, err := os.ReadFile("/etc/os-release"); err == nil {
			for _, line := range strings.Split(string(raw), "\n") {
				if v, ok := strings.CutPrefix(line, "PRETTY_NAME="); ok {
					out["os_release"] = strings.Trim(v, `"`)
				}
			}
		}
	case "darwin":
		if raw, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output(); err == nil {
			out["cpu_model"] = strings.TrimSpace(string(raw))
		}
		if raw, err := exec.Command("sysctl", "-n", "hw.memsize").Output(); err == nil {
			if b, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64); err == nil {
				out["mem_total_mb"] = b / (1024 * 1024)
			}
		}
		if raw, err := exec.Command("uname", "-r").Output(); err == nil {
			out["kernel"] = strings.TrimSpace(string(raw))
		}
	case "windows":
		if v := os.Getenv("PROCESSOR_IDENTIFIER"); v != "" {
			out["cpu_model"] = v
		}
	}
	return out
}

// Filesystem reports the filesystem and mount options behind a path on
// Linux, where /proc/mounts makes it cheap. The store and pgdata
// filesystems change fsync cost, so results record them.
func Filesystem(path string) map[string]any {
	raw, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return nil
	}
	abs, err := absEval(path)
	if err != nil {
		return nil
	}
	best := ""
	var out map[string]any
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Fields(line)
		if len(f) < 4 {
			continue
		}
		mount := f[1]
		if strings.HasPrefix(abs, mount) && len(mount) > len(best) {
			best = mount
			out = map[string]any{"mount": mount, "fstype": f[2], "options": f[3]}
		}
	}
	return out
}

func absEval(path string) (string, error) {
	resolved, err := os.Getwd()
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(path, "/") {
		return path, nil
	}
	return resolved + "/" + path, nil
}
