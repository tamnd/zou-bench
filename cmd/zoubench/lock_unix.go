//go:build !windows

package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// lockWorkdir takes the workdir for this process and holds it for as
// long as the process runs. Two soaks pointed at one workdir share a
// store, a port and a runtime tree, so the second one's compactions,
// gc passes and kill drills land on the first one's data, and neither
// set of numbers means anything afterwards. The lock is an open file
// descriptor rather than a pid file, so it goes away when the process
// does, including under kill -9, and there is never a stale lock to
// clear by hand. The pid inside is for the human reading the error,
// nothing depends on it.
func lockWorkdir(dir string) (*os.File, error) {
	path := filepath.Join(dir, "sustain.lock")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		held, _ := io.ReadAll(f)
		f.Close()
		who := strings.TrimSpace(string(held))
		if who == "" {
			who = "unknown"
		}
		return nil, fmt.Errorf("workdir %s is already being soaked by pid %s, "+
			"pick another workdir or stop that run first", dir, who)
	}
	if err := f.Truncate(0); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	fmt.Fprintln(f, os.Getpid())
	return f, nil
}
