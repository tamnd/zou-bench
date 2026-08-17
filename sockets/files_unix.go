//go:build !windows

package sockets

import "syscall"

// RaiseFiles lifts this process's open file limit as far as the hard
// limit allows and returns what it ended up being.
//
// A socket is a file descriptor, so a hundred thousand of them is a
// hundred thousand descriptors, and the default soft limit on every box
// worth benchmarking on is far below that. The alternative is a run that
// stops at 1024 sockets and reports a refusal per socket after it, which
// is a measurement of a shell's ulimit rather than of a server. This
// raises the generator's own limit and nothing else: it is not tuning
// the thing under test.
func RaiseFiles(want uint64) (uint64, error) {
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		return 0, err
	}
	if lim.Cur >= want {
		return lim.Cur, nil
	}
	raised := lim
	raised.Cur = want
	if raised.Max != 0 && want > raised.Max {
		raised.Cur = raised.Max
	}
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &raised); err != nil {
		// The hard limit is the operator's business, so the run says
		// what it got rather than refusing to start: a smoke run at a
		// thousand sockets does not care.
		return lim.Cur, err
	}
	return raised.Cur, nil
}
