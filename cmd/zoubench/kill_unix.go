//go:build !windows

package main

import "syscall"

// sigkill is kill -9. The drills must take a process down without
// giving it any chance to flush or hand off, a catchable signal would
// measure a graceful shutdown instead of a failure.
func sigkill(pid int) error { return syscall.Kill(pid, syscall.SIGKILL) }
