//go:build windows

package sockets

// Windows has no descriptor limit to raise, so there is nothing to do
// and the caller is told what it asked for.
func RaiseFiles(want uint64) (uint64, error) { return want, nil }
