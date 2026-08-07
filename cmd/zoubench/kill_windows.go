//go:build windows

package main

import "errors"

// Windows has no SIGKILL and a drill delivered any other way would
// not be the failure under test, so this arm exists only to keep the
// package compiling there. cmdSustain refuses to run on windows long
// before this could be reached.
func sigkill(int) error { return errors.New("sigkill needs unix signals") }
