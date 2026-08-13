//go:build windows

package main

import (
	"errors"
	"os"
)

// Windows has no flock, and the lock only matters to a command that
// already refuses to run here, so this arm exists to keep the package
// compiling. cmdSustain bails on windows long before this is reached.
func lockWorkdir(string) (*os.File, error) {
	return nil, errors.New("the workdir lock needs flock")
}
