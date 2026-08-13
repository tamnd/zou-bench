//go:build !windows

package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Two six hour soaks ran against one workdir on server2 for ninety
// minutes before anyone noticed. The older one had lost its node and
// kept its compact, gc and ledger loops going against the store the
// younger one was loading, and the dev log filled with inserts into a
// table the younger run had not created yet.
func TestASecondSoakCannotTakeALiveWorkdir(t *testing.T) {
	dir := t.TempDir()
	first, err := lockWorkdir(dir)
	if err != nil {
		t.Fatalf("the first soak could not take the workdir: %v", err)
	}
	_, err = lockWorkdir(dir)
	if err == nil {
		t.Fatal("a second soak took a workdir that was already being soaked")
	}
	// The pid is the whole point of the message: whoever sees it has
	// to be able to go find the run that is already there.
	if !strings.Contains(err.Error(), strconv.Itoa(os.Getpid())) {
		t.Fatalf("the error does not name the holder: %v", err)
	}
	// And the lock comes free with the descriptor, so a workdir whose
	// soak is over is usable again without anything to clean up.
	first.Close()
	second, err := lockWorkdir(dir)
	if err != nil {
		t.Fatalf("the workdir stayed locked after the holder let go: %v", err)
	}
	second.Close()
}

// The supervisor of the run that went wrong sat as a zombie for an
// hour and a half, which is how nothing noticed the node was gone.
func TestANodeThatDiedIsReapedAndReadsAsGone(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "fakezou")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexit 3\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	d, err := startDev(bin, "store", "pgbin", 5497, dir,
		filepath.Join(dir, "dev.log"), filepath.Join(dir, "stats"), false)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	select {
	case <-d.gone:
	case <-time.After(30 * time.Second):
		t.Fatal("nothing reaped the supervisor after it exited")
	}
	if d.alive() {
		t.Fatal("a supervisor that exited still reads as alive")
	}
}

func TestALiveNodeReadsAsAlive(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "fakezou")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nsleep 30\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	d, err := startDev(bin, "store", "pgbin", 5497, dir,
		filepath.Join(dir, "dev.log"), filepath.Join(dir, "stats"), false)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { sigkill(d.cmd.Process.Pid) })
	if !d.alive() {
		t.Fatal("a running supervisor reads as gone")
	}
}
