package main

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// The percentiles a run publishes are computed from these files and
// cannot be recomputed without them, so a published tail whose logs
// went out with a temporary directory is a number nobody can check.
func TestKeptLogsHoldEveryThreadsFile(t *testing.T) {
	dir := t.TempDir()
	want := map[string]string{
		"pgbench_log.4242":   "0 1 1234 0 1756000000 1000\n",
		"pgbench_log.4242.1": "1 1 5678 0 1756000000 2000\n",
	}
	for name, body := range want {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	dest := filepath.Join(t.TempDir(), "run.txnlog.tar.gz")
	if err := keepLogs(dir, dest); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(dest)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		got[hdr.Name] = string(body)
	}
	if len(got) != len(want) {
		t.Fatalf("archived %d files, wrote %d", len(got), len(want))
	}
	for name, body := range want {
		if got[name] != body {
			t.Fatalf("%s came back as %q", name, got[name])
		}
	}
}

// A scenario with the transaction log turned off leaves an empty
// directory, and an empty archive next to a result would say a run kept
// its logs when there were none to keep.
func TestNoLogsMeansNoArchive(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "run.txnlog.tar.gz")
	if err := keepLogs(t.TempDir(), dest); err == nil {
		t.Fatal("an empty log directory archived as if it held something")
	}
	if _, err := os.Stat(dest); err == nil {
		t.Fatal("an archive was written for a run with no logs")
	}
}
