package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A six hour soak on server2 finished every segment and every drill and
// then met a full disk on the last write of the run, which left a zero
// byte file where the numbers should have been. The file name is what a
// collector globs for, so it has to appear only when it holds a run.
func TestResultNeverLandsHalfWritten(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sustain-6h-server2-20260810T215546.json")
	if err := os.WriteFile(path+".part", []byte("leftover"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writeFileAtomic(path, []byte(`{"hours":6}`)); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != `{"hours":6}` {
		t.Fatalf("read back %q, %v", got, err)
	}
	if _, err := os.Stat(path + ".part"); !os.IsNotExist(err) {
		t.Fatalf("the temp file outlived the write")
	}
}

func TestAFullOutdirFallsBackInsteadOfLosingTheRun(t *testing.T) {
	// A directory nobody can write to stands in for the disk that
	// filled up, since the failure the harness has to survive is the
	// write failing, not the particular errno behind it.
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Skipf("cannot make an unwritable directory here: %v", err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o700) })
	if os.Geteuid() == 0 {
		t.Skip("root writes to it anyway")
	}

	name := "sustain-6h-server2-20260810T215546.json"
	path, err := writeResult(dir, name, []byte(`{"hours":6}`))
	if err != nil {
		t.Fatalf("the run was lost: %v", err)
	}
	t.Cleanup(func() { os.Remove(path) })
	if filepath.Dir(path) == dir {
		t.Fatalf("claimed to write into the unwritable directory: %s", path)
	}
	if filepath.Base(path) != name {
		t.Fatalf("the fallback renamed the run: %s", path)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != `{"hours":6}` {
		t.Fatalf("read back %q, %v", got, err)
	}
}

func TestStoreBytesCountsWhatIsThereAndAbstainsOtherwise(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "tenants", "local", "shards", "0000"), 0o755); err != nil {
		t.Fatal(err)
	}
	seg := filepath.Join(dir, "tenants", "local", "shards", "0000", "000001.seg")
	if err := os.WriteFile(seg, []byte(strings.Repeat("x", 4096)), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "MANIFEST"), []byte("m"), 0o644); err != nil {
		t.Fatal(err)
	}
	n, ok := storeBytes(dir)
	if !ok || n != 4097 {
		t.Fatalf("storeBytes = %d, %v", n, ok)
	}

	// A store in a bucket has a size, but not one this process can walk
	// to, and a zero would read as a store that costs nothing.
	if n, ok := storeBytes("s3://a-bucket/prefix"); ok {
		t.Fatalf("claimed to measure a bucket: %d", n)
	}
}
