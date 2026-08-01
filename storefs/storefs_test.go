package storefs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMeasureCountsBytesAndObjectsRecursively(t *testing.T) {
	dir := t.TempDir()
	die := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	die(os.MkdirAll(filepath.Join(dir, "wal", "epoch-1"), 0o755))
	die(os.WriteFile(filepath.Join(dir, "manifest.json"), make([]byte, 100), 0o644))
	die(os.WriteFile(filepath.Join(dir, "wal", "epoch-1", "seg1"), make([]byte, 4096), 0o644))
	fp, err := Measure(dir)
	die(err)
	if fp.Bytes != 4196 || fp.Objects != 2 {
		t.Fatalf("footprint = %+v", fp)
	}
}

func TestMeasureWorksOnASingleFileStore(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.zou")
	if err := os.WriteFile(path, make([]byte, 512), 0o644); err != nil {
		t.Fatal(err)
	}
	fp, err := Measure(path)
	if err != nil {
		t.Fatal(err)
	}
	if fp.Bytes != 512 || fp.Objects != 1 {
		t.Fatalf("footprint = %+v", fp)
	}
}
