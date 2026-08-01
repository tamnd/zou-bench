// Package storefs measures the footprint of a store that lives on the
// local filesystem: a directory of objects, a single .zou file, or a
// sqlite database. Footprint before and after a run gives the space
// side of the story, and against pg_stat_wal's wal_bytes it gives
// write amplification, both without any cooperation from the server.
package storefs

import (
	"io/fs"
	"path/filepath"
)

type Footprint struct {
	Bytes   int64 `json:"bytes"`
	Objects int   `json:"objects"`
}

// Measure walks path if it is a directory, or stats it if it is a
// file. Errors inside the walk skip the entry, a store being actively
// written always has files appearing and vanishing.
func Measure(path string) (Footprint, error) {
	var fp Footprint
	err := filepath.WalkDir(path, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		fp.Bytes += info.Size()
		fp.Objects++
		return nil
	})
	return fp, err
}
