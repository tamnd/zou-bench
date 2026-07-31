// zoubench drives pgbench against whatever a DSN points at, samples the
// server process tree the whole time, parses pgbench's per transaction
// log for real percentiles, and writes one json result file per run.
// The report subcommand merges result files into markdown tables. The
// harness never tunes the server, it only measures.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "run":
		cmdRun(os.Args[2:])
	case "report":
		cmdReport(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: zoubench run <scenario.json> --dsn <dsn> --label <label> [--datadir <dir>] [--outdir <dir>]")
	fmt.Fprintln(os.Stderr, "       zoubench report <results...>")
	os.Exit(2)
}
