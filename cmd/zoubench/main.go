// zoubench drives pgbench against whatever a DSN points at, or the rest
// api over http with the rest subcommand, samples the
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
	case "rest":
		cmdREST(os.Args[2:])
	case "report":
		cmdReport(os.Args[2:])
	case "probe":
		cmdProbe(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: zoubench run <scenario.json> --dsn <dsn> --label <label> [--datadir <dir>] [--storedir <path>] [--pricecard <name>] [--simulated <spec>] [--outdir <dir>]")
	fmt.Fprintln(os.Stderr, "       zoubench rest <scenario.json> --url <base url> --label <label> [--jwt-secret <s>] [--dsn <dsn>] [--datadir <dir>] [--zoustats <path>] [--outdir <dir>]")
	fmt.Fprintln(os.Stderr, "       zoubench report <results...>")
	fmt.Fprintln(os.Stderr, "       zoubench probe --endpoint <url> --bucket <bucket> --name <profile> [--region <r>] [--samples <n>] [--payload <bytes>]")
	os.Exit(2)
}
