# zou-bench

Benchmarks comparing vanilla Postgres 18, [zou](https://github.com/tamnd/zou), and Neon on identical workloads, with every metric captured and every number reproducible from a script in this repo.

The goal is honest measurement for the zou M1b targets: 10x Neon scale, 10x Neon throughput, 1/10 Neon cost, 1/10 of Neon's resource usage.
All three systems run self hosted on our own hardware (server1, server2, server3 on Linux, gamingpc on Windows), Neon via its own docker compose stack with safekeepers and pageserver, so the comparison is real processes on identical machines, not published numbers.
Numbers produced against simulated store latency are labeled simulated and get replaced by real S3 runs, and Neon cloud runs are allowed as separately labeled data points, never as the primary comparison.

## Running

The harness is a single Go binary with no dependencies beyond pgbench.
Every package lives at the module root, so `github.com/tamnd/zou-bench/cost`, `probe`, `pgbench`, and the rest import as a library from other projects, and `cmd/zoubench` is a thin CLI over the same code.

```
go build -o zoubench ./cmd/zoubench
export PGBIN=/path/to/pg18/bin
./zoubench run scenarios/tpcb-scale100.json --dsn "host=/tmp port=5432 dbname=postgres" --label pg18 --datadir /tmp/pgdata
./zoubench run scenarios/tpcb-scale100.json --dsn "host=/tmp port=5490 dbname=postgres" --label zou-minio --datadir /tmp/zou-data --storedir /tmp/zou-store --zoustats /tmp/zou-runtime/store-stats --pricecard minio-server3
./zoubench run scenarios/select-scale100.json --dsn "host=neon-host port=5432 dbname=postgres user=bench" --label neon-selfhosted
./zoubench report results/*.json
```

`run` executes one scenario against one server and writes a dated json result file.
`report` merges result files into markdown tables grouped by scenario, one row per label.

The server under test is started by you, not the harness, so each system's own startup procedure stays authoritative.
`scripts/` holds startup helpers for the local systems.

## What a run captures

From pgbench: tps, average latency and stddev, initial connection time, transactions processed and failed, per statement latencies from `-r`, and init wall time split into its phases (generate, vacuum, primary keys) when the scenario loads data.

From the per transaction log (`-l`): real latency percentiles p50, p90, p95, p99, p999, max, mean, and stddev over every transaction, plus 30 second buckets with per bucket tps, p50, and p99 so a flat average cannot hide a stall.

From the server process tree, sampled when `--datadir` names a local server: process count, RSS peak and median across the whole tree, an RSS timeline and its slope in kb per minute for leak detection, cumulative CPU seconds, major page faults, and process level disk read and write bytes on Linux via /proc.

From the whole system on Linux, sampled every second: per device disk reads, writes, and utilization, network bytes in and out, page cache growth, swap in and out, and a disk write timeline. On other platforms this block says supported false instead of inventing zeros.

From the server itself: version, non default settings, and before and after snapshots of pg_stat_wal, pg_stat_bgwriter, pg_stat_checkpointer, pg_stat_database, and aggregated pg_stat_io, reported as numeric deltas so wal bytes, fsyncs, checkpoints, and buffer evictions during the run are exact.

From the store when `--storedir` names the zou store path: bytes and object count before and after, the byte delta, and write amplification computed as store growth over wal bytes written.

From zou's own op counters when `--zoustats` names the counter file `ZOU_STORE_STATS` pointed at (`zou dev` keeps it at `<runtime>/store-stats`): per op kind and key class counts and bytes, latency p50/p95/p99, io errors, and CAS conflicts, taken as the difference between snapshots at run start and end so only this run's traffic counts. When these are present the cost block prices measured op counts instead of saying unmeasured.

From the price cards in `pricecards/` when `--pricecard` names one: storage dollars per month for the measured footprint, and op dollars only when op counts were actually measured, otherwise the field says unmeasured. Every card carries its source url and the date it was checked, and the self hosted cards amortize a real monthly box price over its disk.

Environment capture rounds it out: cpu model, core count, RAM, kernel, OS, and the filesystem and mount options behind the data dir and store dir, all recorded into the result json.

## Probing a provider

`zoubench probe` measures a real endpoint's latency curve and writes a calibration file that zou's `ZOU_STORE_SIM` loads in place of its built in profiles, so simulated runs replay numbers someone actually measured instead of numbers from a marketing page.

```
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
./zoubench probe --endpoint https://s3.us-east-1.amazonaws.com --bucket my-bucket --name aws-s3-standard
ZOU_STORE_SIM=pricecards/aws-s3-standard.calibration.json zou dev ...
```

The probe speaks plain SigV4 over path style requests, so it works against AWS, R2, GCS interop, B2, Wasabi, and MinIO with the same flags.
It runs put, get, list, and delete sequentially from the machine you invoke it on, which is the point: the file captures the latency zou would pay from that network position.
A few 16 MB round trips estimate throughput, throttle responses are counted into the profile's slowdown rate rather than the latency curve, and every key the probe writes is deleted before it exits.
The file lands next to the price cards by default, `pricecards/<name>.calibration.json`.

## Simulated runs and cost simulation

A run is stamped simulated when `ZOU_STORE_SIM` or `ZOU_STORE_DELAY` is set in the harness environment, or when `--simulated <spec>` says the server was started under one elsewhere.
Stamped runs carry the sim spec in the result json and wear a `(sim)` marker in every report table, per the M1b rules: simulated numbers hold a place until a real bucket run replaces them.

Cost simulation rides on the stamp: a run simulated as `s3-standard` prices its measured op counts with the matching aws-s3-standard card automatically, so a local MinIO run answers what the same workload would have cost on AWS.
The op counts are real, only the latency was simulated, and an explicit `--pricecard` still wins.
When op counts and transactions were both measured, the cost block adds `usd_per_million_txns`, the request dollars per million transactions, which the report shows as `$/M txns`.

## Scenarios

- `tpcb-scale100`: pgbench tpcb-like, scale 100, 8 clients, 60 s, with data load.
- `select-scale100`: pgbench select-only on the loaded scale 100 data.
- `tpcb-scale1000`: scale 1000, dataset larger than RAM on most boxes, 32 clients, 5 min.

Scenario files are plain json, add one per workload and keep them small and explicit.
Fields: name, init, scale, clients, threads, duration, warmup, builtin, script, rate.
warmup runs the same workload for that many seconds before the measured leg, script points at a custom pgbench script instead of a builtin, and rate caps the throughput for latency under load runs.

## Comparing fairly

- Same pgbench binary drives every system, set PGBIN once.
- Stock server settings on every system, tuning belongs in a separate labeled run.
- Neon runs against the self hosted stack on the same hardware as the other systems, cloud Neon runs get a `-cloud` suffix and a note on region and RTT.
- zou simulated store runs set `ZOU_STORE_DELAY` (see zou docs/perf.md) and must carry a `-sim` suffix in the label.

## Results

Result json files live under `results/`, curated tables under `docs/results/`, dated, with hardware and settings recorded in each file.
