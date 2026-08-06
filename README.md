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
`rest` does the same over http against a REST api instead of over the wire protocol.
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

## REST runs

`zoubench rest` drives a Supabase style REST api the way an application does, over keep alive connections with a mix of request shapes, and writes the same result json shape `run` writes.
The numbers land under the same keys, `tps` and `latency_ms` and the rest, so a REST run and a pgbench run merge into the same report tables and the same result book.

```
ZOU=/path/to/zou PGBIN=/path/to/pg/bin scripts/start-zou-dev.sh /tmp/zoudev
./zoubench rest scenarios/rest-warm-reads.json \
    --url http://127.0.0.1:54321/rest/v1 --label zou-rest \
    --jwt-secret super-secret-jwt-token-with-at-least-32-characters-long \
    --dsn "host=127.0.0.1 port=54311 dbname=postgres user=$(id -un)" \
    --datadir /tmp/zoudev/runtime/pgdata --zoustats /tmp/zoudev/runtime/store-stats
```

The scenario's `setup` names a sql file applied with psql before the run, so the tenant a run measures is created by the run itself and two boxes measure the same rows.
The harness mints its own tokens from `--jwt-secret`, an anon key, a service key, and a user token per `--user`, which is why the secret is a flag rather than something copied out of a log.
Each request in the workload names the token it goes out with, so an anon read and an authenticated read under RLS are separate lines in the result rather than one blended average.

A request that answers with a status the workload did not expect is counted as an error and its time is thrown away, not averaged in.
This matters more than it sounds: a wall of 401s is the fastest a server ever answers, and a harness that timed them would publish its best numbers on the day it was most broken.
The run prints a warning naming the first few, and the result json keeps them under `failures`.

Beyond the pgbench block, a REST run records per request latency distributions under `per_request`, the status code histogram, bytes read, and 30 second buckets, and it takes the same pg_stat, process tree, system, and zou op counter captures a `run` takes when the flags name a local server.

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
- `rest-warm-reads`: the REST read mix a small app makes, 8 clients, 60 s, against the tenant `rest-demo.sql` builds.

Scenario files are plain json, add one per workload and keep them small and explicit.
Fields: name, init, scale, clients, threads, duration, warmup, builtin, script, rate.
warmup runs the same workload for that many seconds before the measured leg, script points at a custom pgbench script instead of a builtin, and rate caps the throughput for latency under load runs.

A scenario with `kind` set to `rest` is driven by `zoubench rest` instead, and adds two fields: `setup`, the sql file applied before the run, and `requests`, the workload itself.
A request carries a name, a method and path, a weight in the mix, the token it uses (`anon`, `service`, or `user`), an optional body and Prefer header, and the status it expects.
`{{rand:lo:hi}}` and `{{hex}}` in a path are substituted per request, so a row by primary key run reads a different row every time rather than one very warm page.

Paths are written without an api prefix and `--url` carries it, which is what lets one scenario ask the same questions of two servers that mount their api in different places: zou answers under `/rest/v1`, a bare PostgREST answers at the root.

## Comparing fairly

- Same pgbench binary drives every system, set PGBIN once.
- Stock server settings on every system, tuning belongs in a separate labeled run.
- Neon runs against the self hosted stack on the same hardware as the other systems, cloud Neon runs get a `-cloud` suffix and a note on region and RTT.
- zou simulated store runs set `ZOU_STORE_DELAY` (see zou docs/perf.md) and must carry a `-sim` suffix in the label.

## Results

Result json files live under `results/`, curated tables under `docs/results/`, dated, with hardware and settings recorded in each file.
