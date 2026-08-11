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
`attach` measures cold attach: it spawns the postmaster itself, polls the socket every millisecond with a built in wire protocol client until one real query answers, and records connect, first row, and stop times per cycle. pg_ctl -w checks readiness ten times a second and psql pays process startup, both too coarse for a budget measured in hundreds of milliseconds, which is why the poll is our own.
`report` merges result files into markdown tables grouped by scenario, one row per label.

```
./zoubench attach scenarios/cold-attach.json --pgbin /path/to/pg18/bin --datadir /tmp/zou-data --zoustats /tmp/zou-runtime/store-stats --label zou-minio
```

The server under test is started by you, not the harness, except in `attach` where starting the server is the thing being measured.
`scripts/` holds startup helpers for the local systems.

## What a run captures

From pgbench: tps, average latency and stddev, initial connection time, transactions processed and failed, per statement latencies from `-r`, and init wall time split into its phases (generate, vacuum, primary keys) when the scenario loads data.

From the per transaction log (`-l`): real latency percentiles p50, p90, p95, p99, p999, max, mean, and stddev over every transaction, plus 30 second buckets with per bucket tps, p50, and p99 so a flat average cannot hide a stall.

From the server process tree, sampled when `--datadir` names a local server: process count, RSS peak and median across the whole tree, an RSS timeline and its slope in kb per minute for leak detection, cumulative CPU seconds, major page faults, and process level disk read and write bytes on Linux via /proc.

From the whole system on Linux, sampled every second: per device disk reads, writes, and utilization, network bytes in and out, page cache growth, swap in and out, and a disk write timeline. On other platforms this block says supported false instead of inventing zeros.

From the server itself: version, non default settings, and before and after snapshots of pg_stat_wal, pg_stat_bgwriter, pg_stat_checkpointer, pg_stat_database, and aggregated pg_stat_io, reported as numeric deltas so wal bytes, fsyncs, checkpoints, and buffer evictions during the run are exact.

From the store when `--storedir` names the zou store path: bytes and object count before and after, the byte delta, and write amplification computed as store growth over wal bytes written.

From zou's own op counters when `--zoustats` names the counter file `ZOU_STORE_STATS` pointed at (`zou dev` keeps it at `<runtime>/store-stats`): per op kind and key class counts and bytes, latency p50/p95/p99, io errors, and CAS conflicts, taken as the difference between snapshots at run start and end so only this run's traffic counts. The same file carries the smgr read tier counters, calls, pages, and latency percentiles per tier, cache for local page cache hits, local for reconstruction that never left the process, store for reads that paid a store round trip, which is how a run shows its reads were served from cache instead of the store. When these are present the cost block prices measured op counts instead of saying unmeasured.

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

## Fleet runs

`zoubench fleet` measures a node holding many projects instead of one project answering fast.

```
./zoubench fleet scenarios/fleet-1000.json \
    --zoubin /path/to/zou --pgbin /path/to/pg18/bin \
    --store /srv/fleet-store --workdir /srv/fleet-run \
    --cpus 0-7 --label zou-fleet
```

The node is started by the harness, unlike `run` and `rest`, because half of what is measured is what the node does when nobody asked it to: the attach a request triggers, the eviction at the ceiling, and the memory the whole process tree settles at.
`--cpus` pins it with taskset, which is how a box with more cores than the node is supposed to have still measures that node: the harness and the traffic it generates must not be sharing the cores the answer is about.

Four phases, chosen with `--phases`.
`provision` registers every ref, applies the scenario's `setup` sql over the postgres door, which is the step that runs initdb and captures the genesis, and sends one http request, which is what applies the tenant contract the api needs.
`steady` draws requests from a working set the node's ceiling can hold, so nothing is evicted and the answer is what a packed node costs when the projects being used fit.
`hold` asks for nothing at all and watches, which is the long tail measurement described below.
`churn` draws from every tenant, so with a ceiling below the fleet size the node attaches and evicts for the whole window, and the gap between the two phases' tails is the number a fleet is sized on.
All four run by default, and a scenario with no `hold` in it skips that one on its own.

A steady phase only measures a packed node if its warmup outlasts attaching the whole working set, which at 16 clients and a several second attach is minutes rather than seconds.
Size `warmup` at working set times attach divided by clients and then some, or the measured window reports the attach storm instead of the node, and the difference between the two is three orders of magnitude at the tail.

Provisioning a thousand tenants is a thousand initdbs, so it is written down as it goes.
`<workdir>/fleet-state.json` names every ref that has a database, a table and rows, and a second run against the same store skips them, which is what makes a fleet run resumable and lets a measuring phase be repeated with a different ceiling for free.
A state file that names a different store, or one written with a different jwt secret, is refused rather than trusted.

Every tenant in a fleet is created with the same jwt secret, so token minting stays off the request path.
Each still has its own registry entry, its own database and its own prefix, and HS256 verification costs the same whatever the key is, so nothing measured changes.

A fleet result carries both sides of the story: what the client waited for, as percentiles and 30 second buckets per phase, and what the node says it was doing, read off its ops port as attach counts, the attach latency histogram, registry cache hits and misses, and the attached gauge.
The process tree sampler gives RSS peak, median, timeline and slope across every postmaster the node started, which is the memory ceiling claim, and the runtime directory footprint is measured at the end of each phase, which is the disk one.

Fleet scenario fields on top of the shared ones: `tenants`, `working_set`, `max_attached`, `idle_secs`, `shared_buffers`, `hold`, `sample_secs`, `settle_secs`, and `setup`.
The node's budgets live in the scenario because they are the shape of the deployment being measured rather than tuning: a ceiling below the fleet size is what makes the churn phase churn.

### The long tail

The steady and churn phases measure a node under load, which is the number a fleet is sized on.
The long tail is the other question: eight hundred projects that are mostly asleep, where the bill is not throughput but whatever the node does on its own, forever, per project.
That is a rate rather than a total, and it is only a rate if it is watched over a window with nothing in it, which is what the `hold` phase is.

For `hold` seconds it sends no requests and every `sample_secs` it reads the attached gauge off the ops port and the store counters out of zou's `ZOU_STORE_STATS` file.
Its first `settle_secs` are sampled and then left out of the rates, because a node that has just been under load is still writing the load down, and that work is the load's cost rather than a sleeping project's.
The settled samples stay in the result file, so what was dropped and how much was still going on when the window opened are both visible.
Each interval is then classified by what was attached at the start of it, so a hold longer than `idle_secs` answers both halves at once: the projects the steady phase left attached are let go partway through, and the rest of the same window is a node holding nothing.
An interval whose counters went backwards is dropped rather than counted as negative work, because a counter only shrinks when the node restarted underneath the sampler.
The two rates that come out are `dormant_per_hour`, what one node costs with nothing attached, and `attached_per_project_hour`, which is the attached rate with the node's own dormant rate subtracted before dividing by project hours.

`--pricecard <names>` turns those rates into a monthly bill, one block per named card.
A month is 730 hours of the dormant rate plus 730 hours of the attached rate times the ceiling, priced per op, plus the store bytes measured at the end of the run, and `usd_per_project_month` divides by the fleet size.
The store is only half the bill, so `--box-usd-month` and `--box-source` carry the price of the box the node runs on, from an invoice rather than a guess.
Without them the compute line reads `not priced` instead of zero, and the per project number then covers the store alone, which is what `store_usd_per_project_month` always reports.

## Scenarios

- `tpcb-scale100`: pgbench tpcb-like, scale 100, 8 clients, 60 s, with data load.
- `select-scale100`: pgbench select-only on the loaded scale 100 data.
- `tpcb-scale1000`: scale 1000, dataset larger than RAM on most boxes, 32 clients, 5 min.
- `rest-warm-reads`: the REST read mix a small app makes, 8 clients, 60 s, against the tenant `rest-demo.sql` builds.
- `fleet-1000`: a thousand small projects on one node with a hundred attached at once, a steady phase over a working set that fits and a churn phase over all thousand.
- `fleet-1000-warm`: the same thousand tenants with a ten minute warmup, long enough that the working set is attached before the measured window opens.
- `fleet-800-idle`: eight hundred mostly asleep projects, a hundred attached at once and ninety minutes of hold against a ten minute idle budget, which is the long tail cost scenario.
- `fleet-smoke`: the same shape at ten tenants, for checking the harness works before spending half an hour on initdb.

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

## The dashboard

`docs/dashboard.md` is the one page saying which published claims the benchmarks have actually earned, and it is generated rather than written.

```
zoubench dashboard [--targets docs/targets.json] [--book docs/dashboard.json] [--out docs/dashboard.md] [results...]
```

`docs/targets.json` is the list of claims.
Each one names the scenario and optionally the label that can answer it, a dotted path into the result file, the unit the claim is written in, and which side of which line the number has to land on, along with the milestone line it earns by landing there.
A path segment is a key, an array index, or `field=value`, which picks an element out of a list by name: `cost.card=aws-s3-standard.total_usd_month` reads the same card whatever order the price card flags were given in.
`divide_by` converts what the result file stores into what the claim is written in, kilobytes into gigabytes and the like.

A target with no `compare` is reported rather than judged.
That is for the headline numbers no milestone has written a line under yet, and it is deliberate: a budget invented to make the table look finished is worse than a table that says which of its numbers are context.

Runs happen on whichever machine can run them and the raw json stays out of git, so the readings are kept in `docs/dashboard.json`, merged one run at a time, and committed next to the page.
Hand the command whatever result files the machine has, it updates their rows and leaves the rest alone, and the newest run of a scenario wins whatever order the files arrive in.
Run it with no result files to re-render after editing the targets file, which is how a claim gets reworded or given a tighter line.
The book stores each reading already converted into the claim's units, so changing `divide_by` on a target means handing that scenario's result file over again.

A row is met when the measured number is on the right side of the line and nothing else counts.
A run against a simulated store is never met whatever the number says, per the M1b rule that a simulated number only holds a place until a real bucket replaces it.
A claim nothing has measured yet says so on the page instead of being left off it.
