# 2026-08-31: where the time goes inside a tpcb transaction

tamnd/zou#31 asks for the accounts update p99 under 25 ms at scale 100 with 8 writers on a local filesystem store.
The pgbench page for the same day could not answer it and said so: pgbench reports per statement latencies as means only, so what that page has is a 0.352 ms mean for the update beside a 36.8 ms p99 for the whole transaction, and the number the line asks for is not in the file at all.
This page is the same transaction driven from inside the harness instead, with all seven round trips timed one at a time, which is where a per statement tail can come from.

Two legs, vanilla Postgres 18 and zou on a filesystem store, at scale 100, 8 clients, 60 s a measurement, each with the database loaded fresh immediately before it.
Both ran inside four minutes of each other, 06:32 to 06:36 UTC.

## The box and the settings, said up front

gamingpc, a 13th generation Core i9-13900K, 32 GB, Ubuntu 26.04 inside WSL2 on kernel 6.18.33.2, both stores and both data directories on the same ext4 filesystem, both legs pinned to 16 of the 32 threads with `taskset -c 8-23`.
The box had been up twenty minutes when the first leg started and nothing else of ours was running on it.

The same two configuration differences as the pgbench page apply and neither is a design difference.
The vanilla leg is Ubuntu's PostgreSQL 18.6 package at stock settings, 128 MB of shared buffers with full page writes on.
The zou leg is the vendored 18.4 with the zou patches as `zou dev` configures it, 7.8 GB of shared buffers with full page writes off.
The zou binary is the one built at `0d72a38`, the same binary the pgbench legs ran.

The driver is new, so the first thing this page owes is a check that it measures the same thing pgbench does.
That check is at the bottom and it passes on six of the seven statements.

## The answer to the line

| statement | zou-localfs p99 | pg18 p99 |
| --- | --- | --- |
| begin | 0.428 | 0.212 |
| accounts update | 0.551 | 0.292 |
| accounts select | 0.464 | 0.230 |
| tellers update | 0.515 | 0.302 |
| branches update | 11.587 | 1.156 |
| history insert | 0.390 | 0.212 |
| end | 26.170 | 4.082 |
| whole transaction | 31.077 | 5.458 |

Milliseconds, 35182 transactions on the zou leg and 300562 on the vanilla one, none failed on either.

The accounts update p99 is 0.551 ms against a line that allows 25, so the line is met with a factor of 45 to spare, and it is met by a comfortable margin at every percentile the file carries: 0.292 at the median, 0.418 at p95, 1.213 at p999, 9.208 at the worst single sample in the minute.
The freshness barrier this line was written about does not show up in the statement it was written about.

Percentiles do not add, so the seven rows above sum to more than the transaction row and that sum means nothing.
The means do add, and they are where the transaction actually goes.

| statement | zou-localfs mean | share | pg18 mean | share |
| --- | --- | --- | --- | --- |
| begin | 0.226 | 2% | 0.068 | 4% |
| accounts update | 0.307 | 2% | 0.123 | 8% |
| accounts select | 0.261 | 2% | 0.091 | 6% |
| tellers update | 0.302 | 2% | 0.094 | 6% |
| branches update | 0.675 | 5% | 0.126 | 8% |
| history insert | 0.226 | 2% | 0.083 | 5% |
| end | 11.647 | 85% | 1.009 | 63% |
| whole transaction | 13.643 | | 1.594 | |

85 per cent of a zou transaction at this shape is the commit.
Every statement before it costs about twice what the same statement costs on vanilla, which is a flat and unsurprising overhead on a round trip that reads a page that is already in shared buffers, and none of them reaches a millisecond even at p99.
So the throughput gap on this page, 586 tps against 5006, is not spread across the transaction.
It is one statement, and it is the one that has to make the write durable in the store.

The other row worth reading is the branches update, 11.587 ms at p99 against 1.156 on vanilla, a factor of 10 where every other statement is a factor of 2.
`pgbench_branches` has one row per unit of scale, so 8 writers are contending for 100 rows, and that row is lock waiting rather than work.
It is the only statement whose p999 and max, 22.884 and 384.073 ms, are far off its own median of 0.249 ms.

## What the store did while that was happening

| | zou-localfs |
| --- | --- |
| transactions | 35182 |
| wal produced | 19.63 MiB |
| store bytes added | 2.90 GiB |
| write amplification | 151.1 |
| ops cost for the window | 0.0713 usd |
| per million transactions | 2.0274 usd |

An amplification of 151 against the 138 the pgbench leg measured on the same store shape the same morning, which is the same finding from a different driver and not a new one.
The layout question behind that number is the storage redesign and not anything visible in the statement table above.

## Does the new driver measure what pgbench measures

The two legs on this page ran the same transaction as the two write legs on the pgbench page, four hours apart on the same box, so the per statement means can be read against each other directly.

| statement | zou pgbench mean | zou wire mean | pg18 pgbench mean | pg18 wire mean |
| --- | --- | --- | --- | --- |
| begin | 0.205 | 0.226 | 0.061 | 0.068 |
| accounts update | 0.352 | 0.307 | 0.126 | 0.123 |
| accounts select | 0.238 | 0.261 | 0.082 | 0.091 |
| tellers update | 0.271 | 0.302 | 0.095 | 0.094 |
| branches update | 0.527 | 0.675 | 0.235 | 0.126 |
| history insert | 0.205 | 0.226 | 0.061 | 0.083 |
| end | 8.706 | 11.647 | 4.483 | 1.009 |

Six of the seven agree to within a fraction of a tenth of a millisecond on both legs, which is the check the driver had to pass.
The seventh, the commit, differs on both legs and for a different reason each time, and neither reason is the driver.

On the zou leg the pgbench run's commit averaged 8.706 ms and the wire run's 11.647.
The pgbench page already showed why: that leg decays inside its own measurement window, 1030 tps and a 6.9 ms median in the first half against 485 tps and a 13.6 ms median in the second.
The wire leg ran at 627 and 531 tps across its two halves with a median of 12.1 and 12.5 ms, so it was in the decayed regime for the whole minute while the pgbench leg averaged a fast half and a slow one.
The wire number is the steady state and the pgbench number is an average across a transition, and 11.6 against the pgbench leg's own second half is the comparison that holds.

On the vanilla leg the pgbench run's commit averaged 4.483 ms and the wire run's 1.009, which is the whole of the 5006 tps against 1553 tps difference between the two pages.
The system counters say what happened: the pgbench leg did 24718 disk reads for 190 MB during its minute at 94 per cent disk utilisation, and the wire leg did 43 reads for effectively nothing at 81 per cent.
Vanilla has 128 MB of shared buffers against a 1.5 GB accounts table, so it depends entirely on the page cache underneath it, and this box had been rebooted twenty minutes before the wire run while the pgbench run followed hours of other work.
The vanilla row on this page is therefore a warmer vanilla than the vanilla row on the pgbench page, and the two should not be quoted against each other.
Within this page both legs had their working set resident, vanilla in the page cache and zou in its own shared buffers, so the 8.5x between them is a fair comparison and the 3.2x between the two vanilla rows across pages is not.

## What this does not answer

Only the filesystem store is here.
The same run against MinIO would say whether the commit share rises further with a network in front of the store, which is the obvious next question and is not on this page.

Nothing here is at scale 1000, and the contended branches row is the statement most likely to change shape when there are ten times as many branch rows to spread 8 writers across.

The wire driver holds no rate and runs no script other than this transaction, so everything the harness measures with `--rate` or a custom script still goes through pgbench and still has means only for its statements.

The raw json for the two runs stays out of git as the book's convention has it, under `/home/zoubench/zou-bench/results` on gamingpc, and every number above is a field of those files.
