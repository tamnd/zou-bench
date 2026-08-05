-- One single row update per transaction, so the per transaction latency
-- pgbench logs is the commit wait plus a buffer touch that costs
-- microseconds. This is the commit latency scenario for the S1 exit
-- gate: p50 under 10 ms on the express profile, under 60 ms on
-- standard, at 8 clients.
\set aid random(1, 100000 * :scale)
UPDATE pgbench_accounts SET abalance = abalance + 1 WHERE aid = :aid;
