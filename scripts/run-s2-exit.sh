#!/usr/bin/env bash
# S2 exit legs on one box: build a scale 100 cluster with the caches
# on, run the select-only leg through zoubench with the op counters
# scraped, then stop the server and run the cold attach leg against
# the same data directory. The point of doing both from one script is
# that the attach cycles inherit the caches the select leg warmed, so
# the run shows reads served from cache instead of the store, which is
# the exit claim under test.
# Usage: run-s2-exit.sh <pg-bin-dir> <zou-bin-dir> <workdir> <label> [store-target]
# store-target defaults to a localfs store under workdir, pass an
# s3:// url with credentials in the environment for a bucket run.
# PRICECARD names a price card for the cost block, SCALE overrides 100.
set -euo pipefail

PG_BIN=$1
ZOU_BIN=$2
WORK=$3
LABEL=$4
STORE=${5:-$WORK/store}
SCALE=${SCALE:-100}
PORT=5490
SOCK=$WORK
BENCH=$(cd "$(dirname "$0")/.." && pwd)
ZOUBENCH=${ZOUBENCH:-$BENCH/zoubench}

mkdir -p "$WORK"
export ZOU_TARGET="$STORE"
export ZOU_STORE_STATS="$WORK/store-stats"
export ZOU_PAGE_CACHE="$WORK/pagecache"
export ZOU_READ_CACHE_DIR="$WORK/slabcache"
export ZOU_TENANT=${ZOU_TENANT:-local}
case "$STORE" in s3://*) ;; *) mkdir -p "$STORE" ;; esac

PGDATA=$WORK/pgdata
"$PG_BIN/initdb" -D "$PGDATA" --set io_method=sync --set full_page_writes=off >"$WORK/initdb.log" 2>&1
REDO=$("$PG_BIN/pg_controldata" -D "$PGDATA" | grep "REDO location" | awk '{print $NF}')
"$ZOU_BIN/zou-bootstrap" "$ZOU_TARGET" "$PGDATA" --redo "$REDO"
"$PG_BIN/pg_ctl" -D "$PGDATA" -l "$PGDATA.log" -w -t 300 -o "-p $PORT -k $SOCK -c listen_addresses=''" start >/dev/null

echo "loading pgbench scale $SCALE"
"$PG_BIN/pgbench" -h "$SOCK" -p "$PORT" -i -s "$SCALE" postgres >"$WORK/pginit.log" 2>&1
"$PG_BIN/psql" -h "$SOCK" -p "$PORT" -d postgres -Atqc "checkpoint" >/dev/null
for _ in $(seq 1 120); do
    grep -q "folded a" "$PGDATA.log" && break
    sleep 1
done
grep -q "folded a" "$PGDATA.log" || { echo "the load never folded"; exit 1; }

STOREDIR=()
case "$STORE" in s3://*) ;; *) STOREDIR=(--storedir "$STORE") ;; esac
CARD=()
[ -n "${PRICECARD:-}" ] && CARD=(--pricecard "$PRICECARD")

export PGBIN="$PG_BIN"
"$ZOUBENCH" run "$BENCH/scenarios/select-scale100.json" \
    --dsn "host=$SOCK port=$PORT dbname=postgres" \
    --label "$LABEL" --datadir "$PGDATA" \
    --zoustats "$ZOU_STORE_STATS" "${STOREDIR[@]}" "${CARD[@]}"

"$PG_BIN/pg_ctl" -D "$PGDATA" stop -m fast >/dev/null

"$ZOUBENCH" attach "$BENCH/scenarios/cold-attach.json" \
    --pgbin "$PG_BIN" --datadir "$PGDATA" --sockdir "$SOCK" \
    --label "$LABEL" --zoustats "$ZOU_STORE_STATS"

"$ZOU_BIN/zou" stats "$ZOU_STORE_STATS" >"$WORK/store-stats.json"
echo "full counter dump: $WORK/store-stats.json"
