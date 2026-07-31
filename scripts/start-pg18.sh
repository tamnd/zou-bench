#!/bin/sh
# Start a stock Postgres 18 for baseline runs, io_method=sync to match
# the zou bench settings. Usage: start-pg18.sh <rundir> [port]
set -eu

RUNDIR=${1:?usage: start-pg18.sh <rundir> [port]}
PORT=${2:-54311}
PG=${PGBIN:?set PGBIN to the pg18 bin directory}

mkdir -p "$RUNDIR"
DATADIR=$RUNDIR/data
SOCK=$RUNDIR/sock
mkdir -p "$SOCK"

"$PG/initdb" -D "$DATADIR" --set io_method=sync >"$RUNDIR/initdb.log" 2>&1
"$PG/pg_ctl" -D "$DATADIR" -l "$RUNDIR/server.log" \
    -o "-p $PORT -k $SOCK" start

echo "dsn: host=$SOCK port=$PORT dbname=postgres"
echo "datadir: $DATADIR"
echo "stop: $PG/pg_ctl -D $DATADIR stop"
