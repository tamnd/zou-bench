#!/bin/sh
# Start `zou dev` with a fixed jwt secret and wait until the http api
# answers, so a rest run does not measure a server that was still
# opening its store. Usage: start-zou-dev.sh <rundir> [store] [pgport] [httpport]
#
# The secret is fixed on purpose: the harness mints its own tokens from
# it, and a generated one would mean copying a key out of a log file
# into a command line on every run.
set -eu

RUNDIR=${1:?usage: start-zou-dev.sh <rundir> [store] [pgport] [httpport]}
STORE=${2:-$RUNDIR/store}
PGPORT=${3:-54311}
HTTPPORT=${4:-54321}
ZOU=${ZOU:?set ZOU to the zou binary}
PGBIN=${PGBIN:?set PGBIN to the patched pg bin directory}
SECRET=${ZOU_JWT_SECRET:-super-secret-jwt-token-with-at-least-32-characters-long}
# `zou dev` names the cluster superuser rather than taking it from the
# account that started the process, so it is postgres here and postgres
# on a box where the runs happen to be driven as somebody else. It is
# `SUPERUSER` in crates/zou/src/dev.rs, and it is named there so a store
# one command initialised can be opened by the other.
PGUSER=${PGUSER:-postgres}

mkdir -p "$RUNDIR" "$STORE"
ZOU_JWT_SECRET=$SECRET nohup "$ZOU" dev "$STORE" \
    --pg-bin "$PGBIN" --port "$PGPORT" --http "$HTTPPORT" \
    --runtime "$RUNDIR/runtime" >"$RUNDIR/zou.log" 2>&1 &
echo $! >"$RUNDIR/zou.pid"

# A fresh store runs initdb and captures a genesis checkpoint first,
# which is minutes rather than seconds on a slow disk.
i=0
while [ "$i" -lt 600 ]; do
    code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$HTTPPORT/rest/v1/" || true)
    # 401 is the api refusing an unauthenticated request, which means
    # it is up. Anything that is not a connection failure will do.
    if [ "$code" != "000" ]; then
        break
    fi
    i=$((i + 1))
    sleep 1
done
if [ "$code" = "000" ]; then
    echo "zou dev never answered on $HTTPPORT, see $RUNDIR/zou.log" >&2
    exit 1
fi

echo "url: http://127.0.0.1:$HTTPPORT"
echo "dsn: host=127.0.0.1 port=$PGPORT dbname=postgres user=$PGUSER"
echo "datadir: $RUNDIR/runtime/pgdata"
echo "zoustats: $RUNDIR/runtime/store-stats"
echo "secret: $SECRET"
echo "stop: kill \$(cat $RUNDIR/zou.pid)"
