#!/bin/sh
# Start `zou serve` as the node under test for a socket run, with one
# registered project on it, and wait until the api answers for that
# project. Usage: start-zou-serve.sh <rundir> <ref> [store] [httpports] [pgport] [opsport]
#
# `zou serve` rather than `zou dev`, because dev binds loopback and a
# socket run has its generator on another box. The http ports are a
# comma separated list because one client address has only its ephemeral
# range worth of ports to any one destination port, which is 28231 by
# default and not the 65535 the arithmetic wants to use. Well before the
# range is full the kernel is scanning most of it per connect under a
# hash bucket lock, so at 50k sockets on three ports a generator spends
# 70 percent of itself in __inet_hash_connect and the ramp falls from
# 350 connects a second to 55. Six ports and a widened range on the
# generator keep each destination about a third full. See the socket
# runs section of the README for the generator side of this.
#
# The realtime ceilings are raised rather than turned off, so the
# counting a node does per socket and per change stays in the measured
# path and only the refusal is out of the way.
set -eu

RUNDIR=${1:?usage: start-zou-serve.sh <rundir> <ref> [store] [httpports] [pgport] [opsport]}
REF=${2:?a project ref to serve}
STORE=${3:-$RUNDIR/store}
HTTPPORTS=${4:-54341,54342,54343}
PGPORT=${5:-54345}
OPSPORT=${6:-9464}
ZOU=${ZOU:?set ZOU to the zou binary}
PGBIN=${PGBIN:?set PGBIN to the patched pg bin directory}
SECRET=${ZOU_JWT_SECRET:-super-secret-jwt-token-with-at-least-32-characters-long}
FIRSTPORT=$(echo "$HTTPPORTS" | cut -d, -f1)

mkdir -p "$RUNDIR"
case "$STORE" in
s3://* | http://* | https://*) ;;
*) mkdir -p "$STORE" ;;
esac

"$ZOU" tenant "$STORE" create "$REF" --secret "$SECRET" 2>&1 | grep -v "already" || true

ZOU_REALTIME_MAX_CONCURRENT_USERS=1000000 \
    ZOU_REALTIME_MAX_JOINS_PER_SECOND=100000 \
    ZOU_REALTIME_MAX_CHANNELS_PER_CLIENT=100 \
    ZOU_REALTIME_MAX_EVENTS_PER_SECOND=10000000 \
    ZOU_REALTIME_MAX_PAYLOAD_SIZE_IN_KB=1024 \
    ZOU_STORE_STATS="$RUNDIR/store-stats" \
    nohup "$ZOU" serve "$STORE" \
    --pg-bin "$PGBIN" --runtime "$RUNDIR/runtime" \
    --http "$HTTPPORTS" --pg "$PGPORT" --pool 0 --ops "$OPSPORT" \
    --max-attached 4 --idle-secs 86400 --shared-buffers 256MB \
    >"$RUNDIR/zou.log" 2>&1 &
echo $! >"$RUNDIR/zou.pid"

# The first request for a cold project runs initdb, captures a genesis
# checkpoint and applies the tenant contract, which is minutes rather
# than seconds on a slow disk. It is also the request that makes the api
# side of the project real, so it is the wait and the provisioning both.
ANON=$("$ZOU" tenant "$STORE" keys "$REF" --env | sed -n 's/^ANON_KEY="\(.*\)"$/\1/p')
SERVICE=$("$ZOU" tenant "$STORE" keys "$REF" --env | sed -n 's/^SERVICE_ROLE_KEY="\(.*\)"$/\1/p')
i=0
while [ "$i" -lt 900 ]; do
    code=$(curl -s -o /dev/null -w '%{http_code}' \
        -H "apikey: $ANON" -H "authorization: Bearer $SERVICE" \
        "http://127.0.0.1:$FIRSTPORT/$REF/rest/v1/" || true)
    if [ "$code" = "200" ]; then
        break
    fi
    i=$((i + 1))
    sleep 1
done
if [ "$code" != "200" ]; then
    echo "the project $REF never answered on $FIRSTPORT, last status $code, see $RUNDIR/zou.log" >&2
    exit 1
fi

echo "url: http://$(hostname -I 2>/dev/null | awk '{print $1}'):$FIRSTPORT/$REF"
echo "ports: $HTTPPORTS"
echo "dsn: host=127.0.0.1 port=$PGPORT dbname=postgres user=postgres.$REF"
echo "ops: http://127.0.0.1:$OPSPORT/metrics"
echo "secret: $SECRET"
echo "stop: kill \$(cat $RUNDIR/zou.pid)"
