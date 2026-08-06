#!/bin/sh
# Start a vanilla postgres with PostgREST in front of it, so the same
# rest scenario can be asked of the thing zou is compatible with. The
# jwt secret matches the zou leg's, which is what lets one harness mint
# tokens both servers accept.
# Usage: start-postgrest.sh <rundir> [pgport] [httpport]
set -eu

RUNDIR=${1:?usage: start-postgrest.sh <rundir> [pgport] [httpport]}
PGPORT=${2:-54411}
HTTPPORT=${3:-54421}
PGBIN=${PGBIN:?set PGBIN to a vanilla pg bin directory}
POSTGREST=${POSTGREST:-postgrest}
SECRET=${ZOU_JWT_SECRET:-super-secret-jwt-token-with-at-least-32-characters-long}

mkdir -p "$RUNDIR" "$RUNDIR/sock"
if [ ! -d "$RUNDIR/pgdata" ]; then
    "$PGBIN/initdb" -D "$RUNDIR/pgdata" >"$RUNDIR/initdb.log" 2>&1
fi
# The socket goes in the rundir: a packaged postgres defaults it to
# /var/run/postgresql, which the user running a benchmark cannot write.
"$PGBIN/pg_ctl" -D "$RUNDIR/pgdata" -l "$RUNDIR/pg.log" \
    -o "-p $PGPORT -k $RUNDIR/sock -c listen_addresses=127.0.0.1" start -w -t 120

# The password is on the loopback of a benchmark box and the harness
# needs it in a uri, so it is fixed rather than generated.
"$PGBIN/psql" -h 127.0.0.1 -p "$PGPORT" -d postgres -v ON_ERROR_STOP=1 -q <<SQL
do \$\$ begin
  if not exists (select 1 from pg_roles where rolname = 'authenticator') then
    create role authenticator login noinherit password 'bench';
  else
    alter role authenticator login password 'bench';
  end if;
end \$\$;

-- PostgREST reads the schema once at startup and then only when it is
-- told. The scenario's setup file runs after this script, so without
-- the watch trigger every request in the run answers 404 against a
-- cache that predates the tables.
create or replace function pgrst_watch() returns event_trigger
  language plpgsql as \$\$
begin
  notify pgrst, 'reload schema';
end;
\$\$;
drop event trigger if exists pgrst_watch;
create event trigger pgrst_watch on ddl_command_end execute procedure pgrst_watch();
SQL

PGRST_DB_URI="postgres://authenticator:bench@127.0.0.1:$PGPORT/postgres" \
PGRST_DB_SCHEMAS=public \
PGRST_DB_ANON_ROLE=anon \
PGRST_JWT_SECRET="$SECRET" \
PGRST_SERVER_PORT="$HTTPPORT" \
PGRST_DB_POOL=16 \
    nohup "$POSTGREST" >"$RUNDIR/postgrest.log" 2>&1 &
echo $! >"$RUNDIR/postgrest.pid"

i=0
while [ "$i" -lt 120 ]; do
    code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$HTTPPORT/" || true)
    if [ "$code" != "000" ]; then
        break
    fi
    i=$((i + 1))
    sleep 1
done
if [ "$code" = "000" ]; then
    echo "postgrest never answered on $HTTPPORT, see $RUNDIR/postgrest.log" >&2
    exit 1
fi

echo "url: http://127.0.0.1:$HTTPPORT"
echo "dsn: host=127.0.0.1 port=$PGPORT dbname=postgres user=$(id -un)"
echo "datadir: $RUNDIR/pgdata"
echo "secret: $SECRET"
echo "stop: kill \$(cat $RUNDIR/postgrest.pid); $PGBIN/pg_ctl -D $RUNDIR/pgdata stop -m immediate"
