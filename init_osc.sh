#!/bin/bash
[ $# -lt 3 ] && echo "Usage: $0 psql_database_name <extract.osm.pbf> <tags.lst> [<regions.csv>] [<hourly_sequence_id>]" && exit 1
DBNAME="$1"
EXTRACT="$2"
TAGS="$3"
REGIONS="${4-}"
INIT_SEQ="${5-}"

cd "$(dirname "$0")"
if [ ! -d venv ]; then
    python3.9 -m venv venv
    venv/bin/pip install requirements.txt
fi

PYTHON=venv/bin/python
$PYTHON osc_to_adiff.py init -d "$DBNAME" -t "$TAGS" ${REGIONS+-r "$REGIONS"} "$EXTRACT"

PSQL=( psql "$DBNAME" -v ON_ERROR_STOP=1 )
if [ -z "$INIT_SEQ" ]; then
  REPLICATION='https://planet.openstreetmap.org/replication/hour'
  SEQ="$(curl -s "$REPLICATION/state.txt" | grep sequenceNumber | cut -d = -f 2)"
else
  SEQ="$INIT_SEQ"
fi
${PSQL[@]} -c "create table if not exists osc_tracker_ts (ts integer);"
${PSQL[@]} -c "delete from osc_tracker_ts;"
${PSQL[@]} -c "insert into osc_tracker_ts (ts) values ($SEQ);"
${PSQL[@]} -c "delete from osc_tracker;" || true
