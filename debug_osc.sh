#!/bin/bash
set -euo pipefail
[ $# -lt 2 ] && echo "Usage: $0 psql_database_name tags.lst [<regions.csv>]" && exit 1
DBNAME="$1"
TAGS="$2"
REGIONS="${3-}"

cd "$(dirname "$0")"
PSQL=( psql "$DBNAME" -v ON_ERROR_STOP=1 )
PYTHON=venv/bin/python
NEXT_SEQ="$(${PSQL[@]} -qAtc 'select ts + 1 from osc_tracker_ts order by ts desc limit 1')"
REPLICATION='https://planet.openstreetmap.org/replication/hour'
SEQ="$(curl -s "$REPLICATION/state.txt" | grep sequenceNumber | cut -d = -f 2)"

echo "Updating to sequence number $SEQ"
for ts in $(seq $NEXT_SEQ $SEQ); do
    URL="$REPLICATION/000/$(printf %03d $(($ts/1000)))/$(printf %03d $(($ts%1000))).osc.gz"
    echo "$(date +%H:%M:%S): $URL"
    curl -s --fail "$URL" > $ts.osc.gz
    $PYTHON osc_to_adiff.py process -d "$DBNAME" -t "$TAGS" ${REGIONS+-r "$REGIONS"} $ts.osc.gz -a $ts.adiff -vv 2> $ts.log
    $PYTHON adiff_to_csv.py -t "$TAGS" -p osc_tracker ${REGIONS+-r "$REGIONS"} $ts.adiff > $ts.sql
    ${PSQL[@]} -f $ts.sql
    ${PSQL[@]} -qAtc "insert into osc_tracker_ts (ts) values ($ts);"
    sleep 5
done
${PSQL[@]} -qAtc "delete from osc_tracker_ts where ts < (select max(ts) from osc_tracker_ts);"
