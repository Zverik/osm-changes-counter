#!/bin/bash
set -euo pipefail
[ $# -lt 2 ] && echo "Usage: $0 psql_database_name tags.lst [<regions.csv>]" && exit 1
cd "$(dirname "$0")"
PSQL=( psql "$1" -v ON_ERROR_STOP=1 )
OVERPASS='http://overpass-api.de/api'
PYTHON=venv/bin/python
NEXT_TS="$(${PSQL[@]} -qAtc 'select ts + 1 from adiff_tracker_ts order by ts desc limit 1')"
TS="$($PYTHON lib/gen_adiff_timestamps.py)"
for ts in $(seq $NEXT_TS $TS); do
    download_ok=
    while [ -z "$download_ok" ]; do
        echo "$(date +%H:%M:%S): $ts ($($PYTHON lib/gen_adiff_timestamps.py -$ts))"
        curl -s "$OVERPASS/augmented_diff?id=$ts" > $ts.adiff
        grep -q '<action type=' $ts.adiff && download_ok=yes
        [ -z "$download_ok" ] && sleep 20
    done
    $PYTHON lib/adiff_to_csv.py -t "$2" -p adiff_tracker ${3+-r "$3"} $ts.adiff | ${PSQL[@]}
    rm $ts.adiff
    ${PSQL[@]} -qAtc "insert into adiff_tracker_ts (ts) values ($ts);"
    sleep 10
done
${PSQL[@]} -qAtc "delete from adiff_tracker_ts where ts < (select max(ts) from adiff_tracker_ts);"
