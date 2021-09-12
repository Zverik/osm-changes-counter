#!/bin/bash
set -euo pipefail
[ -z "${1-}" ] && echo "Usage: $0 psql_database_name [<regions.csv>]" && exit 1
PSQL=( psql "$1" -v ON_ERROR_STOP=1 )
NEXT_TS="$(${PSQL[@]} -qAtc 'select ts + 1 from adiff_tracker_ts order by ts desc limit 1')"
TS="$(venv/bin/python gen_adiff_timestamps.py)"
for ts in $(seq $NEXT_TS $TS); do
    echo "$ts ($(date +%H:%M:%S))"
    curl -s "http://overpass-api.de/api/augmented_diff?id=$ts" > $ts.adiff
    venv/bin/python adiff_to_csv.py -t adiff_tracker ${2+-r "$2"} $ts.adiff | ${PSQL[@]}
    rm $ts.adiff
    ${PSQL[@]} -qAtc "insert into adiff_tracker_ts (ts) values ($ts);"
done
${PSQL[@]} -qAtc "delete from adiff_tracker_ts where ts < (select max(ts) from adiff_tracker_ts);"
