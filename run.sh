#!/bin/bash
set -euo pipefail
[ -z "${1-}" ] && echo "Usage: $0 psql_database_name [<regions.csv>]" && exit 1
cd "$(dirname "$0")"
PSQL=( psql "$1" -v ON_ERROR_STOP=1 )
PYTHON=venv/bin/python
NEXT_TS="$(${PSQL[@]} -qAtc 'select ts + 1 from adiff_tracker_ts order by ts desc limit 1')"
TS="$($PYTHON gen_adiff_timestamps.py)"
for ts in $(seq $NEXT_TS $TS); do
    local download_ok=
    while [ -z "$download_ok" ]; do
        echo "$(date +%H:%M:%S): $ts ($($PYTHON gen_adiff_timestamps.py -$ts))"
        curl -s "http://overpass-api.de/api/augmented_diff?id=$ts" > $ts.adiff
        grep -q '<action type=' $ts.adiff && download_ok=yes
        [ -z "$download_ok" ] && sleep 20
    done
    $PYTHON adiff_to_csv.py -t adiff_tracker ${2+-r "$2"} $ts.adiff | ${PSQL[@]}
    rm $ts.adiff
    ${PSQL[@]} -qAtc "insert into adiff_tracker_ts (ts) values ($ts);"
done
${PSQL[@]} -qAtc "delete from adiff_tracker_ts where ts < (select max(ts) from adiff_tracker_ts);"
