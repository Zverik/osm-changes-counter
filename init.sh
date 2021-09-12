#!/bin/bash
[ -z "$1" ] && echo "Usage: $0 psql_database_name [YYYY-MM-DDtHH:MM]" && exit 1
if [ ! -d venv ]; then
    python3 -m venv venv
    venv/bin/pip install requirements.txt
fi

PSQL=( psql "$1" -v ON_ERROR_STOP=1 )
TS="$(venv/bin/python gen_adiff_timestamps.py ${2-})"
${PSQL[@]} -c "create table if not exists adiff_tracker_ts (ts integer);"
${PSQL[@]} -c "delete from adiff_tracker_ts;"
${PSQL[@]} -c "insert into adiff_tracker_ts (ts) values ($TS);"
