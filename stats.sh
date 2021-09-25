#!/bin/bash
set -euo pipefail
[ $# -lt 2 ] && echo "Usage: $0 psql_database_name <output.html>" && exit 1
cd "$(dirname "$0")"
PSQL=( psql "$1" -v ON_ERROR_STOP=1 )
PYTHON=venv/bin/python
CSV_ARG=
[ "${2##*.}" == "csv" ] && CSV_ARG="--csv"
${PSQL[@]} -c "copy (select * from osc_tracker order by ts, osm_id, kind) to stdout (format csv, header)" \
  | $PYTHON generate_user_stats.py $CSV_ARG -o "$2"
