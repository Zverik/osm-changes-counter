#!/bin/bash
set -euo pipefail
[ $# -lt 2 ] && echo "Usage: $0 {psql_database_name|csv_file} <output.html> [<weights.lst> [<uids.csv>]]" && exit 1
cd "$(dirname "$0")"
PYTHON=venv/bin/python
CSV_ARG=
[ "${2##*.}" == "csv" ] && CSV_ARG="--csv"
if [ -e "$1" ]; then
   $PYTHON generate_user_stats.py -i "$1" $CSV_ARG ${3+-w "$3"} ${4+-u "$4"} -o "$2"
else
  PSQL=( psql "$1" -v ON_ERROR_STOP=1 )
  ${PSQL[@]} -c "copy (select * from osc_tracker order by ts, osm_id, kind) to stdout (format csv, header)" \
    | $PYTHON generate_user_stats.py $CSV_ARG ${3+-w "$3"} ${4+-u "$4"} -o "$2"
fi
