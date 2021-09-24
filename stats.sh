#!/bin/bash
set -euo pipefail
[ -z "${1-}" ] && echo "Usage: $0 psql_database_name [<output.html>]" && exit 1
cd "$(dirname "$0")"
PSQL=( psql "$1" -v ON_ERROR_STOP=1 )
PYTHON=venv/bin/python
${PSQL[@]} $1 -c "copy osc_tracker to stdout (format csv, header)" \
  | $PYTHON generate_user_stats.py 1> "${2-&1}"
