#!/bin/bash
set -euo pipefail
[ -z "$1" ] && echo "Usage: $0 psql_database_name [polygon_table_name]" && exit 1
POLYGONS="${2-planet_osm_polygon}"
PSQL=( psql "$1" -v ON_ERROR_STOP=1 )
${PSQL[@]} -c "copy (select name, ST_Transform(way, 4326) from $2 where place = 'city') to stdout (format csv)" > cities.csv
