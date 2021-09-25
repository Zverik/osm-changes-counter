#!/bin/bash
[ $# -lt 3 ] && echo "Usage: $0 {dump|restore} <dbname> <file.sql.gz>" && exit 1
if [ "$1" == "dump" ]; then
  pg_dump "$2" -t 'osc*' --no-acl --no-owner --clean --if-exists | gzip > "$3"
elif [ "$1" == "restore" ]; then
  gzip -dc "$3" | psql "$2"
else
  echo "Wrong command: $1"
  exit 3
fi
