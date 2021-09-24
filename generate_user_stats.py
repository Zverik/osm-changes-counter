#!/usr/bin/env python3
import argparse
import csv
import sys


def update_byuser(byuser, current):
    for uid, edits in current.items():
        if uid not in byuser:
            byuser[uid] = {}
        for k, v in edits.items():
            byuser[uid][k] = v if k not in byuser[uid] else byuser[uid][k] + v


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Reads a PostgreSQL adiff table and calculates user statistics.')
    parser.add_argument('data', type=argparse.FileType('r'), default=sys.stdin,
                        help='CSV file from psql, with header')
    parser.add_argument('-o', '--output', type=argparse.FileType('w'), default=sys.stdout,
                        help='Where to write the resulting table')
    parser.add_argument('-u', '--users', type=argparse.FileType('r'),
                        help='CSV file with users and uids, made with form_to_uid.py')
    parser.add_argument('--csv', action='store_true',
                        help='Write CSV instead of HTML')
    options = parser.parse_args()

    users = set()
    if options.users:
        for row in csv.reader(options.users):
            users.add(int(row[2]) if row[2] else row[1])
    reader = csv.DictReader(options.data)
    rows = [r for r in reader]
    rows.sort(key=lambda r: (r['osm_id'], r['ts']))

    # Gathering data
    current_osm_id = None
    byuser = {}
    current = {}
    usernames = {}
    columns = [set(), set()]
    last_rows = []
    for row in rows:
        if current_osm_id != row['osm_id']:
            update_byuser(byuser, current)
            current = {}
            current_osm_id = row['osm_id']
            last_rows = []
        uid = int(row['uid'])
        if uid not in current:
            current[uid] = {}
        kind = row['kind']
        region = row['region']
        k = (kind, region)

        usernames[uid] = row['username']
        columns[1 if row['length'] else 0].add(kind)
        value = int(row['length']) if row['length'] else 1
        if row['obj_action'] == 'delete':
            for r in last_rows:
                current[r[0]][r[1]] -= value
            last_rows = []
        else:
            current[uid][k] = value if k not in current[uid] else current[uid][k] + value
            last_rows.append((uid, k, value))
    update_byuser(byuser, current)

    # Writing the result
    if options.csv:
        columns = ['user'] + sorted(columns[0]) + sorted(columns[1])
        w = csv.DictWriter(options.output, columns)
        w.writeheader()
        for uid, edits in byuser.items():
            if users:
                if uid not in users and usernames[uid] not in users:
                    continue
            row = {'user': usernames[uid]}
            for k, v in edits.items():
                if k[0] not in row:
                    row[k[0]] = 0
                row[k[0]] += v
            w.writerow(row)
    else:
        raise Exception('HTML printing is not implemented yet.')
