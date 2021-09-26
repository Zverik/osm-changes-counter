#!/usr/bin/env python3
import argparse
import csv
import sys


class Weights:
    def __init__(self, fileobj):
        self.defaults()
        if fileobj:
            self.load(fileobj)

    def load(self, fileobj):
        pass

    def defaults(self):
        self.modify = 0.5
        self.types = {
            'node': 1000,
            'way': 1,
            'relation': 2000,
        }
        self.weights = {}

    def calculate(self, row):
        value = float(row['value']) or 1
        return value * self.get(row['osm_id'], row['kind'], row['action'] == 'modify')

    def get(self, osm_id, kind, is_modify):
        typ = row['osm_id'].split('/')[0]
        weight = self.weights.get(row['kind'], (self.types[typ],))
        if not is_modify:
            return weight[0]
        return weight[0] * (weight[1] if len(weight) > 1 else self.modify)


def update_result(result, weights, current, osm_id, kind, region):
    for uid, contrib in current.items():
        k = (uid, region, kind)
        if k not in result:
            result[k] = [0, 0]
        value = contrib[0] or contrib[1]
        mult = weights.get(osm_id, kind, not contrib[0])
        result[k][0] += value
        result[k][1] += value * mult


def count_by_user(result, region=None):
    table = {}
    for k, v in result.items():
        if region and k[1] != region:
            continue
        uid = k[0]
        kind = k[2]
        if uid not in table:
            table[uid] = {'score': 0}
        if kind not in table[uid]:
            table[uid][kind] = 0
        table[uid][kind] += v[0]
        table[uid]['score'] += v[1]
    return table


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Reads a PostgreSQL adiff table and calculates user statistics.')
    parser.add_argument('-i', '--input', type=argparse.FileType('r'), default=sys.stdin,
                        help='CSV file from psql, with header')
    parser.add_argument('-o', '--output', type=argparse.FileType('w'), default=sys.stdout,
                        help='Where to write the resulting table')
    parser.add_argument('-u', '--users', type=argparse.FileType('r'),
                        help='CSV file with users and uids, made with form_to_uid.py')
    parser.add_argument('-w', '--weights', type=argparse.FileType('r'),
                        help='Definitions for weights for change types')
    parser.add_argument('--csv', action='store_true',
                        help='Write CSV instead of HTML')
    options = parser.parse_args()

    weights = Weights(options.weights)
    users = set()
    if options.users:
        for row in csv.reader(options.users):
            users.add(row[2] or row[1])

    reader = csv.DictReader(options.input)
    rows = [r for r in reader]
    # For joined (deleted) ways, swap osm_id and parent_id
    for r in rows:
        if r['obj_action'] == 'join':
            r['osm_id'], r['prev_id'] = r['prev_id'], r['osm_id']
    rows.sort(key=lambda r: (r['osm_id'], r['kind'], r['ts'], r['version']))

    # Gathering data
    usernames = {}  # uid -> username
    columns = [set(), set()]  # list of columns for nodes and ways
    current = {}  # uid -> (n_created, n_modified)
    last_added = None  # uid of the last action if that was "create",
    # negative if that was "delete"
    osm_id_kind = None  # (osm_id, kind, region)
    result = {}  # (uid, region, kind) -> (count, score)
    for row in rows:
        oik = (row['osm_id'], row['kind'], row['region'])
        if osm_id_kind != oik:
            if osm_id_kind:
                update_result(result, weights, current, osm_id_kind[0],
                              osm_id_kind[1], osm_id_kind[2])
            current = {}
            last_added = None
            osm_id_kind = oik
        uid = row['uid']
        usernames[uid] = row['username']
        columns[1 if row['length'] else 0].add(row['kind'])
        if uid not in current:
            current[uid] = [0, 0]

        value = float(row.get('length') or 1)
        if row['action'] == 'create':
            if last_added and last_added < 0:
                # When restoring after deletion, count as modification
                current[uid][1] = value
            else:
                current[uid][0] = value
            last_added = int(uid)
        elif row['action'] == 'delete':
            if last_added and last_added > 0:
                # Undo last creation if there was one.
                current[str(last_added)][0] = max(0, current[str(last_added)][0] - value)
                current[str(last_added)][1] = 0
            last_added = -int(uid)
        elif row['action'] == 'modify':
            current[uid][1] = value
            # Not emptying last_added, since we allow intermediate
            # modifications. But a deletion undoes creation.
        else:
            raise KeyError(f'Wrong action {row["action"]} for {osm_id_kind}')
    update_result(result, weights, current, osm_id_kind[0],
                  osm_id_kind[1], osm_id_kind[2])

    # Writing the result
    if options.csv:
        data = count_by_user(result)
        for uid in data:
            if users:
                if uid not in users and usernames[uid] not in users:
                    continue
            data[uid]['user'] = usernames[uid]

        columns = ['user'] + sorted(columns[0]) + sorted(columns[1]) + ['score']
        w = csv.DictWriter(options.output, columns)
        w.writeheader()
        for row in sorted(data.values(), key=lambda r: r['score'], reverse=True):
            w.writerow(row)
    else:
        raise Exception('HTML printing is not implemented yet.')
