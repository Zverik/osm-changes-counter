#!/usr/bin/env python3
import argparse
import csv
import sys
import os
import json


class Weights:
    def __init__(self, fileobj):
        self.defaults()
        if fileobj:
            self.load(fileobj)

    def load(self, fileobj):
        for row in fileobj:
            parts = [r.strip() for r in row.split(':', 1)]
            if len(parts) != 2:
                continue
            k = parts[0]
            if '.' in k:
                kk = k.split('.')
                k = kk[0]
                t = kk[1]
            else:
                t = None
            v = parts[1]
            if k == 'modify':
                self.modify = float(v)
            elif k == 'type' and t:
                self.types[t] = float(v)
            elif k == 'usergroup' and t != 'label':
                self.usergroups[t] = v
            elif t == 'label':
                self.labels[k] = v
            else:
                if k not in self.weights:
                    self.weights[k] = [1]
                if t is None:
                    self.weights[k][0] = float(v)
                elif t == 'modify':
                    if len(self.weights[k]) == 1:
                        self.weights[k].append(float(v))
                    else:
                        self.weights[k][1] = float(v)

    def defaults(self):
        self.modify = 0.5
        self.types = {
            'node': 1000,
            'way': 1,
            'relation': 2000,
        }
        self.weights = {}
        self.labels = {}
        self.usergroups = {}

    def calculate(self, row):
        value = float(row['value']) or 1
        return value * self.get(row['osm_id'], row['kind'], row['action'] == 'modify')

    def get(self, osm_id, kind, is_modify):
        typ = row['osm_id'].split('/')[0]
        base = self.types[typ]
        weight = self.weights.get(row['kind'], [1])
        if not is_modify:
            return base * weight[0]
        elif len(weight) > 1:
            return base * weight[1]
        return base * weight[0] * self.modify


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
    # Round all numbers
    for v in table.values():
        for k in v:
            v[k] = round(v[k])
    return table


def prepare_json(result):
    table = {}  # (uid, region) -> dict of kinds
    for k, v in result.items():
        ur = (k[0], k[1])
        kind = k[2]
        if ur not in table:
            table[ur] = {
                'uid': k[0],
                'region': k[1],
                'score': 0
            }
        if kind not in table[ur]:
            table[ur][kind] = 0
        table[ur][kind] += v[0]
        table[ur]['score'] += v[1]
    # Round all numbers
    for v in table.values():
        for k in v:
            if k not in ('uid', 'region'):
                v[k] = round(v[k])
    return list(table.values())


def drop_user(users, usernames, uid):
    return users and uid not in users and usernames[uid] not in users


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
    usergroups = {}
    if options.users:
        for row in csv.reader(options.users):
            users.add(row[2] or row[1])
            if len(row) > 3 and row[3].strip():
                usergroups[row[2] or row[1]] = row[3].strip()

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
    min_ts = max_ts = None  # plain string
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
        if not min_ts or row['ts'] < min_ts:
            min_ts = row['ts']
        if not max_ts or row['ts'] > max_ts:
            max_ts = row['ts']
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
    columns = ['user'] + sorted(columns[0]) + sorted(columns[1])
    if usergroups:
        columns.append('usergroup')
    columns.append('score')
    if options.csv:
        data = count_by_user(result)
        for uid in list(data.keys()):
            if drop_user(users, usernames, uid):
                del data[uid]
                continue
            data[uid]['user'] = usernames[uid]
            if usergroups:
                group = usergroups.get(uid) or usergroups.get(usernames[uid])
                data[uid]['usergroup'] = weights.usergroups.get(group, group)

        w = csv.DictWriter(options.output, columns)
        w.writerow({c: weights.labels.get(c, c) for c in columns})
        for row in sorted(data.values(), key=lambda r: r['score'], reverse=True):
            w.writerow(row)
    else:
        json_data = prepare_json(result)
        json_data = [r for r in json_data if not drop_user(users, usernames, r['uid'])]
        for row in json_data:
            row['user'] = usernames[row['uid']]
            if usergroups:
                group = usergroups.get(row['uid']) or usergroups.get(row['user'])
                row['usergroup'] = weights.usergroups.get(group, group)
        template = open(os.path.join(
            os.path.dirname(__file__), 'user_stats_template.html'
        ), 'r').read()
        template = template.replace('{{min_ts}}', min_ts)
        template = template.replace('{{max_ts}}', max_ts)
        template = template.replace('{{usergroups}}', json.dumps(
            weights.usergroups, ensure_ascii=False))
        template = template.replace('{{columns}}', json.dumps(columns, ensure_ascii=False))
        template = template.replace('{{tr_columns}}', json.dumps(
            [weights.labels.get(c, c) for c in columns], ensure_ascii=False))
        template = template.replace('{{data}}', json.dumps(json_data, ensure_ascii=False))
        options.output.write(template)
