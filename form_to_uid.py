#!/usr/bin/env python3
import sys
import os
import csv
import requests
import urllib.parse as up
from lxml import etree


def find_uid(username):
    resp = requests.get(
        'https://api.openstreetmap.org/api/0.6/changesets',
        {'display_name': username}
    )
    if resp.status_code != 200:
        sys.stderr.write(f'User display name is wrong: {username}\n')
        return None
    root = etree.fromstring(resp.content)
    changeset = root.find('changeset')
    if changeset is None:
        # sys.stderr.write(f'User found with 0 changesets: {username}\n')
        return None
    return changeset.get('uid')


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Reads the google docs user form and finds uid for each user.')
        print('Usage: {0} <gdocs_form.csv> <output.csv>')
        sys.exit(1)

    uids = {}
    if os.path.exists(sys.argv[2]):
        with open(sys.argv[2], 'r') as f:
            for row in csv.reader(f):
                if row[1].strip():
                    uids[row[0]] = row[1]

    with open(sys.argv[1], 'r') as f:
        for row in csv.reader(f):
            username = row[4].strip()
            if '2021' not in row[0] or not username:
                continue
            if '/' in username and 'openstreetmap.org' in username:
                username = up.unquote(username.split('/')[-1])
            if username in uids:
                continue
            uid = find_uid(username)
            uids[username] = uid
            sys.stderr.write('.')
            sys.stderr.flush()

    with open(sys.argv[2], 'w') as f:
        w = csv.writer(f)
        for name, uid in uids.items():
            w.writerow([name, uid])
