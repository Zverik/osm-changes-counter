#!/usr/bin/env python3
import argparse
import sys
import csv
from lxml import etree
from pyproj import Geod
from shapely import wkb
from shapely.geometry import Point, LineString
from shapely.strtree import STRtree


COLUMNS = [
    # UTC timestamp for the change
    ('ts', 'timestamp with time zone not null'),
    # One of create, delete, modify,
    # split (created from splitting), join (deleted for joining)
    ('action', 'text not null'),
    # For a tag: create, delete, modify (for a value)
    ('obj_action', 'text not null'),
    # Tag kind, e.g. crossing, maxspeed
    ('kind', 'text not null'),
    # System data from an object
    ('changeset', 'integer not null'),
    ('uid', 'integer not null'),
    ('username', 'text not null'),
    ('osm_id', 'text not null'),
    ('version', 'integer not null'),
    # For splitting and joining, osm_id of an ancestor way
    ('prev_id', 'text'),
    # When filtering by regions, a region name
    ('region', 'text'),
    # Location of a node or a centroid
    ('lat', 'double precision not null'),
    ('lon', 'double precision not null'),
    # For ways, length in meters
    ('length', 'integer'),
]


class Regions:
    def __init__(self, fileobj=None):
        self.tree = None
        self.region_map = {}
        if fileobj:
            self.load(fileobj)

    def is_empty(self):
        return self.tree is None or len(self.region_map) == 0

    def load(self, fileobj):
        regions = []
        csv.field_size_limit(1000000)
        for row in csv.reader(fileobj):
            regions.append((row[0], wkb.loads(bytes.fromhex(row[1]))))
        self.tree = STRtree([r[1] for r in regions])
        self.region_map = {id(r[1]): r[0] for r in regions}

    def find(self, lon, lat):
        if not self.tree:
            return None
        pt = Point(lon, lat)
        results = self.tree.query(pt)
        results = [r for r in results if r.contains(pt)]
        return None if not results else self.region_map[id(results[0])]


def get_float_attr(attr, obj, backup=None):
    if attr in obj.attrib:
        return float(obj.get(attr))
    return float(backup.get(attr))


def get_osm_id(obj):
    return f'{obj.tag}/{obj.get("id")}'


def init_data_from_object(obj, backup=None):
    result = {
        'ts': obj.get('timestamp').replace('T', ' ').replace('Z', '+00'),
        'changeset': obj.get('changeset'),
        'uid': obj.get('uid'),
        'username': obj.get('user'),
        'osm_id': get_osm_id(obj),
        'version': obj.get('version'),
    }
    if obj.tag == 'node':
        result.update({
            'lon': get_float_attr('lon', obj, backup),
            'lat': get_float_attr('lat', obj, backup),
        })
    else:
        bounds = obj.find('bounds')
        if bounds is None:
            bounds = backup.find('bounds')
        if bounds is None:
            sys.stderr.write(f'Missing bounds for {result}\n')
        result.update({
            'lon': (float(bounds.get('minlon')) + float(bounds.get('maxlon'))) / 2,
            'lat': (float(bounds.get('minlat')) + float(bounds.get('maxlat'))) / 2,
        })
    if obj.tag == 'way':
        # Calculate length
        nodes = obj.findall('nd')
        if len(nodes) == 0:
            nodes = backup.findall('nd')
        if len(nodes) < 2 or not all([nd.get('lat') for nd in nodes]):
            return None
        line = LineString([(float(nd.get('lon')), float(nd.get('lat'))) for nd in nodes])
        geod = Geod(ellps='WGS84')
        result['length'] = round(geod.geometry_length(line))
    elif obj.tag == 'relation' and len(obj.find('member')) == 0:
        return None
    return result


def get_tag_value(tag, obj=None):
    if obj is None:
        return None
    tags = [t for t in obj.findall('tag') if t.get('k') == tag]
    return None if not tags else tags[0].get('v')


def get_tag_action(tag, obj, old=None) -> str:
    new_value = get_tag_value(tag, obj)
    old_value = get_tag_value(tag, old)
    if new_value == old_value:
        return None
    if new_value:
        return 'create' if not old_value else 'modify'
    else:
        return 'delete'


def is_new_tag_action(k, v, obj, old=None) -> str:
    has_new = get_tag_value(k, obj) == v
    has_old = get_tag_value(k, old) == v
    if has_new == has_old:
        return None
    return 'create' if not has_old else 'delete'


def reduce_tag_actions(*args) -> str:
    actions = set([a for a in args if a])
    if not actions:
        return None
    if len(actions) == 1:
        return list(actions)[0]
    return 'modify'


def get_kinds(obj, old=None):
    result = []  # list of (kind, action)
    if obj.tag == 'node':
        result.append(('traffic_calming', get_tag_action('traffic_calming', obj, old)))
        ta = is_new_tag_action('highway', 'crossing', obj, old)
        if ta:
            result.append(('crossing', ta))
        else:
            result.append(('crossing_island', get_tag_action('crossing:island', obj, old)))
        result.append(('stop', is_new_tag_action('highway', 'bus_stop', obj, old)))
    elif obj.tag == 'way':
        result.append(('maxspeed', reduce_tag_actions(
            get_tag_action('maxspeed', obj, old),
            get_tag_action('maxspeed:backward', obj, old),
            get_tag_action('maxspeed:forward', obj, old),
        )))
        result.append(('lanes', reduce_tag_actions(
            get_tag_action('lanes', obj, old),
            get_tag_action('lanes:backward', obj, old),
            get_tag_action('lanes:forward', obj, old),
        )))
        result.append(('lit', get_tag_action('lit', obj, old)))
    return [r for r in result if r[1]]


def is_way_inside(way, another):
    """Returns True if way's nodes are inside another's nodes."""
    nodes = [n.get('ref') for n in way.findall('nd')]
    anodes = [n.get('ref') for n in another.findall('nd')]
    # We look for at least len(nodes) / 2 + 1 matches.
    cnt_matches = len([n for n in nodes if n in anodes])
    return nodes[0] in anodes and nodes[-1] in anodes and cnt_matches > len(nodes) / 2


def find_way_in_another_modified(way, adiff, is_created: bool):
    """
    So we have a created or deleted way. It may be a result of
    splitting or merging other way(s). So for created way, we look
    for its nodes inside an old version of another modified way.
    For deleted way, we look for its nodes inside a new version
    of another modified way. And we return that way back.
    """
    if way.tag != 'way':
        return None
    for action in adiff.findall('action'):
        if action.get('type') != 'modify':
            continue
        old_way = action.find('old' if is_created else 'new')[0]
        if old_way.tag == 'way' and is_way_inside(way, old_way):
            return old_way
    return None


def write_header(output, table=None):
    col_names = ','.join(c[0] for c in COLUMNS)
    if not table:
        output.write(col_names + '\n')
    else:
        output.write(f"create table if not exists {table} (\n")
        for c in COLUMNS:
            comma = '' if c == COLUMNS[-1] else ','
            output.write(f"    {c[0]} {c[1]}{comma}\n")
        output.write(");\n")
        output.write(f"copy {table} ({col_names}) from stdin (format csv);\n")


def process_single_action(action, adiff, regions=None):
    """
    Processes a single action in an augmented diff.
    Returns a list of rows to print.
    """
    atype = action.get('type')
    obj = action[0] if atype == 'create' else action.find('new')[0]
    if obj.tag == 'relation':
        # We do not process relations.
        return
    old = None if atype == 'create' else action.find('old')[0]
    data = init_data_from_object(obj, old)
    if regions and not regions.is_empty():
        data['region'] = regions.find(data['lon'], data['lat'])
        if not data['region']:
            return
    # Note that for deleted objects "obj" has all its data,
    # and "old" has just some of the header values.
    ancestor = None
    if obj.tag == 'way':
        if atype == 'create':
            ancestor = find_way_in_another_modified(obj, adiff, True)
            if ancestor is not None:
                # Some way was split into this (and possibly others).
                sys.stderr.write(f'Found an ancestor {ancestor.get("id")} for '
                                 f'the created way {obj.get("id")}!\n')
                atype = 'split'
                data['prev_id'] = get_osm_id(ancestor)
                old = ancestor  # just for comparing tags
        elif atype == 'delete':
            ancestor = find_way_in_another_modified(old, adiff, False)
            if ancestor is not None:
                # This way (and possibly others) were merged into the ancestor.
                sys.stderr.write(f'Found an ancestor {ancestor.get("id")} for '
                                 f'the deleted way {old.get("id")}!\n')
                atype = 'join'
                data['prev_id'] = get_osm_id(ancestor)
                obj = ancestor  # just for comparing tags
    data['obj_action'] = atype
    # Find tagging differences and write them out.
    kinds = get_kinds(obj, old)
    for k in kinds:
        data['action'] = k[1]
        data['kind'] = k[0]
        yield data


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Extracts road changes from an augmented diff file.')
    parser.add_argument('adiff', type=argparse.FileType('rb'),
                        help='Augmented diff file')
    parser.add_argument('-o', '--output', type=argparse.FileType('w'), default=sys.stdout,
                        help='Output CSV or SQL file')
    parser.add_argument('-r', '--regions', type=argparse.FileType('r'),
                        help='CSV file with names and wkb geometry for regions to filter')
    parser.add_argument('-t', '--table',
                        help='Instead of CSV, print SQL for importing into this psql table')
    options = parser.parse_args()

    # Read regions and the augmented diff.
    regions = Regions(options.regions)
    adiff = etree.parse(options.adiff).getroot()

    # Prepare writer and write the header.
    writer = csv.DictWriter(options.output, [c[0] for c in COLUMNS])
    wrote_header = False

    # Iterate over every action (each of which has just one object).
    for action in adiff.findall('action'):
        for row in process_single_action(action, adiff, regions):
            if not wrote_header:
                write_header(options.output, options.table)
                wrote_header = True
            writer.writerow(row)
