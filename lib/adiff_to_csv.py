#!/usr/bin/env python3
import argparse
import sys
import csv
from filters import TagFilter, RegionFilter
from lxml import etree
from pyproj import Geod
from shapely.geometry import LineString


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


def compare_kinds(tag_filter, obj, old=None):
    result = []
    tobj = {kv.get('k'): kv.get('v') for kv in obj.findall('tag')}
    told = {} if old is None else {kv.get('k'): kv.get('v') for kv in old.findall('tag')}
    new_kinds = tag_filter.get_kinds(obj.tag, tobj, told)
    old_kinds = tag_filter.get_kinds(obj.tag, told, tobj)
    modified = tag_filter.get_modified_kinds(obj.tag, told, tobj, False)
    result.extend([(k, 'create') for k in new_kinds - old_kinds])
    result.extend([(k, 'delete') for k in old_kinds - new_kinds])
    result.extend([(k, 'modify') for k in modified])
    return result


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
    candidate = None
    version = None
    for action in adiff.findall('action'):
        if action.get('type') != 'modify':
            continue
        old_way = action.find('old' if is_created else 'new')[0]
        if (old_way.tag == 'way' and old_way.get('id') != way.get('id') and
                is_way_inside(way, old_way)):
            if version is None or version < old_way.get('version'):
                # For split, we compare "old" base way with the created way.
                # For join, we compare deleted way with the "old" base way.
                #   So that we can negate adding tags on base way
                #   that were present on deleted way.
                candidate = action.find('old')[0]
    return candidate


def write_header(output, table=None):
    col_names = ','.join(c[0] for c in COLUMNS)
    if not table:
        output.write(col_names + '\n')
    else:
        output.write("SET client_min_messages = 'ERROR';\n")
        output.write(f"create table if not exists {table} (\n")
        for c in COLUMNS:
            comma = '' if c == COLUMNS[-1] else ','
            output.write(f"    {c[0]} {c[1]}{comma}\n")
        output.write(");\n")
        # Copying into a temporary table
        output.write(f"drop table if exists tmp_{table};\n")
        output.write(f"create table tmp_{table} (like {table} including defaults);\n")
        output.write(f"copy tmp_{table} ({col_names}) from stdin (format csv);\n")


def write_footer(output, table=None):
    if table:
        output.write("\\.\n\n")
        output.write(f"insert into {table} select * from tmp_{table} "
                     "on conflict do nothing;\n")
        output.write(f"drop table tmp_{table};\n")
        output.write(f"create unique index if not exists idx_{table} on {table} "
                     "(osm_id, version, kind);\n")


def process_single_action(action, adiff, regions=None, tag_filter=None):
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
    if not data:
        return
    if regions and not regions.is_empty:
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
                atype = 'split'
                data['prev_id'] = get_osm_id(ancestor)
                old = ancestor  # just for comparing tags
        elif atype == 'delete':
            ancestor = find_way_in_another_modified(old, adiff, False)
            if ancestor is not None:
                # This way (and possibly others) were merged into the ancestor.
                atype = 'join'
                data['prev_id'] = get_osm_id(ancestor)
                obj = ancestor  # just for comparing tags
    data['obj_action'] = atype
    # Find tagging differences and write them out.
    kinds = compare_kinds(tag_filter, obj, old)
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
    parser.add_argument('-t', '--tags', type=argparse.FileType('r'),
                        help='File with a list of tags to watch')
    parser.add_argument('-r', '--regions', type=argparse.FileType('r'),
                        help='CSV file with names and wkb geometry for regions to filter')
    parser.add_argument('-p', '--table',
                        help='Instead of CSV, print SQL for importing into this psql table')
    options = parser.parse_args()

    # Read regions and the augmented diff.
    tags = TagFilter(options.tags)
    regions = RegionFilter(options.regions)
    adiff = etree.parse(options.adiff).getroot()

    # Prepare writer and write the header.
    writer = csv.DictWriter(options.output, [c[0] for c in COLUMNS])
    wrote_header = False

    # Iterate over every action (each of which has just one object).
    for action in adiff.findall('action'):
        for row in process_single_action(action, adiff, regions, tags):
            if not wrote_header:
                write_header(options.output, options.table)
                wrote_header = True
            writer.writerow(row)
    if wrote_header:
        write_footer(options.output, options.table)
