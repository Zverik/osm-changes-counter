import argparse
import psycopg2
import osmium
import requests
import logging
import gzip
import itertools
from lxml import etree
from osc_db import OscDatabase, StoredObject
from filters import TagFilter, RegionFilter


OSM_API = 'https://api.openstreetmap.org/api/0.6'


class InitHandler(osmium.SimpleHandler):
    def __init__(self, db, tag_filter, region_filter):
        super().__init__()
        self.db = db
        self.tag_filter = tag_filter
        self.region_filter = region_filter

    def tags_to_dict(self, obj):
        return {tag.k: tag.v for tag in obj.tags}

    def node(self, n):
        tags = self.tags_to_dict(n)
        if not self.tag_filter.is_empty and not self.tag_filter.get_kinds('node', tags):
            return
        if (not self.region_filter.is_empty and
                not self.region_filter.find(n.location.lon, n.location.lat)):
            return
        self.db.save_object(StoredObject('node', n.id, n.version, tags))
        # Save its location
        self.db.update_locations([(n.id, n.location.lat, n.location.lon)])

    def way(self, w):
        if len(w.nodes) < 2:
            return
        tags = self.tags_to_dict(w)
        if not self.tag_filter.is_empty and not self.tag_filter.get_kinds('way', tags):
            return
        self.db.save_object(StoredObject(
            'way', w.id, w.version, tags, [n.ref for n in w.nodes]
        ))
        # Also store node locations
        self.db.update_locations([(n.ref, n.location.lat, n.location.lon) for n in w.nodes])


class Bounds:
    def __init__(self):
        self.minlon = 1000
        self.maxlon = -1000
        self.minlat = 1000
        self.maxlat = -1000

    def extend(self, lat, lon):
        if lat < self.minlat:
            self.minlat = lat
        if lat > self.maxlat:
            self.maxlat = lat
        if lon < self.minlon:
            self.minlon = lon
        if lon > self.maxlon:
            self.maxlon = lon

    @property
    def is_empty(self):
        return self.minlon > 360

    def to_xml(self):
        if self.is_empty:
            raise ValueError('Non-initialized Bounds object!')
        return etree.Element(
            'bounds', minlat=str(self.minlat), minlon=str(self.minlon),
            maxlat=str(self.maxlat), maxlon=str(self.maxlon))


class AdiffBuilder:
    def __init__(self, db, tag_filter, region_filter):
        self.db = db
        self.tag_filter = tag_filter
        self.region_filter = region_filter

    def scan_node_locations(self, fileobj) -> dict:
        """Searches for nodes and returns dict of node_id -> (lat, lon)."""
        locs = {}
        for _, action in etree.iterparse(fileobj, events=['end'],
                                         tag=['create', 'modify', 'delete']):
            for node in action.findall('node'):
                node_id = node.get('id')
                if node.get('lat'):
                    locs[node_id] = float(node.get('lat')), float(node.get('lon'))
            action.clear()
        fileobj.seek(0)
        return locs

    def scan_relevant_ways_nodes(self, fileobj, locations) -> set:
        """Looks for ways with no nodes in the database or in osc, and adds these to locations."""
        if self.region_filter.is_empty:
            return
        node_ids = set()
        for _, action in etree.iterparse(fileobj, events=['end'],
                                         tag=['create', 'modify', 'delete']):
            for way in action.findall('way'):
                if not self.wrong_tags(way):
                    point = self.get_representative_point(way, locations)
                    if not point:
                        ids = self.get_node_ids(way)
                        if ids and node_ids.isdisjoint(set(ids)):
                            node_ids.add(ids[0])
            action.clear()
        # Now download nodes from OSM API
        loc = self.download_node_locations(node_ids)
        locations.update(loc)
        fileobj.seek(0)

    def get_node_ids(self, obj):
        if obj.tag == 'way':
            return [nd.get('ref') for nd in obj.findall('nd')]
        if obj.tag == 'relation':
            return [nd.get('ref') for nd in obj.findall('member') if nd.get('type') == 'node']
        return None

    def get_representative_point(self, obj, locations, download=False) -> tuple:
        """Returns (lat, lon) for an xml object of way or node."""
        if obj.tag == 'node':
            if obj.get('lat'):
                return float(obj.get('lat')), float(obj.get('lon'))
            # Deleted node, look up coordinates in the database
            node_ids = [obj.get('id')]
        else:
            node_ids = set(self.get_node_ids(obj))
            if not node_ids:
                # Deleted way/relation, get nodes from the database
                old = self.db.read_object(obj.tag, obj.get('id'))
                if old and old.nodes:
                    node_ids = set(old.nodes)
        if not node_ids:
            return None

        # First look up nodes in the same osmChange
        loc = {k: locations[k] for k in node_ids if k in locations}
        if not loc:
            # Not found, e.g. just a tag change. Look up in the database
            loc = self.db.get_locations(node_ids)
        if not loc and download:
            loc = self.download_node_locations(list(node_ids)[:1])
        return None if not loc else loc[list(loc.keys())[0]]

    def get_locations_from_everywhere(self, node_ids, locations=None):
        id_set = set(node_ids)
        loc = {} if not locations else {k: locations[k] for k in id_set if k in locations}
        if len(loc) < len(id_set):
            loc.update(self.db.get_locations(id_set - loc.keys()))
        if len(loc) < len(id_set):
            loc.update(self.download_node_locations(id_set - loc.keys()))
        return loc

    def add_locations(self, obj, locations=None):
        if obj.tag == 'node':
            return
        bounds = Bounds()
        node_ids = self.get_node_ids(obj)
        loc = self.get_locations_from_everywhere(node_ids, locations)
        if obj.tag == 'way':
            # Add locations to nodes
            for nd in obj.findall('nd'):
                if not nd.get('lat'):
                    nd_id = nd.get('ref')
                    if nd_id in loc:
                        nd.set('lat', str(loc[nd_id][0]))
                        nd.set('lon', str(loc[nd_id][1]))
                        bounds.extend(*loc[nd_id])
                else:
                    bounds.extend(float(nd.get('lat')), float(nd.get('lon')))

        if obj.tag == 'relation':
            # Set coordinates only for nodes
            for nd in obj.findall('member'):
                if nd.get('type') == 'node' and not nd.get('lat'):
                    nd_id = nd.get('ref')
                    if nd_id in loc:
                        nd.set('lat', str(loc[nd_id][0]))
                        nd.set('lon', str(loc[nd_id][1]))
                        bounds.extend(*loc[nd_id])
                elif nd.get('type') == 'node':
                    bounds.extend(float(nd.get('lat')), float(nd.get('lon')))

        if not bounds.is_empty:
            if obj.find('bounds') is None:
                obj.append(bounds.to_xml())
        else:
            logging.debug('No bounds to add to %s %s', obj.tag, obj.get('id'))

    def copy_with_locations(self, parent, obj, locations):
        new = etree.SubElement(parent, obj.tag)
        for k, v in obj.items():
            new.set(k, v)
        for tag in obj.findall('tag'):
            tag_node = etree.SubElement(new, 'tag')
            tag_node.set('k', tag.get('k'))
            tag_node.set('v', tag.get('v'))

        if obj.tag == 'way':
            # Copy nodes
            for child in obj.findall('nd'):
                etree.SubElement(new, 'nd', ref=child.get('ref'))

        if obj.tag == 'relation':
            # Copy members
            for child in obj.findall('member'):
                etree.SubElement(
                    new, 'member', type=child.get('type'),
                    ref=child.get('ref'), role=child.get('role')
                )
        if obj.tag != 'node':
            self.add_locations(new, locations)
        return new

    def store_locations(self, obj):
        nodes = []
        if obj.tag == 'node':
            if obj.get('lat'):
                nodes.append((obj.get('id'), float(obj.get('lat')), float(obj.get('lon'))))
        elif obj.tag == 'way':
            for nd in obj.findall('nd'):
                if nd.get('lat'):
                    nodes.append((nd.get('ref'), float(nd.get('lat')), float(nd.get('lon'))))
        self.db.update_locations(nodes)

    def stored_to_xml(self, parent, stored):
        obj = etree.SubElement(
            parent, stored.typ,
            id=str(stored.osm_id), version=str(stored.version)
        )
        for k, v in stored.tags.items():
            etree.SubElement(obj, 'tag', k=k, v=v)
        if stored.nodes:
            for node_id in stored.nodes:
                if stored.typ == 'way':
                    etree.SubElement(obj, 'nd', ref=node_id)
                else:
                    etree.SubElement(obj, 'member', ref=node_id, type='node', role='')
        return obj

    def download_version(self, osm_type, osm_id, version):
        resp = requests.get(f'{OSM_API}/{osm_type}/{osm_id}/{version}')
        logging.debug('Queried OSM API for %s %s v%s, status code %s',
                      osm_type, osm_id, version, resp.status_code)
        if resp.status_code != 200:
            raise IOError(f'Could not download version {osm_type}/{osm_id}/{version}')
        return etree.fromstring(resp.content)[0]

    def iter_chunks(self, iterable, count):
        it = iter(iterable)
        while True:
            chunk = tuple(itertools.islice(it, count))
            if not chunk:
                return
            yield chunk

    def download_node_locations(self, node_ids):
        """
        Downloads locations from OSM API.
        Returns a dict of node_id -> (lat, lon).
        """
        if not node_ids:
            return {}
        loc = {}
        for chunk in self.iter_chunks(node_ids, 500):
            resp = requests.get(f'{OSM_API}/nodes', {'nodes': ",".join(chunk)})
            logging.debug('Requesting nodes from OSM API: %s. Status code %s',
                          ', '.join(chunk), resp.status_code)
            if resp.status_code != 200:
                raise KeyError(f'Missing node reference: {resp.text}. Req: {chunk}.')
            xmlresp = etree.fromstring(resp.content)
            for obj in xmlresp.findall('node'):
                if obj.get('lat'):
                    loc[obj.get('id')] = (float(obj.get('lat')), float(obj.get('lon')))

        # Now we need to test for deleted nodes
        for node_id in node_ids:
            if str(node_id) not in loc:
                resp = requests.get(f'{OSM_API}/node/{node_id}/history')
                logging.debug('Requested node %s history, status code %s',
                              node_id, resp.status_code)
                if resp.status_code != 200:
                    raise IOError(f'Failed to retrieve history for node {node_id}.')
                xmlresp = etree.fromstring(resp.content)
                for obj in reversed(xmlresp.findall('node')):
                    if obj.get('lat'):
                        loc[obj.get('id')] = (float(obj.get('lat')), float(obj.get('lon')))
                        break
        return loc

    def wrong_tags(self, obj, tags=None):
        if tags is None:
            tags = {t.get('k'): t.get('v') for t in obj.findall('tag')}
        return not self.tag_filter.is_empty and not self.tag_filter.get_kinds(obj.tag, tags)

    def process_osc(self, filename, adiff):
        logging.info('Reading osmChange file %s', filename)
        logging.info('Scanning for node locations')
        fileobj = gzip.open(filename)
        locations = self.scan_node_locations(fileobj)
        if not self.region_filter.is_empty:
            logging.info('Downloading missing node locations')
            self.scan_relevant_ways_nodes(fileobj, locations)
        logging.info('Iterating over actions')
        root = etree.Element('osm', version='0.6', generator='OSC to ADIFF')
        for _, action in etree.iterparse(fileobj, events=['end'],
                                         tag=['create', 'modify', 'delete']):
            for obj in action:
                obj_desc = f'Action {action.tag} {obj.tag} {obj.get("id")} v{obj.get("version")}'
                tags = {t.get('k'): t.get('v') for t in obj.findall('tag')}
                if not self.region_filter.is_empty:
                    # If tags are right, download a representative node from OSM API
                    point = self.get_representative_point(
                        obj, locations, not self.wrong_tags(obj, tags))
                    if not point or not self.region_filter.find(point[1], point[0]):
                        # No coords or coord is not in a region
                        coord_str = '(null)' if not point else f'({point[1]}, {point[0]})'
                        logging.debug('%s: %s outside of regions', obj_desc, coord_str)
                        obj.clear()
                        continue
                if action.tag == 'create':
                    # No tag history, just check what we have
                    if self.wrong_tags(obj, tags):
                        logging.debug('%s: no relevant tags', obj_desc)
                        obj.clear()
                        continue
                    # Simply copy as-is, adding locations to way nodes
                    na = etree.SubElement(root, 'action', type='create')
                    new = self.copy_with_locations(na, obj, locations)
                    # Store locations to db
                    self.store_locations(new)
                    # Add object to our database to monitor its changes
                    self.db.save_object(StoredObject(
                        obj.tag, obj.get('id'), obj.get('version'), tags,
                        self.get_node_ids(obj)
                    ))
                else:
                    old = db.read_object(obj.tag, obj.get('id'))
                    if not old and self.wrong_tags(obj, tags):
                        # Skipping if there is no history (meaning no relevant tags in old versions)
                        # and no relevant tags in the new version.
                        logging.debug('%s: no history and no relevant tags', obj_desc)
                        obj.clear()
                        continue
                    if action.tag == 'delete' and not old:
                        # Skip deletions of things we don't have history on
                        logging.debug('%s: no history, meaning no relevant tags', obj_desc)
                        obj.clear()
                        continue
                    na = etree.SubElement(root, 'action', type=action.tag)
                    na_old = etree.SubElement(na, 'old')
                    na_new = etree.SubElement(na, 'new')
                    if action.tag == 'delete':
                        # Restore old version
                        self.stored_to_xml(na_old, old)
                        # Add locations to old nodes and save them to db if needed
                        # Not passing locations to use stored ones.
                        self.add_locations(na_old[0])
                        # self.store_locations(na_old[0])  # not sure this is needed
                        # Note that even for ways there are no tags and no referenced nodes
                        self.copy_with_locations(na_new, obj, locations)
                        # Register deletion as zero tags to our database
                        self.db.save_object(StoredObject(
                            obj.tag, obj.get('id'), obj.get('version'), {}
                        ))
                    elif action.tag == 'modify':
                        # First copy new version with locations
                        new = self.copy_with_locations(na_new, obj, locations)
                        # Store locations to db
                        self.store_locations(new)
                        # Restore or download old version
                        if old:
                            self.stored_to_xml(na_old, old)
                            # Again, no current locations to use stored ones.
                            self.add_locations(na_old[0])
                        else:
                            old = self.download_version(
                                obj.tag, obj.get('id'), int(obj.get('version')) - 1)
                            if old is not None:
                                na_old.append(old)
                                self.add_locations(na_old[0], locations)
                        self.db.save_object(StoredObject(
                            obj.tag, obj.get('id'), obj.get('version'), tags,
                            self.get_node_ids(obj)
                        ))
                    else:
                        raise ValueError(f'Unknown osc action: {action.tag}')
                logging.debug('%s: written to augmented diff', obj_desc)
                obj.clear()
            action.clear()
        fileobj.close()
        logging.info('Done, writing the augmented diff')
        tree = etree.ElementTree(root)
        tree.write(adiff, pretty_print=True, encoding='utf-8')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Converts osmChange to Augmented Diffs based on tag and region filters.')
    parser.add_argument('action', choices=['init', 'process'])
    parser.add_argument('input', help='Source file, either a pbf or an osmChange')
    parser.add_argument('-a', '--adiff', type=argparse.FileType('wb'),
                        help='Augmented diff file to produce')
    parser.add_argument('-t', '--tags', type=argparse.FileType('r'),
                        help='File with a list of tags to watch')
    parser.add_argument('-r', '--regions', type=argparse.FileType('r'),
                        help='CSV file with names and wkb geometry for regions to filter')
    parser.add_argument('-v', '--verbose', action='count', default=0,
                        help='Print messages. Specify twice to print debug messages')
    psql = parser.add_argument_group('PostgreSQL connection')
    psql.add_argument('-d', '--database', required=True, help='PSQL database name')
    psql.add_argument('-H', '--dbhost', help='PSQL hostname, default is localhost')
    psql.add_argument('-P', '--dbport', type=int, help='PSQL port, default is 5432')
    psql.add_argument('-U', '--dbuser', help='PSQL user')
    psql.add_argument('-W', '--dbpass', help='PSQL password')
    options = parser.parse_args()

    if not options.verbose:
        log_level = logging.WARNING
    elif options.verbose == 1:
        log_level = logging.INFO
    else:
        log_level = logging.DEBUG
    logging.basicConfig(level=log_level, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    conn = psycopg2.connect(
        dbname=options.database,
        user=options.dbuser,
        password=options.dbpass,
        host=options.dbhost,
        port=options.dbport,
    )
    tags = TagFilter(options.tags)
    regions = RegionFilter(options.regions)
    db = OscDatabase(conn, tags)

    if options.action == 'init':
        db.create_tables()
        handler = InitHandler(db, tags, regions)
        handler.apply_file(options.input, locations=True)
    elif options.action == 'process':
        a = AdiffBuilder(db, tags, regions)
        a.process_osc(options.input, options.adiff)
    else:
        raise ValueError(f'Wrong action: {options.action}')
    db.close()
