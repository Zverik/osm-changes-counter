import json
from psycopg2.extras import execute_values


TABLE_OBJECTS = 'osc_watched_objects'
TABLE_LOCATIONS = 'osc_node_locations'
COORD_MULTIPLIER = 10000000
FULL_TYPES = {'n': 'node', 'w': 'way', 'r': 'relation'}


class StoredObject:
    def __init__(self, typ, osm_id, version, tags, nodes=None):
        self.typ = FULL_TYPES[typ[0].lower()]
        self.osm_id = int(osm_id)
        self.version = int(version)
        if nodes is None:
            self.nodes = None
        elif isinstance(nodes, str):
            self.nodes = [int(n) for n in nodes.split(',')]
        else:
            self.nodes = None if not nodes else [int(n) for n in nodes]
        self.tags = json.loads(tags) if isinstance(tags, str) else tags

    @property
    def nodes_str(self):
        return None if not self.nodes else ','.join([str(n) for n in self.nodes])

    @property
    def db_id(self):
        return f'{self.typ[0]}{self.osm_id}'


class OscDatabase:
    def __init__(self, conn, tag_filter=None):
        self.conn = conn
        self.cur = conn.cursor()
        self.tag_filter = tag_filter

    def close(self):
        if self.cur:
            self.cur.close()
        self.conn.commit()
        self.conn.close()

    def create_tables(self):
        self.cur.execute(f"drop table if exists {TABLE_OBJECTS}")
        self.cur.execute(f"drop table if exists {TABLE_LOCATIONS}")
        self.cur.execute(f"""create table {TABLE_OBJECTS} (
            osm_id text primary key,
            version integer,
            tags text,
            nodes text)""")
        self.cur.execute(f"""create table {TABLE_LOCATIONS} (
            node_id bigint primary key,
            lat integer not null,
            lon integer not null)""")

    def read_object(self, typ, osm_id):
        self.cur.execute(
            f"select version, tags, nodes from {TABLE_OBJECTS} where osm_id = %s",
            (f'{typ[0]}{osm_id}',))
        row = self.cur.fetchone()
        if not row:
            return None
        return StoredObject(typ, osm_id, row[0], row[1], row[2])

    def save_object(self, obj):
        tags = obj.tags if not self.tag_filter else self.tag_filter.filter_relevant(obj.tags)
        self.cur.execute(
            f"""insert into {TABLE_OBJECTS}
            (osm_id, version, tags, nodes) values (%s, %s, %s, %s)
            on conflict (osm_id) do update set tags = EXCLUDED.tags,
            version = EXCLUDED.version, nodes = EXCLUDED.nodes""",
            (obj.db_id, obj.version, json.dumps(tags), obj.nodes_str)
        )

    def update_locations(self, nodes):
        """nodes is a list of (node_id, lat, lon)."""
        if not nodes:
            return
        # Deduplicate nodes
        node_dict = {n[0]: (n[1], n[2]) for n in nodes}
        execute_values(
            self.cur, f"""insert into {TABLE_LOCATIONS} (node_id, lat, lon) values %s
            on conflict (node_id) do update set lat = EXCLUDED.lat, lon = EXCLUDED.lon""",
            [(int(node_id), round(coord[0] * COORD_MULTIPLIER),
              round(coord[1] * COORD_MULTIPLIER)) for node_id, coord in node_dict.items()]
        )

    def get_locations(self, node_ids):
        """Returns dict of node_id -> (lat, lon)."""
        self.cur.execute(
            f"select node_id, lat, lon from {TABLE_LOCATIONS} where node_id in %s",
            (tuple(node_ids),))
        coords = {}
        for row in self.cur:
            coords[str(row[0])] = (row[1] / COORD_MULTIPLIER, row[2] / COORD_MULTIPLIER)
        return coords
