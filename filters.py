import csv
from shapely import wkb
from shapely.geometry import Point
from shapely.strtree import STRtree


class TagFilter:
    def __init__(self, fileobj):
        self.relevant_keys = set()
        self.kinds = {'n': {}, 'w': {}}
        if fileobj:
            self.load(fileobj)

    @property
    def is_empty(self) -> bool:
        return not self.relevant_keys

    def load(self, fileobj):
        for row in fileobj:
            parts = [k.lower() for k in row.strip().split()]
            if len(parts) > 1:
                self.kinds[parts[0][0]][parts[-1]] = parts[1]
                self.relevant_keys.add(parts[-1].split('=')[0])

    def get_kinds(self, typ, tags) -> set:
        """Receives a dict of tags and matches kinds to these."""
        if self.is_empty:
            return set()
        kinds = self.kinds[typ[0].lower()]
        result = set()
        for tag, kind in kinds.items():
            if '=' in tag:
                kv = tag.split('=')
                if tags.get(kv[0]) == kv[1]:
                    result.add(kind)
            else:
                if tag in tags:
                    result.add(kind)
        return result

    def list_kinds(self, typ) -> dict:
        """Returns a dict of kind -> [tag1, tag2, ...]."""
        kinds = self.kinds[typ[0].lower()]
        result = {}
        for tag, kind in kinds.items():
            if kind not in result:
                result[kind] = []
            result[kind].append(tag)
        return result

    def filter_relevant(self, tags) -> dict:
        if self.is_empty:
            return tags
        return {k: tags[k] for k in tags if k in self.relevant_keys}


class RegionFilter:
    def __init__(self, fileobj=None):
        self.tree = None
        self.region_map = {}
        if fileobj:
            self.load(fileobj)

    @property
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
        if self.is_empty:
            return None
        pt = Point(lon, lat)
        results = self.tree.query(pt)
        results = [r for r in results if r.contains(pt)]
        return None if not results else self.region_map[id(results[0])]
