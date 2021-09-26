import csv
from shapely import wkb
from shapely.geometry import Point
from shapely.strtree import STRtree


class TagFilter:
    def __init__(self, fileobj):
        self.relevant_keys = set()
        self.kinds = {'n': {}, 'w': {}, 'r': {}, 'a': {}}
        if fileobj:
            self.load(fileobj)

    @property
    def is_empty(self) -> bool:
        return not self.relevant_keys

    def load(self, fileobj):
        for row in fileobj:
            parts = [k.lower() for k in row.strip().split()]
            if len(parts) > 1:
                kind = parts[1].split('+')[0]
                tag = parts[-1].split('+')[0]
                self.kinds[parts[0][0]][tag] = kind
                self.relevant_keys.add(tag.split('=')[0])

    def check_context(self, ctx, tags1, tags2=None, strong_ctx=True) -> bool:
        if not ctx:
            return True
        if tags2 is None:
            tags2 = {}
        kv = ctx.split('=')
        if strong_ctx:
            if kv[0] not in tags1 or kv[0] not in tags2:
                return False
            if len(kv) > 1 and (tags1[kv[0]] != kv[1] or tags2[kv[0]] != kv[1]):
                return False
        else:
            if kv[0] not in tags1 and kv[0] not in tags2:
                return False
            if len(kv) > 1:
                if tags1.get(kv[0]) != kv[1] and tags2.get(kv[0]) != kv[1]:
                    return False
        return True

    def matches(self, tag, tags, ctx_backup=None) -> bool:
        if '+' in tag:
            # Check for context
            parts = tag.split('+')
            tag = parts[0]
            if not self.check_context(parts[1], tags, ctx_backup, False):
                return False
        # Check for the actual tag
        kv = tag.split('=')
        if kv[0] in tags:
            if len(kv) == 1 or tags[kv[0]] == kv[1]:
                return True
        return False

    def get_kinds(self, typ, tags, ctx_backup=None) -> set:
        """
        Receives a dict of tags and matches kinds to these.
        Set ctx_backup for another tag set for context tags checking.
        """
        if self.is_empty:
            return set()
        kinds = self.kinds.get(typ[0].lower(), {})
        result = set()
        for tag, kind in kinds.items():
            if self.matches(tag, tags, ctx_backup):
                result.add(kind)
        return result

    def get_modified_kinds(self, typ, tags_old, tags_new, strong_ctx=True) -> set:
        """
        Returns which kinds were modified, only for kinds both present in old and new.
        Set strong_ctx to false to allow context tags to be in just one of the objects.
        """
        result = set()
        if self.is_empty or not tags_old or not tags_new:
            return result
        kinds = self.kinds.get(typ[0].lower(), {})
        for tag, kind in kinds.items():
            if '+' in tag:
                # Check for context
                parts = tag.split('+')
                tag = parts[0]
                if not self.check_context(parts[1], tags_old, tags_new, strong_ctx):
                    continue
            # Check for the actual tag
            kv = tag.split('=')
            if kv[0] in tags_old and kv[0] in tags_new:
                if len(kv) == 1 and tags_old[kv[0]] != tags_new[kv[0]]:
                    result.add(kind)
                elif len(kv) > 1 and tags_old[kv[0]] == kv[1] and tags_new[kv[0]] == kv[1]:
                    if tags_old != tags_new:
                        result.add(kind)
        # Check for new or deleted tags of the same kind
        for kind in set(kinds.values()):
            kind_tags = set([t for t, k in kinds.items() if k == kind])
            old_opts = set([t for t in kind_tags if self.matches(
                t, tags_old, None if strong_ctx else tags_new)])
            new_opts = set([t for t in kind_tags if self.matches(
                t, tags_new, None if strong_ctx else tags_old)])
            if old_opts and new_opts and old_opts != new_opts:
                result.add(kind)
        return result

    def list_kinds(self, typ) -> dict:
        """Returns a dict of kind -> [(tag1, context1), (tag2,), ...]."""
        kinds = self.kinds.get(typ[0].lower(), {})
        result = {}
        for tag, kind in kinds.items():
            if kind not in result:
                result[kind] = []
            result[kind].append(tag.split('+'))
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
