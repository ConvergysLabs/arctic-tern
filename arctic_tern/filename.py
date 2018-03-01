import re

compile_ = re.compile(r'(\d+)\s?[-_]?\s?([\w\s-]*)\.sql')


class MigrationFile:
    def __init__(self, stamp, name, hash_ = None):
        self.stamp = stamp
        self.name = name
        self.hash_ = hash_
        self.path = None


def parse_file_name(name: str) -> MigrationFile:
    match_ = compile_.match(name)
    if match_:
        stamp = int(match_.group(1))
        name = match_.group(2) or None
        mf = MigrationFile(stamp, name)
        return mf
    return None
