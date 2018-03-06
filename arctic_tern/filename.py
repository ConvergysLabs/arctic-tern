import re

compile_ = re.compile(r'(\d+)\s?[-_]?\s?([\w\s-]*)\.sql')


class MigrationFile:
    def __init__(self, stamp, name, hash_=None):
        self.stamp = stamp
        self.name = name
        self.hash_ = hash_
        self.path = None

    def __eq__(self, other: 'MigrationFile'):
        return self.stamp == other.stamp \
               and self.name == other.name \
               and self.hash_ == other.hash_

    def is_equal(self, other: 'MigrationFile'):
        if other is None:
            return False

        if self.stamp == other.stamp:
            if self.hash_ != other.hash_:
                raise ValueError('Hash mismatch in file {}.  It has likely been modified.'.format(self.path))
            else:
                return True
        else:
            return False


def parse_file_name(name: str) -> MigrationFile:
    match_ = compile_.match(name)
    if match_:
        stamp = int(match_.group(1))
        name = match_.group(2) or None
        mf = MigrationFile(stamp, name)
        return mf
    return None
