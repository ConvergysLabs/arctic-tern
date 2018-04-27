import re
import os
import hashlib
from typing import Tuple

compile_ = re.compile(r'(\d+)\s?[-_]?\s?([\w\s-]*)\.sql')


class MigrationFile:
    def __init__(self, stamp, name, path, hash_=None):
        self.stamp = stamp
        self.name = name
        self.path = path
        self.hash_ = hash_

    def is_after(self, other: 'MigrationFile'):
        if other is None:
            return False
        return self.stamp > other.stamp

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

    def __str__(self):
        return f'Migration [{self.stamp} {self.name}]'


def construct_migration(name: str, abs_dir: str) -> MigrationFile:
    try:
        stamp, label = parse_file_name(name)
        full_path = os.path.join(abs_dir, name)
        hash_ = _hash(full_path)
        mf = MigrationFile(stamp, label, full_path, hash_)
        return mf
    except TypeError:
        return None


def parse_file_name(name: str) -> Tuple[int, str]:
    match_ = compile_.match(name)
    if match_:
        stamp = int(match_.group(1))
        label = match_.group(2) or None
        return stamp, label
    return None


def _hash(file: str) -> str:
    sha3 = hashlib.sha3_224()
    with open(file, "rb") as stream:
        for chunk in iter(lambda: stream.read(65536), b""):
            sha3.update(chunk)
    return sha3.hexdigest()
