import re

compile_ = re.compile(r'(\d+)\s?[-_]?\s?([\w\s-]*)\.sql')


def parse_file_name(name: str):
    match_ = compile_.match(name)
    if match_:
        stamp = int(match_.group(1))
        name = match_.group(2) or None
        return [stamp, name]
    return None
