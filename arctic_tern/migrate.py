import hashlib
import os

import psycopg2.extras
from psycopg2.extensions import connection, cursor

from arctic_tern.filename import parse_file_name


def migrate(dir: str, schema: str = None, dsn: str = None, **kwargs):
    conn: connection = psycopg2.connect(dsn, **kwargs)
    sql_files = [f for f in os.listdir(dir) if f.endswith('.sql')]
    for sql_file in sql_files:
        print('Importing ' + sql_file)
        abs_api_dir = os.path.abspath(dir)
        full = os.path.join(abs_api_dir, sql_file)
        with open(full) as file:
            _execute_with_schema(conn, schema, file.read())
        conn.commit()

    conn.close()


def _get_sql_file(dir: str):
    l = []
    for fn in os.listdir(dir):
        v = parse_file_name(fn)
        if v:
            v.append(_hash(dir, fn))
            l.append(v)
    return l


def _hash(dir, file):
    abs_dir = os.path.abspath(dir)
    full = os.path.join(abs_dir, file)
    sha256 = hashlib.sha256()
    with open(full, "rb") as stream:
        for chunk in iter(lambda: stream.read(65536), b""):
            sha256.update(chunk)
    hexdigest = sha256.hexdigest()
    return hexdigest


def _prepare(conn: connection, schema: str):
    create = """CREATE TABLE arctic_tern_migrations IF NOT EXISTS
                (
                    stamp NOT NULL PRIMARY KEY,
                    file_name varchar,
                    sha1 char(64)
                );"""
    _execute_with_schema(conn, schema, create)

    pass


def _execute_with_schema(conn: connection, schema: str, *args, **kwargs):
    with conn.cursor() as curs: # type: cursor
        if schema:
            curs.execute('SET search_path TO %s', [schema])
        curs.execute(*args, **kwargs)


if __name__ == "__main__":
    # migrate('../tests/scripts', dbname='mig', user='postgres', password='root')
    # migrate('../tests/scripts', dbname='mig', user='postgres', password='root', schema='tern')
    print(_get_sql_file('../tests/scripts'))