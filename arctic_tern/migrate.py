import hashlib
import os
from typing import List

import psycopg2
from psycopg2.extensions import connection, cursor

from arctic_tern.filename import parse_file_name, MigrationFile


def migrate(dir: str, conn: connection, schema: str = None):
    _prepare_meta_table(conn, schema)
    pm = _fetch_previous_migrations(_get_schema_cursor(conn, schema))
    pmi = iter(pm)
    cm = _next_or_none(pmi)

    for sql_file in _get_sql_files(dir):
        while sql_file.is_after(cm):
            print(f'Ignoring historical migration {cm.stamp} {cm.name}')
            cm = _next_or_none(pmi)

        if sql_file.is_equal(cm):
            print(f'Skipping previously executed {sql_file.stamp} {sql_file.name}')
            cm = _next_or_none(pmi)
        else:
            curs = _get_schema_cursor(conn, schema)
            _execute_file(sql_file, curs)
            curs.close()
            conn.commit()


def _next_or_none(iterator):
    try:
        return next(iterator)
    except StopIteration:
        return None


def _execute_file(migration_file: MigrationFile, curs: cursor):
    print(f'Executing migration {migration_file.stamp} {migration_file.name}')
    try:
        with open(migration_file.path) as stream:
            curs.execute(stream.read())
    except psycopg2.Error as e:
        print(e.pgerror)
        raise e

    t = """INSERT INTO arctic_tern_migrations VALUES (%s, %s, %s, now())"""
    curs.execute(t, [migration_file.stamp, migration_file.name, migration_file.hash_])
    print(f'Finished migration {migration_file.stamp} {migration_file.name}!')


def _get_sql_files(dir: str) -> List[MigrationFile]:
    abs_dir = os.path.abspath(dir)
    file_list = []
    for fn in os.listdir(dir):
        file_info = parse_file_name(fn)
        if file_info:
            full_path = os.path.join(abs_dir, fn)
            file_info.path = full_path
            file_info.hash_ = _hash(full_path)
            file_list.append(file_info)
    return file_list


def _hash(file: str) -> str:
    sha3 = hashlib.sha3_224()
    with open(file, "rb") as stream:
        for chunk in iter(lambda: stream.read(65536), b""):
            sha3.update(chunk)
    return sha3.hexdigest()


def _prepare_meta_table(conn: connection, schema: str):
    create = """CREATE TABLE IF NOT EXISTS {}.arctic_tern_migrations
                (
                    stamp bigint NOT NULL PRIMARY KEY,
                    file_name varchar,
                    sha3 char(56),
                    migrate_time timestamptz
                );"""
    c2 = create.format(schema or 'public')
    with conn.cursor() as curs:  # type: cursor
        curs.execute(c2)


def _fetch_previous_migrations(curs: cursor):
    sql = """SELECT * FROM arctic_tern_migrations"""
    curs.execute(sql)

    migrations = []
    for row in curs:
        stamp = row[0]
        name = row[1]
        sha3 = row[2]
        mf = MigrationFile(stamp, name, sha3)
        migrations.append(mf)

    return migrations


def _execute_with_schema(conn: connection, schema: str, *args, **kwargs):
    with conn.cursor() as curs: # type: cursor
        if schema:
            curs.execute('SET search_path TO %s', [schema])
        curs.execute(*args, **kwargs)


def _get_schema_cursor(conn: connection, schema: str = None) -> cursor:
    curs = conn.cursor()
    if schema:
        curs.execute('SET search_path TO %s', [schema])
    return curs


if __name__ == "__main__":
    pub = psycopg2.connect(dbname='mig', user='postgres', password='root')
    migrate('../tests/scripts', conn=pub)
    # tern = psycopg2.connect(dbname='mig', user='postgres', password='root', schema='tern')
    migrate('../tests/scripts', conn=pub, schema='tern')
    # print(_get_sql_files('../tests/scripts'))
