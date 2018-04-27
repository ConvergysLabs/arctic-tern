import os
from typing import List

import psycopg2
from psycopg2.extensions import connection, cursor

from arctic_tern.filename import construct_migration, MigrationFile


def migrate(migration_dir: str, conn: connection, schema: str = None):
    _migrate(_get_sql_files(migration_dir), conn, schema)


def migrate_multi(migration_dirs: List[str], conn: connection, schema: str = None):
    combined_migrations = []
    for md in migration_dirs:
        combined_migrations.extend(_get_sql_files(md))
    _migrate(combined_migrations, conn, schema)


def _migrate(migrations: List[MigrationFile], conn: connection, schema: str = None):
    _prepare_meta_table(conn, schema)
    prev_mig = _fetch_previous_migrations(_get_schema_cursor(conn, schema))
    prev_mig_iter = iter(prev_mig)
    current_mig = _next_or_none(prev_mig_iter)
    migrations.sort(key=lambda k: k.stamp)

    for migration in migrations:
        while migration.is_after(current_mig):
            print(f'IGNORE  {current_mig.stamp} {current_mig.name}')
            current_mig = _next_or_none(prev_mig_iter)

        if migration.is_equal(current_mig):
            print(f'SKIP    {migration.stamp} {migration.name}')
            current_mig = _next_or_none(prev_mig_iter)
        else:
            curs = _get_schema_cursor(conn, schema)
            _execute_file(migration, curs)
            curs.close()
            conn.commit()


def _next_or_none(iterator):
    try:
        return next(iterator)
    except StopIteration:
        return None


def _execute_file(migration_file: MigrationFile, curs: cursor):
    print(f'EXECUTE {migration_file.stamp} {migration_file.name}')
    try:
        with open(migration_file.path) as stream:
            curs.execute(stream.read())
    except psycopg2.Error as e:
        print(e.pgerror)
        raise e

    t = """INSERT INTO arctic_tern_migrations VALUES (%s, %s, %s, now())"""
    curs.execute(t, [migration_file.stamp, migration_file.name, migration_file.hash_])


def _get_sql_files(dir: str) -> List[MigrationFile]:
    abs_dir = os.path.abspath(dir)
    file_list = []
    for fn in os.listdir(dir):
        file_info = construct_migration(fn, abs_dir)
        if file_info:
            file_list.append(file_info)

    return file_list


def _prepare_meta_table(conn: connection, schema: str):
    create = f"""CREATE TABLE IF NOT EXISTS {schema or 'public'}.arctic_tern_migrations
                (
                    stamp bigint NOT NULL PRIMARY KEY,
                    file_name varchar,
                    sha3 char(56),
                    migrate_time timestamptz
                );"""
    with conn.cursor() as curs:  # type: cursor
        curs.execute(create)


def _fetch_previous_migrations(curs: cursor):
    sql = """SELECT * FROM arctic_tern_migrations ORDER BY stamp asc"""
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
    for f in _get_sql_files('../tests/scripts'):
        print(f)
    # migrate('../tests/scripts', conn=pub)
    # tern = psycopg2.connect(dbname='mig', user='postgres', password='root', schema='tern')
    # migrate('../tests/scripts', conn=pub, schema='tern')
    # print(_get_sql_files('../tests/scripts'))
