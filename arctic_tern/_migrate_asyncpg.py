import logging
import os
import re
from typing import List

from asyncpg.connection import Connection
from asyncpg.exceptions import PostgresError

from arctic_tern.filename import construct_migration, MigrationFile

log = logging.getLogger("arctic_tern.migrate")
illegal_schema_chars = re.compile(r"[^\w]+")


async def migrate(migration_dir: str, conn: Connection, schema: str = None):
    await _set_schema_path(conn, schema)
    await _migrate(_get_sql_files(migration_dir), conn, schema)


async def migrate_multi(migration_dirs: List[str], conn: Connection, schema: str = None):
    await _set_schema_path(conn, schema)
    combined_migrations = []
    for md in migration_dirs:
        combined_migrations.extend(_get_sql_files(md))
    await _migrate(combined_migrations, conn, schema)


async def _set_schema_path(conn: Connection, schema: str):
    if not schema:
        return

    if illegal_schema_chars.search(schema):
        raise ValueError("Illegal characters found in schema name")

    sql = f'SET search_path TO "{schema}"'
    await conn.execute(sql)


async def _migrate(migrations: List[MigrationFile], conn: Connection, schema):
    await _prepare_meta_table(conn, schema)
    prev_mig = await _fetch_previous_migrations(conn)
    prev_mig_iter = iter(prev_mig)
    current_mig = _next_or_none(prev_mig_iter)
    migrations.sort(key=lambda k: k.stamp)

    for migration in migrations:
        while migration.is_after(current_mig):
            log.info(f"IGNORE  {current_mig.stamp} {current_mig.name}")
            current_mig = _next_or_none(prev_mig_iter)

        stamp_match = False
        try:
            if migration.is_equal(current_mig):
                log.info(f"SKIP    {migration.stamp} {migration.name}")
                stamp_match = True
        except ValueError:
            log.warning(
                f"BADHASH {migration.stamp} {migration.name} Expected {current_mig.hash_} got {migration.hash_}"
            )
            stamp_match = True

        if stamp_match:
            current_mig = _next_or_none(prev_mig_iter)
        else:
            await _execute_file(migration, conn)


def _next_or_none(iterator):
    try:
        return next(iterator)
    except StopIteration:
        return None


async def _execute_file(migration_file: MigrationFile, conn: Connection):
    log.info(f"EXECUTE {migration_file.stamp} {migration_file.name}")

    try:
        async with conn.transaction():
            with open(migration_file.path) as stream:
                migration_sql = stream.read()
                await conn.execute(migration_sql)

        insert_sql = "INSERT INTO arctic_tern_migrations VALUES ($1, $2, $3, now())"
        await conn.execute(
            insert_sql, migration_file.stamp, migration_file.name, migration_file.hash_
        )
    except PostgresError as e:
        log.error(e)
        raise e


def _get_sql_files(dir: str) -> List[MigrationFile]:
    abs_dir = os.path.abspath(dir)
    file_list = []
    for fn in os.listdir(dir):
        file_info = construct_migration(fn, abs_dir)
        if file_info:
            file_list.append(file_info)

    return file_list


async def _prepare_meta_table(conn: Connection, schema: str):
    create = f"""CREATE TABLE IF NOT EXISTS {schema or 'public'}.arctic_tern_migrations
                (
                    stamp bigint NOT NULL PRIMARY KEY,
                    file_name varchar,
                    sha3 char(56),
                    migrate_time timestamptz
                );"""
    await conn.execute(create)


async def _fetch_previous_migrations(conn: Connection):
    sql = "SELECT * FROM arctic_tern_migrations ORDER BY stamp asc"

    migrations = [
        MigrationFile(record["stamp"], record["file_name"], None, record["sha3"])
        for record in await conn.fetch(sql)
    ]

    return migrations
