from unittest.mock import AsyncMock, call

import pytest
from asyncpg.connection import Connection

from arctic_tern.migrate import migrate, migrate_multi

# All test coroutines will be treated as marked.
pytestmark = pytest.mark.asyncio


async def test_migrate():
    conn = AsyncMock(Connection)
    create_sql = f"""CREATE TABLE IF NOT EXISTS public.arctic_tern_migrations
                (
                    stamp bigint NOT NULL PRIMARY KEY,
                    file_name varchar,
                    sha3 char(56),
                    migrate_time timestamptz
                );"""
    calls = [
        call(create_sql),
        call("ALTER TABLE address ADD COLUMN apartment varchar;"),
        call(
            "INSERT INTO arctic_tern_migrations VALUES ($1, $2, $3, now())",
            2,
            "add-apartment",
            "714ac7e2ae171eddcb0687fd5fc81af95d83b9e49c8c48edc3d51be0",
        ),
    ]

    await migrate("tests/other", conn)

    conn.fetch.assert_awaited_with(
        "SELECT * FROM arctic_tern_migrations ORDER BY stamp asc"
    )
    conn.execute.assert_has_awaits(calls)


@pytest.mark.parametrize(
    "dirs", [(["tests/scripts", "tests/other"]), (["tests/other", "tests/scripts"]),]
)
async def test_migrate_multi(dirs):
    conn = AsyncMock(Connection)
    expected = [
        (1, "person-address"),
        (2, "add-apartment"),
        (3, "fail"),
    ]

    def check(sql, *args):
        if sql.startswith("INSERT INTO arctic_tern_migrations"):
            e = expected.pop(0)
            assert e == args[:2]

    conn.execute.side_effect = check

    await migrate_multi(dirs, conn)
