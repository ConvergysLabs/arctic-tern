from arctic_tern._migrate_psycopg import migrate, migrate_multi
import pytest
from unittest.mock import MagicMock, call


def test_migrate():
    conn = MagicMock()
    migrate("tests/other", conn)
    calls = [
        call('SELECT * FROM arctic_tern_migrations ORDER BY stamp asc'),
        call('ALTER TABLE address ADD COLUMN apartment varchar;'),
        call('INSERT INTO arctic_tern_migrations VALUES (%s, %s, %s, now())', [2, 'add-apartment', '714ac7e2ae171eddcb0687fd5fc81af95d83b9e49c8c48edc3d51be0'])
    ]
    conn.cursor().execute.assert_has_calls(calls)


@pytest.mark.parametrize("dirs", [
    (["tests/scripts", "tests/other"]),
    (["tests/other", "tests/scripts"]),
])
def test_migrate_multi(dirs):
    conn = MagicMock()
    expected = [
        [1, "person-address"],
        [2, "add-apartment"],
        [3, "fail"],
    ]

    def check(sql, *args):
        if sql.startswith("INSERT INTO arctic_tern_migrations"):
            e = expected.pop(0)
            assert e == args[0][:2]

    conn.cursor().execute.side_effect = check

    migrate_multi(dirs, conn)