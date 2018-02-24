import os
import psycopg2.extras
from psycopg2.extensions import connection


def migrate(dir: str, schema: str = None, dsn: str = None, **kwargs):
    conn: connection = psycopg2.connect(dsn, **kwargs)
    sql_files = [f for f in os.listdir(dir) if f.endswith('.sql')]
    for sql_file in sql_files:
        print('Importing ' + sql_file)
        abs_api_dir = os.path.abspath(dir)
        full = os.path.join(abs_api_dir, sql_file)
        with open(full) as file:
            _execute_with_schema(conn, schema, file.read())
            # with conn.cursor() as curs:
            #     if schema:
            #         curs.execute('SET search_path TO %s', schema)
            #     curs.execute(file.read())
        conn.commit()

    conn.close()


def _prepare(conn: connection, schema: str):
    with conn.cursor() as curs:
        create = """CREATE TABLE arctic_tern_migrations IF NOT EXISTS
                    (
                        timestamp_ NOT NULL PRIMARY KEY,
                        file_name varchar,
                        sha1 char(40)
                    );"""

    pass


def _execute_with_schema(conn: connection, schema: str, *args, **kwargs):
    with conn.cursor() as curs:
        if schema:
            curs.execute('SET search_path TO %s', [schema])
        curs.execute(*args, **kwargs)


if __name__ == "__main__":
    migrate('../tests/scripts', dbname='mig', user='postgres', password='root')
    migrate('../tests/scripts', dbname='mig', user='postgres', password='root', schema='tern')