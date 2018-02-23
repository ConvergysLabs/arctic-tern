import psycopg2.extras
import psycopg2.extensions

with open('../tests/scripts/1.sql') as f:
    for line in f.readlines():
        print(line)

conn: psycopg2.extensions.connection = psycopg2.connect(dbname='mig', user='postgres', password='root')

with conn:
    with conn.cursor() as curs: # type: psycopg2.extensions.cursor
        curs.execute('SET search_path TO tern')
        psycopg2.extras.register_uuid()
        curs.execute(open('../tests/scripts/1.sql').read())

conn.close()
