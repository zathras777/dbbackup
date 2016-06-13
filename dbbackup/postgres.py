import psycopg2
import os

from dbbackup.connector import Connector, which

PGDUMP = which('pg_dump')


class PostgresConnector(Connector):
    def _db_(self, dbname=None):
        settings = {'dbname': dbname or 'postgres',
                    'host': self.opts.get('host', 'localhost'),
                    'user': self.opts['username'],
                    'password': self.opts['password']}
        if 'port' in self.opts:
            settings['port'] = int(self.opts['port'])
        conn_string = " ".join(["{}='{}'".format(k, v) for k, v in settings.items()])
        try:
            return psycopg2.connect(conn_string)
        except psycopg2.OperationalError as e:
            print("Unable to open connection to postgres", e)
            return None

    def get_database_list(self):
        cursor = self.db.cursor()
        cursor.execute("select datname from pg_database")
        databases = []
        for row in cursor.fetchall():
            if row[0].startswith('template'):
                continue
            if row[0] == 'postgres' and self.opts.get('skip_postgres', True):
                continue
            databases.append(row[0])
        return databases

    def get_table_list(self, dbname):
        db = self._db_(dbname)
        cursor = db.cursor()
        cursor.execute("select tablename, schemaname from pg_tables where schemaname not in ('pg_catalog', 'information_schema')")
        tables = [(x[0], x[1]) for x in cursor.fetchall()]
        db.close()
        return tables

    def filename(self, database, table):
        return os.path.join(self.backup_dir, database, "{}_{}.sql".format(table[1], table[0]))

    def dump_table(self, dbname, table, ofn):
        conn = "postgresql://{}:{}@{}".format(self.opts['username'],
                                              self.opts['password'],
                                              self.opts.get('host', 'localhost'))
        if 'port' in self.opts:
            conn += ":{}".format(self.opts['port'])
        conn += "/{}".format(dbname)
        args = [PGDUMP, '--dbname', conn,
                '-t', "{}.{}".format(table[1], table[0]),
                '-n', table[1],
                '-f', ofn]
        return args
