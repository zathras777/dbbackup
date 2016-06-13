import MySQLdb

from dbbackup.connector import Connector, which

MYSQLDUMP = which('mysqldump')


class MysqlConnector(Connector):
    def _db_(self):
        return MySQLdb.connect(self.opts.get('host', 'localhost'),
                               self.opts['username'],
                               self.opts['password'])

    def get_database_list(self):
        if self.db is None:
            print("No connection available to MySQL.")
            return[]
        cursor = self.db.cursor()
        cursor.execute("SHOW databases")
        databases = []
        for row in cursor.fetchall():
            if '_schema' in row[0] or row[0] == 'sys':
                continue
            if row[0] == 'mysql' and self.opts.get('skip_mysql', True):
                continue
            databases.append(row[0])
        return databases

    def get_table_list(self, dbname):
        if self.db is None:
            print("No connection available to MySQL.")
            return[]
        cursor = self.db.cursor()
        cursor.execute("USE {}".format(dbname))
        cursor.execute("SHOW tables")
        return [x[0] for x in cursor.fetchall()]

    def dump_table(self, dbname, table, ofn):
        args = [MYSQLDUMP, '-h', self.opts.get('host', 'localhost'),
                '-u', self.opts['username'],
                '--password={}'.format(self.opts['password']),
                '-r', ofn]
        if 'port' in self.opts:
            args.extend(['-P', self.opts['port']])
        args.extend([dbname, table])
        return args
