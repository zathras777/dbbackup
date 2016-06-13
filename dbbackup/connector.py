import os
import stat
import subprocess
import tarfile
from datetime import datetime
from glob import glob

try:
    PermError = PermissionError
except NameError:
    PermError = OSError

def which(file):
    for path in os.environ["PATH"].split(os.pathsep):
        if os.path.exists(os.path.join(path, file)):
                return os.path.join(path, file)
    return None


class Connector(object):
    def __init__(self, opts, debug=False):
        self.backup_dir = opts.pop('backup_dir', '/tmp')
        self.archive_dir = opts.pop('archive_dir', self.backup_dir)
        self.opts = opts
        self.debug = debug
        self.db = None
        self.files = {}
        self.errors = []
        if self.check_directory():
            self.db = self._db_()

    def __del__(self):
        if self.db is not None:
            self.db.close()

    def check_directory(self, *extra):
        if len(extra) > 0 and os.path.isabs(extra[0]):
            full_path = os.path.join(*extra)
        else:
            full_path = os.path.join(self.backup_dir, *extra)
        print(full_path)
        if not os.path.exists(full_path):
            try:
                os.makedirs(full_path)
                os.chmod(full_path, stat.S_IRWXU)
            except PermError:
                print("ERROR: Unable to create directory {} due to permissions.".format(full_path))
                return False
        return True

    def filename(self, database, table):
        return os.path.join(self.backup_dir, database, table + '.sql')

    def archive_filename(self, dbname, extension):
        fn = "{}_{}.{}".format(dbname, datetime.today().strftime("%Y%m%d_%H%M"), extension)
        n = 0
        while os.path.exists(os.path.join(self.archive_dir, fn)):
            fn = "{}_{}_{}.{}".format(dbname, datetime.today().strftime("%Y%m%d_%H%M"), n, extension)
            n += 1
        return os.path.join(self.backup_dir, fn)

    def _gzip(self, db, silent=False):
        self.check_directory(self.archive_dir)
        afn = self.archive_filename(db, 'tar.gz')
        with tarfile.open(afn, "w:gz") as tf:
            for fn in self.files[db]:
                tf.add(fn, os.path.basename(fn))
        if not silent:
            print("  database {} archived as {}".format(db, afn))

    def process(self, silent=False):
        if silent:
            self.debug = False
        if self.db is None:
            if not silent:
                print("No database connection.")
            return
        databases = self.opts.get('databases', self.get_database_list())
        for db in databases:
            self.files[db] = []
            if self.check_directory(db) is False:
                continue
            if self.debug:
                print("Database {}".format(db))
            for tbl in self.get_table_list(db):
                fn = self.filename(db, tbl)
                cmd = self.dump_table(db, tbl, fn)
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                process.wait()
                if process.returncode == 0:
                    self.files[db].append(fn)
                    if self.debug:
                        print("  Dumped {}:{}".format(db, tbl))
                    os.chmod(fn, stat.S_IRWXU)
                else:
                    print("  ERROR: Unable to dump table {}".format(tbl))
                    self.errors.append("Unable to dump table {}".format(tbl))
                    self.errors.append(process.stderr.read().decode())

            if self.opts.get('gzip', False):
                self._gzip(db, silent)

        if len(self.errors):
            print("\nErrors during table dump:")
            print("\n".join(self.errors))
            print("\n\n")

        if 'keep' in self.opts:
            for db in databases:
                self.keep(db, int(self.opts['keep']))

    def keep(self, dbname, number):
        archives = glob(os.path.join(self.archive_dir, "{}_*".format(dbname)))
        for a in sorted(archives, reverse=True)[number:]:
            if self.debug:
                print("  Removing old archive {}".format(a))
            os.unlink(a)

    def _db_(self):
        raise NotImplementedError

    def get_database_list(self):
        raise NotImplementedError

    def get_table_list(self, dbname):
        raise NotImplementedError

    def dump_table(self, dbname, tbl, filename):
        raise NotImplementedError
