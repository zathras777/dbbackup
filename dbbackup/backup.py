import copy

from dbbackup.mysql import MysqlConnector
from dbbackup.postgres import PostgresConnector

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser


class Backerupper(object):
    def __init__(self, config_fn, debug=False):
        self.backups = {}
        self.config_fn = config_fn
        self.parse_config_file(debug)

    def parse_config_file(self, debug=False):
        cfg = ConfigParser.ConfigParser()
        cfg.read(self.config_fn)
        sects = cfg.sections()
        common = {}
        if 'Common' in sects:
            common = {k: cfg.get('Common', k) for k in cfg.options('Common')}
            sects.pop(sects.index('Common'))

        for sect in sects:
            opts = copy.copy(common)
            opts.update({k: cfg.get(sect, k) for k in cfg.options(sect)})
            typ = opts.pop('type', None)
            if typ is None:
                print("Skipping backup {} as no type specified".format(sect))
                continue
            if typ not in ['mysql', 'postgres']:
                print("Invalid type specified, {}.".format(opts['type']))
                continue

            if typ == 'mysql':
                self.backups[sect] = MysqlConnector(opts, debug)
            elif typ == 'postgres':
                self.backups[sect] = PostgresConnector(opts, debug)

    def start(self, silent=False):
        for name in self.backups:
            if not silent:
                print("Processing {}".format(name))
            conn = self.backups[name]
            conn.process(silent)
