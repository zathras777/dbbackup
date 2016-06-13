import argparse
import sys

from dbbackup.backup import Backerupper
from dbbackup import __version__


def main():
    parser = argparse.ArgumentParser(description='Database Backup Utility')
    parser.add_argument('--config', default='/usr/local/etc/db_backup.conf', help='Configuration file to process')
    parser.add_argument('--debug', action='store_true', help="Debug")
    parser.add_argument('--silent', action='store_true', help="Only output errors")

    args = parser.parse_args()

    if not args.silent:
        print("db_backup version {}".format(__version__))
    backup = Backerupper(args.config, args.debug)
    if len(backup.backups) == 0:
        if not args.silent:
            print("Nothing to be done, exiting.")
        sys.exit(0)

    backup.start(args.silent)
