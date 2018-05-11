import glob
import logging
import os
import os.path
import re
import sys
from collections import namedtuple

from .util import clean_s3_path


logger = logging.getLogger(__name__)


class SSTable(namedtuple('SSTable', 'keyspace table name local_dir files')):
    __slots__ = ()

    def delete_files(self, dry_run=True):
        logger.debug("Deleting SSTable: %s", self)

        verb = 'Removed' if not dry_run else 'Removed (dry-run)'

        for f in self.files:
            path = os.path.join(self.local_dir, f.filename)
            if not dry_run:
                os.remove(path)
            sys.stdout.write('{}: {}\n'.format(verb, path))

    def storage_path(self, base=''):
        if base:
            base = clean_s3_path(base) + '/'

        return '{}{}/{}'.format(base, self.keyspace, self.table)


SSTableFile = namedtuple('SSTableFile', 'filename size')


def find_sstable_from_data(data_file, keyspace_name, table_name):
    logger.debug("Found SSTable data file: %s", data_file)

    # Use the Data file as pivot to find all the other files. It's hard to
    # tell if any other components are guaranteed to exist, so we just
    # assume the Data is.
    sstable_name = re.sub(r'-Data\.db$', '', os.path.basename(data_file))
    sstable_dir = os.path.dirname(data_file)
    sstable_paths = glob.glob(
        os.path.join(sstable_dir, sstable_name + '-*.*'))

    sstable_files = []
    for sstable_path in sstable_paths:
        # logger.debug("Found SSTable file: %s", sstable_path)
        sstable_files.append(SSTableFile(os.path.basename(sstable_path),
                                         os.path.getsize(sstable_path)))

    return SSTable(keyspace_name, table_name, sstable_name, sstable_dir,
                   tuple(sstable_files))


def traverse_backups_dir(path, keyspace_name, table_name):
    for data_file in glob.glob(os.path.join(path, '*-Data.db')):
        yield find_sstable_from_data(data_file, keyspace_name, table_name)


def traverse_keyspace_dir(path, keyspace_name, table_filter=None):
    for table_fname in os.listdir(path):
        table_path = os.path.join(path, table_fname)
        table_match = re.match(r'^([a-z][a-z0-9_]*)-[0-9a-f]{32}$',
                               table_fname, re.I)
        if not table_match:
            logger.debug("Ignoring invalid table directory: %s", table_fname)
            continue

        table_name = table_match.group(1)
        if table_filter and not table_filter(table_name):
            logger.debug("Ignoring filtered table directory: %s", table_fname)
            continue

        backups_path = os.path.join(table_path, 'backups')
        if not os.path.isdir(backups_path):
            logger.debug("Ignoring table directory without backups: %s",
                         table_path)
            continue

        for sstable in traverse_backups_dir(backups_path, keyspace_name,
                                            table_name):
            yield sstable


def traverse_data_dir(path, keyspace_filter=None, table_filter=None):
    logger.debug("Searching for SSTables in data dir: %s", path)

    for ks_fname in os.listdir(path):
        ks_path = os.path.join(path, ks_fname)
        if not os.path.isdir(ks_path):
            continue

        if not re.match(r'^[a-z0-9_]{,48}$', ks_fname, re.I):
            logger.debug("Ignoring invalid keyspace directory: %s", ks_fname)
            continue

        if keyspace_filter and not keyspace_filter(ks_fname):
            logger.debug("Ignoring filtered keyspace directory: %s", ks_fname)
            continue

        for sstable in traverse_keyspace_dir(ks_path, ks_fname, table_filter):
            yield sstable
