from __future__ import absolute_import, unicode_literals

import argparse
import json
import logging
import os
import re
import shlex
import subprocess

import boto3

from .util import clean_s3_path
from .sstable import traverse_data_dir
from .transfer import TransferManager
from .watcher import Watcher


logger = logging.getLogger(__name__)


class Runner(object):
    def __init__(self, args):
        self.parse_filters(args)
        self.parse_node_info(args)
        self.parse_s3_options(args)
        self.parse_misc(args)

        self._s3_client = None

    def get_node_info_from_nodetool(self, cmd):
        cmd = list(cmd) + ['info']
        try:
            out = subprocess.check_output(cmd)
        except (subprocess.CalledProcessError, OSError) as e:
            raise RuntimeError('nodetool failed: {}'.format(e))

        data = {}
        for line in out.splitlines():
            match = re.match(r'^([^:]+?)\s+:\s+(.+)\s*$', line)
            if not match:
                continue

            key, value = match.group(1, 2)
            data[key] = value

        return data

    def parse_node_info(self, args):
        if args.node_host_id and args.node_datacenter:
            logger.info('Using provided host ID and DC')
            host_id = args.node_host_id
            datacenter = args.node_datacenter
        elif not args.node_host_id and not args.node_datacenter:
            logger.info('Determining host ID and DC using nodetool')

            # Run nodetool earlier than necessary, since it's much quicker to fail than
            # traversing the data dir to find the SSTables
            nodetool_cmd = shlex.split(os.environ.get('NODETOOL_CMD', 'nodetool'))
            node_info = self.get_node_info_from_nodetool(nodetool_cmd)
            host_id = node_info['ID']
            datacenter = node_info['Data Center']
        else:
            raise RuntimeError(
                'Must provide both --node-host-id and --node-datacenter, or '
                'neither')

        self.host_id = host_id
        self.datacenter = datacenter
        logger.info('Host ID: %s', host_id)
        logger.info('Datacenter: %s', datacenter)

    @property
    def s3_client(self):
        if self._s3_client is None:
            self._s3_client = boto3.client('s3')
        return self._s3_client

    def parse_s3_options(self, args):
        self.s3_bucket = args.s3_bucket
        self.s3_path = '{}/{}/{}'.format(clean_s3_path(args.s3_path),
                                         self.datacenter, self.host_id)

        logger.info('Storing backups at s3://%s/%s', self.s3_bucket,
                    self.s3_path)

        self.s3_settings = {
            'Metadata': args.s3_metadata,
            'ACL': args.s3_acl,
            'StorageClass': args.s3_storage_class
        }
        logger.info('Using S3 settings: %s', self.s3_settings)

    def _gen_filter(self, includes, excludes):
        includes = frozenset(includes)
        excludes = frozenset(excludes)

        def check(value):
            if includes and value not in includes:
                return False

            return value not in excludes

        return check

    def parse_filters(self, args):
        self.keyspace_filter = self._gen_filter(
            args.keyspaces, args.excluded_keyspaces)
        self.table_filter = self._gen_filter(
            args.tables, args.excluded_tables)

    def parse_misc(self, args):
        self.delete = args.delete
        self.dry_run = args.dry_run
        self.data_dirs = args.data_dirs

    def get_manager(self):
        return TransferManager(
            s3_client=self.s3_client, s3_bucket=self.s3_bucket,
            s3_path=self.s3_path, s3_settings=self.s3_settings,
            delete=self.delete, dry_run=self.dry_run)

    def get_watcher(self, manager):
        return Watcher(
            transfer_manager=manager, data_dirs=self.data_dirs,
            keyspace_filter=self.keyspace_filter,
            table_filter=self.table_filter)

    def _schedule_one_shot_uploads(self, manager, batch_size=1000):
        logger.info('Synchronizing existing SSTables')

        # Make the uploads in batches to avoid having to traverse all the data
        # directories before even starting to send something. Break and schedule
        # the current batch if it reached the maximum size, or the keyspace
        # or table has changed. This way we make the existing object search more
        # efficient by ensuring the SSTables scheduled together have a large
        # common path prefix (thus returning less wasted results).

        batch = []

        def schedule_batch():
            if batch:
                manager.schedule(batch)
                batch[:] = []

        for data_dir in self.data_dirs:
            sstables = traverse_data_dir(data_dir, self.keyspace_filter,
                                         self.table_filter)
            ks_table = None
            for sstable in sstables:
                last_ks_table, ks_table,  = \
                    ks_table, (sstable.keyspace, sstable.table)

                if last_ks_table != ks_table or len(batch) == batch_size:
                    schedule_batch()

                batch.append(sstable)

            schedule_batch()

    def run_one_shot(self):
        with self.get_manager() as manager:
            self._schedule_one_shot_uploads(manager)
            manager.close()
            manager.join()

    def run_watcher(self):
        with self.get_manager() as manager:
            # Ensure the watcher is started before queueing the transfer of
            # existing SSTables. Otherwise some tables could be missed if they
            # are created after the initial transfers have started, but before
            # the Watcher is seeing them.
            with self.get_watcher():
                logging.info('Watching SSTables')

                self._schedule_one_shot_uploads(manager)
                manager.join()


def positive_int(s):
    i = int(s)
    if i <= 0:
        raise ValueError('Value `{}` is not a positive integer'.format(s))
    return i


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('watchdog').setLevel(logging.WARN)
    logging.getLogger('boto3').setLevel(logging.WARN)
    logging.getLogger('botocore').setLevel(logging.WARN)
    logging.getLogger('s3transfer').setLevel(logging.WARN)

    argp = argparse.ArgumentParser()
    argp.add_argument(
        '--keyspace', action='append', dest='keyspaces', default=[],
        metavar='KEYSPACE',
        help='Only include given keyspace. Can be specified multiple times')
    argp.add_argument(
        '--exclude-keyspace', action='append', dest='excluded_keyspaces',
        default=[], metavar='KEYSPACE',
        help='Exclude given keyspace. Can be specified multiple times')
    argp.add_argument(
        '--table', action='append', dest='tables', default=[],
        metavar='TABLE',
        help='Only include given table. Can be specified multiple times')
    argp.add_argument(
        '--exclude-table', action='append', dest='excluded_tables',
        default=[], metavar='TABLE',
        help='Exclude given table. Can be specified multiple times')

    argp.add_argument(
        '--s3-bucket', required=True, metavar='BUCKET',
        help='Name of S3 bucket to send SSTables to')
    argp.add_argument(
        '--s3-path', default='/', metavar='PATH',
        help='Path inside S3 bucket to send SSTables to. Subdirectories for '
             'the datacenter name and host ID will be appended to it to '
             'determine the final path')
    argp.add_argument(
        '--s3-acl', default='private', metavar='ACL',
        help='Canned ACL to use for transfers')
    argp.add_argument(
        '--s3-metadata', default='{}', metavar='METADATA_JSON',
        type=json.loads,
        help='Metadata to apply to transferred files, in JSON format')
    argp.add_argument(
        '--s3-storage-class', default='STANDARD', metavar='STORAGE_CLASS',
        help='Storage class to apply to transferred files')

    argp.add_argument(
        '--node-host-id', default=None, metavar='UUID',
        help="Node ID to use when determining backup path. Leave empty"
             "to autodetect using nodetool")
    argp.add_argument(
        '--node-datacenter', default=None, metavar='DC',
        help="Datacenter to use when determining backup path. Leave empty"
             "to autodetect using nodetool")

    argp.add_argument(
        '--delete', default=False, action='store_true',
        help='Whether to delete transferred files after finishing. Files '
             'will only be deleted after all other files for the same SSTable '
             'have been successfully sent, to avoid leaving partial data '
             'behind')
    argp.add_argument(
        '--one-shot', default=False, action='store_true',
        help='Run synchronization once instead of watching continuously for '
             'new data')
    argp.add_argument(
        '--dry-run', default=False, action='store_true',
        help="Don't upload or delete any files, only print intended actions")
    argp.add_argument(
        '--batch-size', default=1000, type=positive_int,
        help="Number of SSTables to accumulate during traversal before "
             "uploading. Raising this number be more network ")

    argp.add_argument(
        'data_dirs', nargs='+', metavar='data_dir',
        help='Path to one or more data directories to find backup files in')

    args = argp.parse_args()
    runner = Runner(args)
    if args.one_shot:
        runner.run_one_shot()
    else:
        runner.run_watcher()
