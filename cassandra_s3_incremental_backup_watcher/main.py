from __future__ import absolute_import, unicode_literals

import argparse
import json
import logging
import os
import re
import shlex
import subprocess
import sys
import time

import boto3

from .util import clean_s3_path
from .sstable import traverse_data_dir
from .transfer import TransferManager
from .watcher import Watcher


logger = logging.getLogger(__name__)


def get_node_info(nodetool_cmd):
    cmd = nodetool_cmd + ['info']
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


def check_includes_excludes(includes, excludes):
    includes = frozenset(includes)
    excludes = frozenset(excludes)

    def check(value):
        if includes and value not in includes:
            return False

        return value not in excludes

    return check


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
        help="Don't upload or delete any files, only print intended actions ")
    argp.add_argument(
        'data_dirs', nargs='+', metavar='data_dir',
        help='Path to one or more data directories to find backup files in')

    args = argp.parse_args()

    if args.node_host_id and args.node_datacenter:
        logger.info('Using provided host ID and DC')
        host_id = args.node_host_id
        datacenter = args.node_datacenter
    elif not args.node_host_id and not args.node_datacenter:
        logger.info('Determining host ID and DC using nodetool')

        # Run nodetool earlier than necessary, since it's much quicker to fail than
        # traversing the data dir to find the SSTables
        nodetool_cmd = shlex.split(os.environ.get('NODETOOL_CMD', 'nodetool'))
        node_info = get_node_info(nodetool_cmd)
        host_id = node_info['ID']
        datacenter = node_info['Data Center']
    else:
        argp.error('Must provide both --node-host-id and --node-datacenter, '
                   'or neither')
        sys.exit(1)

    logger.info('Host ID: %s, Datacenter: %s', host_id, datacenter)

    keyspace_filter = check_includes_excludes(
        args.keyspaces, args.excluded_keyspaces)
    table_filter = check_includes_excludes(
        args.tables, args.excluded_tables)

    s3_client = boto3.client('s3')
    s3_path = '{}/{}/{}'.format(clean_s3_path(args.s3_path), datacenter,
                                host_id)

    logger.info('Storing backups at s3://%s/%s', args.s3_bucket, s3_path)

    s3_settings = {
        'Metadata': args.s3_metadata,
        'ACL': args.s3_acl,
        'StorageClass': args.s3_storage_class
    }

    logger.debug('S3 settings: %s', s3_settings)

    manager = TransferManager(
        s3_client=s3_client, s3_bucket=args.s3_bucket, s3_path=s3_path,
        s3_settings=s3_settings, delete=args.delete, dry_run=args.dry_run)

    with manager:
        logger.debug('Started TransferManager')

        def schedule_one_shot():
            sstables = []
            for data_dir in args.data_dirs:
                sstables.extend(traverse_data_dir(data_dir, keyspace_filter,
                                                  table_filter))

            logger.info(
                'Starting initial SSTable sync (%d found)', len(sstables))
            manager.schedule(sstables)

        if args.one_shot:
            schedule_one_shot()
            manager.close()
        else:
            # Make sure to start the watcher before queueing the transfer of
            # existing SSTables. Otherwise some tables could be missed if they
            # are created after the initial transfers have started, but the
            # Watcher is not running yet.
            watcher = Watcher(
                transfer_manager=manager, data_dirs=args.data_dirs,
                keyspace_filter=keyspace_filter, table_filter=table_filter)
            with watcher:
                logging.info('Watching SSTables')
                schedule_one_shot()

                while True:
                    time.sleep(1)
