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
from .transfer import generate_transfers


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
        '--delete', default=False, action='store_true',
        help='Whether to delete transferred files after finishing. Files '
             'will only be deleted after all other files for the same SSTable '
             'have been successfully sent, to avoid leaving partial data '
             'behind')
    argp.add_argument(
        '--dry-run', default=False, action='store_true',
        help="Don't upload or delete any files, only print intended actions ")
    argp.add_argument(
        'data_dirs', nargs='+', metavar='data_dir',
        help='Path to one or more data directories to find backup files in')

    args = argp.parse_args()

    # Run nodetool earlier than necessary, since it's much quicker to fail than
    # traversing the data dir to find the SSTables
    nodetool_cmd = shlex.split(os.environ.get('NODETOOL_CMD', 'nodetool'))
    node_info = get_node_info(nodetool_cmd)
    host_id = node_info['ID']
    data_center = node_info['Data Center']

    host_id = 'ed76c4a4-c7d1-11e7-8ff8-9cb6d0d1faaf'
    data_center = 'test1'

    keyspace_filter = check_includes_excludes(
        args.keyspaces, args.excluded_keyspaces)
    table_filter = check_includes_excludes(
        args.tables, args.excluded_tables)

    sstables = []
    for data_dir in args.data_dirs:
        sstables.extend(traverse_data_dir(data_dir, keyspace_filter,
                                          table_filter))

    s3_client = boto3.client('s3')
    s3_path = '{}/{}/{}'.format(clean_s3_path(args.s3_path), data_center,
                                host_id)
    transfers = list(generate_transfers(s3_client, args.s3_bucket, s3_path,
                                        sstables))

    s3_settings = {
        'Metadata': args.s3_metadata,
        'ACL': args.s3_acl,
        'StorageClass': args.s3_storage_class
    }

    for transfer in transfers:
        transfer.run(s3_client, s3_settings, delete=args.delete,
                     dry_run=args.dry_run)
