import logging
import os
import shutil
import uuid
from contextlib import ExitStack
from random import getrandbits

import boto3
import pytest
from fallocate import fallocate

from cassandra_s3_incremental_backup_watcher.sstable import SSTable, SSTableFile
from cassandra_s3_incremental_backup_watcher.main import Runner

logger = logging.getLogger(__name__)


class SSTableGenerator(object):
    SSTABLE_FILE_TYPES = [
      "CompressionInfo.db",
      "Data.db",
      "Digest.crc32",
      "Filter.db",
      "Index.db",
      "Statistics.db",
      "Summary.db",
      "TOC.txt"
    ]

    @classmethod
    def table_suffix(cls):
        return '{:x}'.format(getrandbits(16 * 8))

    def __init__(self, base_dir, keyspace, table, size=0):
        table_instance = '{}-{}'.format(table, self.table_suffix())

        self.keyspace = keyspace
        self.table = table
        self.size = size
        self.local_dir = os.path.join(
            base_dir, keyspace, table_instance, 'backups')

        self._number = 1
        self._sstables = []

    @property
    def sstables(self):
        return self._sstables

    def _generate(self, number):
        sstable_name = "mc-{}-big".format(number)

        files = []
        for file_type in self.SSTABLE_FILE_TYPES:
            filename = "{}-{}".format(sstable_name, file_type)
            path = os.path.join(self.local_dir, filename)

            with open(path, 'wb') as f:
                if self.size != 0:
                    fallocate(f, 0, self.size)

            files.append(SSTableFile(filename=filename, size=self.size))

        return SSTable(keyspace=self.keyspace, table=self.table,
                       name=sstable_name, local_dir=self.local_dir, files=files)

    def generate(self, count=1):
        for _ in range(count):
            sstable = self._generate(self._number)
            self._number += 1
            self._sstables.append(sstable)

    def __enter__(self):
        os.makedirs(self.local_dir + '/', exist_ok=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            shutil.rmtree(self.local_dir)
        except Exception:
            logger.warn('Failed to clean up temporary SSTable dir: %s',
                        self.local_dir, exc_info=True)


class TestRunner(Runner):
    def __init__(self, data_dirs, delete=False, dry_run=True):
        super(TestRunner, self).__init__(None)

        self.delete = delete
        self.dry_run = dry_run
        self.data_dirs = data_dirs

    def parse_node_info(self, args):
        self.datacenter = 'datacenter1'
        self.host_id = str(uuid.uuid4())

    def parse_s3_options(self, args):
        self.s3_bucket = 'test'
        self.s3_path = '{}/{}/{}'.format('test', self.datacenter, self.host_id)
        self.s3_settings = {}

    def parse_filters(self, args):
        self.keyspace_filter = lambda ks: True
        self.table_filter = lambda tbl: True

    def parse_misc(self, args):
        pass

    @property
    def s3_client(self):
        return boto3.client(
            's3', region_name='us-east-1',
            aws_access_key_id='WEBSCGVPTHKQ0BH6B62L', aws_secret_access_key='gft5dPKqmvGG1MQsKhiyqNG8GCT7MSl64v9/k22d',
            endpoint_url='http://127.0.0.1:9000/')


@pytest.fixture
def s3_res():
    endpoint = os.environ.get('AWS_S3_ENDPOINT_URL')
    res = boto3.resource('s3', endpoint_url=endpoint)
    yield res


@pytest.fixture
def s3_client(s3_res):
    return s3_res.client


@pytest.fixture
def keyspaces():
    return ('ks1', 'ks2')


@pytest.fixture
def tables():
    return ('tbl1', 'tbl2')


@pytest.fixture
def sstable_generators(tmpdir, keyspaces, tables):
    with ExitStack() as stack:
        gens = [stack.enter_context(SSTableGenerator(
                    str(tmpdir), keyspace, table, size=100))
                for keyspace in keyspaces
                for table in tables]
        yield gens


@pytest.fixture
def sstables(sstable_generators):
    sstables = []

    for gen in sstable_generators:
        gen.generate(2)
        sstables.extend(gen.sstables)

    return sstables
