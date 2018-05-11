import logging
import os
import shutil
import socket
import subprocess
import tempfile
import time
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
    def __init__(self, data_dirs, s3_resource, s3_bucket, delete=False,
                 dry_run=True):
        super(TestRunner, self).__init__(None)

        self.delete = delete
        self.dry_run = dry_run
        self.data_dirs = data_dirs
        self.s3_bucket = s3_bucket
        self._s3_resource = s3_resource

    def parse_node_info(self, args):
        self.datacenter = 'datacenter1'
        self.host_id = str(uuid.uuid4())

    def parse_s3_options(self, args):
        self.s3_path = '{}/{}/{}'.format('test', self.datacenter, self.host_id)
        self.s3_settings = {}

    def parse_filters(self, args):
        self.keyspace_filter = lambda ks: True
        self.table_filter = lambda tbl: True

    def parse_misc(self, args):
        pass


@pytest.fixture
def keyspaces():
    return ('ks1', 'ks2')


@pytest.fixture
def tables():
    return ('tbl1', 'tbl2')


@pytest.fixture
def sstable_dir(tmpdir):
    return tmpdir.mkdir('sstables')


@pytest.fixture
def sstables(sstable_dir, keyspaces, tables):
    with ExitStack() as stack:
        sstables = []
        for keyspace in keyspaces:
            for table in tables:
                gen = stack.enter_context(
                    SSTableGenerator(str(sstable_dir), keyspace, table, size=100))
                gen.generate(5)
                sstables.extend(gen.sstables)

        yield sstables


def wait_for_port(host, port, timeout=10):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            sock = socket.create_connection((host, port), 1)
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()
        except socket.error:
            continue
        else:
            return

    raise socket.timeout('Waiting for {}:{}'.format(host, port))


@pytest.fixture(scope='session')
def s3_endpoint():
    access_key = 'test'
    secret_key = 'test1234'

    minio_data_dir = tempfile.mkdtemp()
    try:
        minio_env = os.environ.copy()
        minio_env.update({'MINIO_ACCESS_KEY': access_key,
                          'MINIO_SECRET_KEY': secret_key})

        proc = subprocess.Popen(
            ['minio', 'server', str(minio_data_dir)], env=minio_env)
        try:
            wait_for_port('127.0.0.1', 9000)

            os.environ['AWS_ACCESS_KEY_ID'] = access_key
            os.environ['AWS_SECRET_ACCESS_KEY'] = secret_key
            yield 'http://127.0.0.1:9000/'
        finally:
            proc.terminate()
            proc.wait()
    finally:
        shutil.rmtree(minio_data_dir)


@pytest.fixture
def s3_resource(s3_endpoint):
    return boto3.resource('s3', endpoint_url=s3_endpoint)


@pytest.fixture
def s3_bucket(s3_resource):
    bucket_name = '{:x}'.format(getrandbits(8 * 8))
    bucket = s3_resource.create_bucket(Bucket=bucket_name)
    yield bucket


@pytest.fixture
def make_runner(sstable_dir, s3_resource, s3_bucket):
    def make_runner(**kwargs):
        return TestRunner(data_dirs=[str(sstable_dir)],
                          s3_resource=s3_resource,
                          s3_bucket=s3_bucket.name, **kwargs)

    return make_runner
