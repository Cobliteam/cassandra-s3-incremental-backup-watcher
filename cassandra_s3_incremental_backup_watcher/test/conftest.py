import logging
import os
import shutil
import subprocess
import tempfile
from random import getrandbits

import boto3
import pytest

from . import TestRunner, wait_for_port
from .sstable import MultiSSTableGenerator, SSTableGenerator


logger = logging.getLogger(__name__)


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
def sstable_gen(sstable_dir, keyspaces, tables):
    gens = [SSTableGenerator(str(sstable_dir), keyspace, table, size=100)
            for keyspace in keyspaces
            for table in tables]

    multi_gen = MultiSSTableGenerator(gens, rounds=5, interval=0.5,
                                      sstables_per_round=1)
    with multi_gen:
        yield multi_gen


@pytest.fixture
def sstables(sstable_gen):
    sstable_gen.generate(5)
    yield sstable_gen.sstables


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
    bucket_name = '{:016x}'.format(getrandbits(8 * 8))
    bucket = s3_resource.create_bucket(Bucket=bucket_name)
    yield bucket


@pytest.fixture
def make_runner(sstable_dir, s3_resource, s3_bucket):
    def make_runner(**kwargs):
        return TestRunner(data_dirs=[str(sstable_dir)],
                          s3_resource=s3_resource,
                          s3_bucket=s3_bucket.name, **kwargs)

    return make_runner
