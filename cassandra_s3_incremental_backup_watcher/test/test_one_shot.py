import os
from contextlib import ExitStack

import pytest
from moto import mock_s3

from .conftest import SSTableGenerator, TestRunner


@pytest.fixture
def keyspaces():
    return ('ks1', 'ks2')[:1]


@pytest.fixture
def tables():
    return ('tbl1', 'tbl2')[:1]


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


def test_one_shot_default(tmpdir, s3_res, sstables):
    runner = TestRunner(delete=False, dry_run=False, data_dirs=[str(tmpdir)])
    bucket = s3_res.create_bucket(Bucket=runner.s3_bucket)

    runner.run_one_shot()

    for sstable in sstables:
        dest_base_path = sstable.storage_path(runner.s3_path)

        for sstable_file in sstable.files:
            # Check that files have been uploaded
            dest_path = '{}/{}'.format(dest_base_path,
                                       sstable_file.filename)
            obj = bucket.Object(dest_path).get()
            assert obj['ContentLength'] == sstable_file.size

            # Check that files have not been deleted
            src_path = os.path.join(sstable.local_dir,
                                    sstable_file.filename)
            assert os.path.isfile(src_path)


@mock_s3
def test_one_shot_delete(tmpdir, s3_res, sstables):
    runner = TestRunner(delete=True, dry_run=False, data_dirs=[str(tmpdir)])
    bucket = s3_res.create_bucket(Bucket=runner.s3_bucket)

    runner.run_one_shot()

    for sstable in sstables:
        dest_base_path = sstable.storage_path(runner.s3_path)

        for sstable_file in sstable.files:
            # Check that files have been uploaded
            dest_path = '{}/{}'.format(dest_base_path,
                                       sstable_file.filename)
            obj = bucket.Object(dest_path).get()
            assert obj['ContentLength'] == sstable_file.size

            # Check that files have been deleted
            src_path = os.path.join(sstable.local_dir,
                                    sstable_file.filename)
            assert not os.path.exists(src_path)


@mock_s3
def test_one_shot_dry_run(tmpdir, s3_res, sstables):
    runner = TestRunner(delete=True, dry_run=True, data_dirs=[str(tmpdir)])
    bucket = s3_res.create_bucket(Bucket=runner.s3_bucket)

    runner.run_one_shot()

    for sstable in sstables:
        # Check that files have not been uploaded
        objects = list(bucket.objects.all())
        assert not objects

        # Check that files have not been deleted
        for sstable_file in sstable.files:
            src_path = os.path.join(sstable.local_dir,
                                    sstable_file.filename)
            assert os.path.isfile(src_path)
