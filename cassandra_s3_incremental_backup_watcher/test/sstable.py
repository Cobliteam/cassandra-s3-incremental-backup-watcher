import logging
import os.path
import shutil
import sys
import time
from contextlib import ExitStack
from random import getrandbits
from threading import RLock, Thread

import pytest
from botocore.exceptions import ClientError
from fallocate import fallocate

from cassandra_s3_incremental_backup_watcher.sstable import SSTable, SSTableFile


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
        return '{:032x}'.format(getrandbits(16 * 8))

    def __init__(self, base_dir, keyspace, table, size=0):
        table_instance = '{}-{}'.format(table, self.table_suffix())

        self.keyspace = keyspace
        self.table = table
        self.size = size
        self.local_dir = os.path.join(
            base_dir, keyspace, table_instance, 'backups')

        self._number = 1

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
        generated = []
        for _ in range(count):
            generated.append(self._generate(self._number))
            self._number += 1

        return generated

    def __enter__(self):
        os.makedirs(self.local_dir + '/', exist_ok=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            shutil.rmtree(self.local_dir)
        except Exception:
            logger.warn('Failed to clean up temporary SSTable dir: %s',
                        self.local_dir, exc_info=True)


class MultiSSTableGenerator(Thread):
    def __init__(self, sstable_gens, rounds=1, interval=1.0,
                 sstables_per_round=1):
        super(MultiSSTableGenerator, self).__init__()

        self.rounds = rounds
        self.interval = interval
        self.sstables_per_round = sstables_per_round

        self._remaining_rounds = rounds
        self._gens = list(sstable_gens)
        self._open_gens = []
        self._sstables = []
        self._state_lock = RLock()

    @property
    def sstables(self):
        with self._state_lock:
            return list(self._sstables)

    def generate(self, count=1):
        logger.warn('Generating SSTables')

        generated = []
        for gen in self._open_gens:
            sstables = gen.generate(count)
            with self._state_lock:
                self._sstables.extend(sstables)

            generated.extend(sstables)

        return generated

    def run(self):
        next_run = time.monotonic() + self.interval

        while self._remaining_rounds > 0:
            while True:
                now = time.monotonic()
                diff = next_run - now
                if diff > 0:
                    time.sleep(diff / 2.0)
                    logger.warn('Waiting %f', diff)
                else:
                    next_run = now + self.interval
                    self.generate(self.sstables_per_round)
                    self._remaining_rounds -= 1
                    break

    def __enter__(self):
        self._exit_stack = ExitStack()
        try:
            while self._gens:
                self._open_gens.append(
                    self._exit_stack.enter_context(self._gens.pop(0)))
        except Exception:
            self.__exit__(*sys.exc_info())
            raise

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._open_gens = []
        self._exit_stack.close()
        self._exit_stack = None


def check_sstable_sent(sstable, s3_bucket, prefix):
    __tracebackhide__ = True

    dest_base_path = sstable.storage_path(prefix)
    for sstable_file in sstable.files:
        # Check that files have been uploaded
        dest_path = '{}/{}'.format(dest_base_path,
                                   sstable_file.filename)
        try:
            obj = s3_bucket.Object(dest_path).get()
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                pytest.fail('Object not present in S3: %s', dest_path)
            else:
                raise

        expected_size = sstable_file.size
        actual_size = obj['ContentLength']
        msg = 'Object has wrong size in S3: {} != {}. Key: {}'.format(
            expected_size, actual_size, dest_path)

        assert expected_size == actual_size, msg


def check_sstable_files_exist(sstable):
    for sstable_file in sstable.files:
        src_path = os.path.join(sstable.local_dir,
                                sstable_file.filename)
        assert os.path.exists(src_path), \
            'File should exist: {}'.format(src_path)


def check_sstable_files_deleted(sstable):
    for sstable_file in sstable.files:
        src_path = os.path.join(sstable.local_dir,
                                sstable_file.filename)
        assert not os.path.exists(src_path), \
            'File should not exist: {}'.format(src_path)
