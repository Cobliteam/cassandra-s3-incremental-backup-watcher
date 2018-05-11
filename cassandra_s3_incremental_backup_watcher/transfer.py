from __future__ import absolute_import, unicode_literals

import logging
import os.path
import sys
from collections import defaultdict, namedtuple
from concurrent.futures import CancelledError, Future

from s3transfer.manager import TransferConfig, \
                               TransferManager as S3TransferManager
from s3transfer.subscribers import BaseSubscriber

logger = logging.getLogger(__name__)

SSTableTransfer = namedtuple('SSTableTransfer',
                             'sstable bucket dest_paths total_size')


class TransferManager(BaseSubscriber):
    def __init__(self, s3_client, s3_bucket, s3_path, s3_settings,
                 delete=False, dry_run=True, s3_config=None):
        self.s3_client = s3_client
        self.s3_bucket = s3_bucket
        self.s3_path = s3_path
        self.s3_settings = s3_settings
        self.s3_config = s3_config and TransferConfig(**s3_config)
        self.delete = delete
        self.dry_run = dry_run

        self._pending_transfers = defaultdict(set)
        self._failed_transfers = defaultdict(set)
        self._closed = False
        self._finished = Future()

    def _list_objects(self, Prefix=None, **kwargs):
        if Prefix is None:
            Prefix = self.s3_path
        else:
            Prefix = '{}/{}'.format(self.s3_path, Prefix)

        paginate = self.s3_client.get_paginator('list_objects_v2').paginate
        return paginate(Bucket=self.s3_bucket, Prefix=Prefix, **kwargs)

    def _sstable_paths(self, sstable):
        storage_path = sstable.storage_path(self.s3_path)

        for sstable_file in sstable.files:
            src_path = os.path.join(sstable.local_dir,
                                    sstable_file.filename)
            dest_path = storage_path + '/' + sstable_file.filename
            yield sstable_file, src_path, dest_path

    def _prepare_upload(self, sstable, src_path, dest_path):
        self._pending_transfers[sstable].add(dest_path)

    def _upload(self, sstable, src_path, dest_path):
        logger.debug('Starting transfer: %s => %s', src_path, dest_path)

        transfer = self._s3_manager.upload(
            src_path, self.s3_bucket, dest_path, extra_args=self.s3_settings,
            subscribers=[self])
        transfer.meta.user_context['sstable'] = sstable
        return transfer

    def _finish_upload(self, sstable, dest_path, failed=False):
        if self.dry_run:
            print('Sent (dry-run): {}'.format(dest_path))
        else:
            print('Sent: {}'.format(dest_path))

        sstable_pending = self._pending_transfers[sstable]
        sstable_failed = self._failed_transfers[sstable]

        sstable_pending.remove(dest_path)
        if failed:
            sstable_failed.add(dest_path)

        if not sstable_pending:
            logger.debug('Finished uploading SSTable: %s', sstable)

            if not sstable_failed:
                sstable.delete_files(dry_run=self.dry_run)

            del self._pending_transfers[sstable]

        if self._closed and not self._pending_transfers:
            self._finished.set_result(None)

    def schedule(self, sstables, check_remote=True):
        if self._closed:
            raise RuntimeError('Scheduling is closed')

        existing_objects = {}
        if check_remote:
            for response in self._list_objects():
                for item in response.get('Contents', []):
                    existing_objects[item['Key']] = item['Size']

        # Aggregate all the transfers that will be made to ensure a transfer
        # starting and ending quickly won't falsely trigger the finishing
        # logic by seemingly being the only one.
        for sstable in sstables:
            for _, src_path, dest_path in self._sstable_paths(sstable):
                self._prepare_upload(sstable, src_path, dest_path)

        # Start actual uploads
        for sstable in sstables:
            for sstable_file, src_path, dest_path in \
                    self._sstable_paths(sstable):
                # This will always be None if check_remote is disabled.
                existing_size = existing_objects.get(dest_path)
                if existing_size == sstable_file.size:
                    logger.debug('Skipping matching remote SSTable: %s',
                                 dest_path)
                    self._finish_upload(sstable, dest_path)
                    continue

                self._upload(sstable, src_path, dest_path)

    def on_done(self, future, **kwargs):
        sstable = future.meta.user_context['sstable']
        dest_path = future.meta.call_args.key

        try:
            future.result()
        except (KeyboardInterrupt, CancelledError):
            sys.stdout.write('Cancelled: {}\n'.format(dest_path))
            self._finish_upload(sstable, dest_path, failed=True)
            self.shutdown(cancel=True)
        except Exception as e:
            sys.stdout.write('Failed: {}: {}\n'.format(dest_path, e))
            self._finish_upload(sstable, dest_path, failed=True)
        else:
            self._finish_upload(sstable, dest_path)

    def close(self):
        self._closed = True

        if not self._pending_transfers and not self._finished.done():
            self._finished.set_result(None)

    def join(self):
        try:
            self._finished.result()
        except CancelledError:
            self._finished.cancel()
            raise

    def shutdown(self, cancel=False):
        self._closed = True

        if not self._finished.done():
            self._finished.cancel()

        if self._s3_manager:
            self._s3_manager.shutdown(cancel=cancel)
            self._s3_manager = None

    def __enter__(self):
        self._s3_manager = S3TransferManager(self.s3_client,
                                             config=self.s3_config)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        cancel = exc_val is not None
        self.shutdown(cancel=cancel)
