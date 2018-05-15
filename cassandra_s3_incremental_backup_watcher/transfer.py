import logging
import os.path
import sys
from collections import namedtuple
from concurrent.futures import CancelledError
from threading import Event, Lock

from s3transfer.manager import TransferConfig, \
                               TransferManager as S3TransferManager
from s3transfer.subscribers import BaseSubscriber


logger = logging.getLogger(__name__)

SSTableTransfer = namedtuple('SSTableTransfer',
                             'sstable bucket dest_paths total_size')


def common_prefix(a, b):
    i = 0
    for i in range(min(len(a), len(b))):
        if a[i] != b[i]:
            break
    return a[:i + 1]


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

        self._s3_manager = None
        self._pending_transfers = {}
        self._failed_transfers = {}
        self._closed = False
        self._state_lock = Lock()
        self._finished = Event()

    def _list_objects(self, Prefix=None, **kwargs):
        if Prefix is None:
            Prefix = self.s3_path

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
        logger.debug('prepare %s', dest_path)

        with self._state_lock:
            try:
                pending = self._pending_transfers[sstable]
                pending.add(dest_path)
            except KeyError:
                self._pending_transfers[sstable] = set([dest_path])
                self._failed_transfers[sstable] = set()

    def _upload(self, sstable, src_path, dest_path):
        logger.debug('Sending: %s => %s', src_path, dest_path)

        transfer = self._s3_manager.upload(
            src_path, self.s3_bucket, dest_path, extra_args=self.s3_settings,
            subscribers=[self])
        transfer.meta.user_context['sstable'] = sstable
        return transfer

    def _check_finished(self):
        if self._closed and not self._pending_transfers:
            self._finished.set()

    def _finish_sstable(self, sstable):
        logger.debug('Finished uploading SSTable: %s', sstable)

        if self.delete and not self._failed_transfers[sstable]:
            sstable.delete_files(dry_run=self.dry_run)

        del self._pending_transfers[sstable]
        self._check_finished()

    def _finish_upload(self, sstable, dest_path, exception=None,
                       cancelled=False):
        if exception is not None:
            if cancelled:
                sys.stdout.write('Cancelled: {}\n'.format(dest_path))
            else:
                sys.stdout.write(
                    'Failed: {}: {}\n'.format(dest_path, exception))
        elif self.dry_run:
            sys.stdout.write('Sent (dry-run): {}\n'.format(dest_path))
        else:
            sys.stdout.write('Sent: {}\n'.format(dest_path))

        with self._state_lock:
            sstable_pending = self._pending_transfers[sstable]
            sstable_failed = self._failed_transfers[sstable]

            sstable_pending.remove(dest_path)
            if exception is not None:
                sstable_failed.add(dest_path)

            if not sstable_pending:
                self._finish_sstable(sstable)

    def schedule(self, sstables):
        if self._closed:
            raise RuntimeError('Scheduling is closed')

        # Aggregate all the transfers that will be made to ensure a transfer
        # starting and ending quickly won't falsely trigger the finishing
        # logic by seemingly being the only one.
        # Also calculate the common prefix for the destination S3 paths so we
        # can look for existing objects more efficiently.
        prefix = None
        for sstable in sstables:
            for _, src_path, dest_path in self._sstable_paths(sstable):
                if prefix is None:
                    prefix = dest_path
                else:
                    prefix = common_prefix(prefix, dest_path)

                self._prepare_upload(sstable, src_path, dest_path)

        # Find existing objects to avoid repeated uploads
        existing_objects = {}
        for response in self._list_objects(Prefix=prefix):
            for item in response.get('Contents', []):
                existing_objects[item['Key']] = item['Size']

        # Start actual uploads
        for sstable in sstables:
            for sstable_file, src_path, dest_path in \
                    self._sstable_paths(sstable):
                if self.dry_run:
                    self._finish_upload(sstable, dest_path)
                    continue

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
            try:
                future.result()
            except (KeyboardInterrupt, CancelledError) as e:
                self._finish_upload(sstable, dest_path, exception=e,
                                    cancelled=True)
                self.shutdown(cancel=True)
                raise
            except Exception as e:
                self._finish_upload(sstable, dest_path, exception=e)
                raise
            else:
                self._finish_upload(sstable, dest_path)
        except Exception:
            logger.debug('on_done', exc_info=True)

    def close(self):
        self._closed = True
        self._check_finished()

    def join(self, timeout=None):
        self._finished.wait(timeout=timeout)

    def shutdown(self, cancel=False):
        self.close()

        # Call _shutdown directly until the following issue is fixed:
        # https://github.com/boto/s3transfer/pull/105
        self._s3_manager._shutdown(cancel=cancel, cancel_msg='')
        self.join(10)

    def __enter__(self):
        self._s3_manager = S3TransferManager(self.s3_client,
                                             config=self.s3_config)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            cancel = exc_val is not None
            self.shutdown(cancel=cancel)
        finally:
            self._s3_manager = None
