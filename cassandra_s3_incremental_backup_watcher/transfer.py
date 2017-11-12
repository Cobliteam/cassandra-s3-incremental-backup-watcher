from __future__ import absolute_import, unicode_literals

import logging
import os.path
import sys
import time
from functools import partial
from threading import Timer, RLock

logger = logging.getLogger(__name__)


class BandwidthMeter(object):
    def __init__(self, half_life=0.5):
        self.half_life = half_life

        self._last_update = None
        self._average_bandwidth = 0
        self._total_units = 0

    def average_bandwidth(self):
        return self._average_bandwidth

    def total_units(self):
        return self._total_units

    def update(self, units_transmitted, new_time):
        if self._last_update:
            time_diff = max(new_time - self._last_update, 10e-6)

            # Use an exponentially weighted moving average to show upload speed
            # The half-life determines how long until an observation loses half
            # of it's impact in the current average. This approach is copied
            # from Riemann:
            # https://github.com/riemann/riemann/blob/0.2.14/src/riemann/streams.clj#L952

            old_weight = 0.5 ** (time_diff / self.half_life)
            new_weight = 1 - old_weight

            old_avg = self._average_bandwidth
            new_avg = units_transmitted / time_diff
            cur_avg = (old_avg * old_weight) + (new_avg * new_weight)

            self._average_bandwidth = cur_avg

        self._last_update = new_time
        self._total_units += units_transmitted


class SSTableTransfer(object):
    def __init__(self, sstable, bucket, dest_paths, total_size):
        self.sstable = sstable
        self.bucket = bucket
        self.dest_paths = dest_paths
        self.total_size_bytes = total_size

        self.progress_lock = RLock()
        self.progress_timer = None
        self.progress_meter = BandwidthMeter()
        self.cur_filename = None

    def progress_callback(self, filename, bytes_written):
        with self.progress_lock:
            self.progress_meter.update(bytes_written, time.time())

            old_fname, self.cur_filename = self.cur_filename, filename
            if old_fname != filename:
                # Always update the progress if a new transfer has started
                self.print_progress()

    def print_progress(self):
        with self.progress_lock:
            # Cancel the current timer, as we might have been triggered by a
            # forced update instead of the timer.
            if self.progress_timer:
                self.progress_timer.cancel()

            percentage = (self.progress_meter.total_units() * 100.0 /
                          self.total_size_bytes)

            left_part = '{}/{}/{}'.format(
                self.sstable.keyspace, self.sstable.table, self.cur_filename)
            right_part = '{:.02f}% ({:.02f} KiB/s)'.format(
                percentage, self.progress_meter.average_bandwidth() / 1024.0)

            # We could determine the current terminal width, but that would
            # require an external library or some annoying fnctl stuff.
            num_spaces = max(80 - len(left_part) - len(right_part), 1)
            spaces = ' ' * num_spaces

            sys.stderr.write('\r' + left_part + spaces + right_part)
            sys.stderr.flush()

            self.progress_timer = Timer(1.0, self.print_progress)
            self.progress_timer.start()

    def run(self, s3_client, s3_settings, delete=False, dry_run=True):
        try:
            for sstable_file, dest_path in zip(self.sstable.files,
                                               self.dest_paths):
                # Files that are already present in the destination should be
                # skipped (they have a dest_path of None)
                if not dest_path:
                    continue

                full_path = os.path.join(self.sstable.local_dir,
                                         sstable_file.filename)
                full_path = os.path.abspath(full_path)

                if dry_run:
                    verb = 'Uploaded (dry-run)'
                else:
                    verb = 'Uploaded'
                    progress_callback = None

                    if sys.stderr.isatty():
                        progress_callback = partial(self.progress_callback,
                                                    sstable_file.filename)

                    s3_client.upload_file(full_path, self.bucket, dest_path,
                                          ExtraArgs=s3_settings,
                                          Callback=progress_callback)

                    # Break after the last progress line, as we usually erase it
                    # and never print the newline.
                    if sys.stderr.isatty():
                        sys.stderr.write('\n')
                        sys.stderr.flush()

                sys.stdout.write('{} {}\n'.format(verb, dest_path))
                sys.stdout.flush()
        finally:
            with self.progress_lock:
                if self.progress_timer:
                    self.progress_timer.cancel()
                    self.progress_timer = None

        # Delete all files at once after all transfers complete, to avoid
        # keeping partial SSTables
        if delete:
            self.sstable.delete_files(dry_run)


def generate_transfers(s3_client, s3_bucket, s3_path, sstables):
    paginator = s3_client.get_paginator('list_objects_v2')

    # Grabbing all objects, even if the bucket has lots of files, is much
    # faster than doing one roundtrip per-SSTable with a prefix search.
    existing_objects = {}
    for response in paginator.paginate(Bucket=s3_bucket, Prefix=s3_path):
        for item in response.get('Contents', []):
            existing_objects[item['Key']] = item['Size']

    for sstable in sstables:
        storage_path = sstable.storage_path(s3_path)
        dest_paths = []
        total_size = 0

        for sstable_file in sstable.files:
            dest_path = storage_path + '/' + sstable_file.filename
            existing_size = existing_objects.get(dest_path)

            if existing_size == sstable_file.size:
                logger.debug('Skipping matching Remote SSTable: %s', dest_path)
                dest_path = None
            else:
                total_size += sstable_file.size

            dest_paths.append(dest_path)

        yield SSTableTransfer(sstable, s3_bucket, dest_paths, total_size)
