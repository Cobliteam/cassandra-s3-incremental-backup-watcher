from __future__ import absolute_import, unicode_literals

import logging
import re

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, LoggingEventHandler

from .sstable import find_sstable_from_data


KEYSPACE_REGEX = r'([a-z0-9_]{,48})'
TABLE_REGEX = r'([a-z][a-z0-9_]*)-[0-9a-f]{32}'
SSTABLE_DATA_REGEX = r'{}/{}/backups/.+-Data\.db'.format(
    KEYSPACE_REGEX, TABLE_REGEX)

logger = logging.getLogger(__name__)


class Watcher(FileSystemEventHandler):
    def __init__(self, transfer_manager, data_dirs, keyspace_filter,
                 table_filter):
        super(Watcher, self).__init__()

        self.transfer_manager = transfer_manager
        self.data_dirs = data_dirs
        self.keyspace_filter = keyspace_filter or (lambda _: True)
        self.table_filter = table_filter or (lambda _: True)

        self._observer = None

    def _on_new_file(self, src_path):
        match = re.search(SSTABLE_DATA_REGEX, src_path, re.I)
        if not match:
            return

        keyspace, table = match.group(1, 2)

        if not self.keyspace_filter(keyspace):
            logger.info('Ignoring new SSTable %s, as the keyspace  %s is '
                        'filtered', src_path, keyspace)
            return

        if not self.table_filter(table):
            logger.info('Ignoring new SSTable %s, as the table %s is filtered',
                        src_path, table)
            return

        sstable = find_sstable_from_data(src_path, keyspace, table)

        logger.debug('Scheduling processing of SStable: %s', sstable)
        self.transfer_manager.schedule([sstable], check_remote=False)

    def on_created(self, event):
        if not event.is_directory:
            self._on_new_file(event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            self._on_new_file(event.dest_path)

    def __enter__(self):
        self._observer = Observer()

        for data_dir in self.data_dirs:
            logger.debug('Watching directory: %s', data_dir)
            self._observer.schedule(self, data_dir, recursive=True)

        logger.debug('Starting Observer')
        self._observer.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._observer:
            try:
                if exc_val:
                    self._observer.stop()
            finally:
                self._observer.join(timeout=30)
                self._observer = None
