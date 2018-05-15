import time

import pytest

from .sstable import check_sstable_files_deleted, check_sstable_files_exist, \
                     check_sstable_sent


@pytest.mark.integration
def test_watcher_default(sstable_gen, s3_bucket, make_runner):
    sstable_gen.generate(5)

    with make_runner(delete=False, dry_run=False) as runner:
        sstable_gen.start()
        runner.start_watcher()

        sstable_gen.join()
        # Give the watcher time to find the files
        time.sleep(2)

        runner.stop_watcher()
        runner.join()

    for sstable in sstable_gen.sstables:
        check_sstable_sent(sstable, s3_bucket, runner.s3_path)
        check_sstable_files_exist(sstable)


@pytest.mark.integration
def test_watcher_delete(sstable_gen, s3_bucket, make_runner):
    sstable_gen.generate(5)

    with make_runner(delete=True, dry_run=False) as runner:
        sstable_gen.start()
        runner.start_watcher()

        sstable_gen.join()
        # Give the watcher time to find the files
        time.sleep(2)

        runner.stop_watcher()
        runner.join()

    for sstable in sstable_gen.sstables:
        check_sstable_sent(sstable, s3_bucket, runner.s3_path)
        check_sstable_files_deleted(sstable)


@pytest.mark.integration
def test_watcher_dry_run(sstable_gen, s3_bucket, make_runner):
    sstable_gen.generate(5)

    with make_runner(delete=False, dry_run=True) as runner:
        sstable_gen.start()
        runner.start_watcher()

        sstable_gen.join()
        # Give the watcher time to find the files
        time.sleep(2)

        runner.stop_watcher()
        runner.join()

    # Check that no files have been uploaded
    objects = list(s3_bucket.objects.all())
    assert not objects
