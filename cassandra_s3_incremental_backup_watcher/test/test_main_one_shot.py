import pytest

from .sstable import check_sstable_files_deleted, check_sstable_files_exist, \
                     check_sstable_sent


@pytest.mark.integration
def test_one_shot_default(sstables, s3_bucket, make_runner):
    with make_runner(delete=False, dry_run=False) as runner:
        runner.start_one_shot()
        runner.join()

    for sstable in sstables:
        check_sstable_sent(sstable, s3_bucket, runner.s3_path)
        check_sstable_files_exist(sstable)


@pytest.mark.integration
def test_one_shot_delete(sstables, s3_bucket, make_runner):
    with make_runner(delete=True, dry_run=False) as runner:
        runner.start_one_shot()
        runner.join()

    for sstable in sstables:
        check_sstable_sent(sstable, s3_bucket, runner.s3_path)
        check_sstable_files_deleted(sstable)


@pytest.mark.integration
def test_one_shot_dry_run(sstables, s3_bucket, make_runner):
    with make_runner(delete=False, dry_run=True) as runner:
        runner.start_one_shot()
        runner.join()

    for sstable in sstables:
        # Check that files have not been uploaded
        objects = list(s3_bucket.objects.all())
        assert not objects
