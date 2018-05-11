import os

import pytest


@pytest.mark.integration
def test_one_shot_default(sstables, s3_bucket, make_runner):
    runner = make_runner(delete=False, dry_run=False)
    runner.run_one_shot()

    for sstable in sstables:
        dest_base_path = sstable.storage_path(runner.s3_path)

        for sstable_file in sstable.files:
            # Check that files have been uploaded
            dest_path = '{}/{}'.format(dest_base_path,
                                       sstable_file.filename)
            obj = s3_bucket.Object(dest_path).get()
            assert obj['ContentLength'] == sstable_file.size

            # Check that files have not been deleted
            src_path = os.path.join(sstable.local_dir,
                                    sstable_file.filename)
            assert os.path.isfile(src_path)


@pytest.mark.integration
def test_one_shot_delete(sstables, s3_bucket, make_runner):
    runner = make_runner(delete=True, dry_run=False)
    runner.run_one_shot()

    for sstable in sstables:
        dest_base_path = sstable.storage_path(runner.s3_path)

        for sstable_file in sstable.files:
            # Check that files have been uploaded
            dest_path = '{}/{}'.format(dest_base_path,
                                       sstable_file.filename)
            obj = s3_bucket.Object(dest_path).get()
            assert obj['ContentLength'] == sstable_file.size

            # Check that files have been deleted
            src_path = os.path.join(sstable.local_dir,
                                    sstable_file.filename)
            assert not os.path.exists(src_path)


@pytest.mark.integration
def test_one_shot_dry_run(sstables, s3_bucket, make_runner):
    runner = make_runner(delete=False, dry_run=True)
    runner.run_one_shot()

    for sstable in sstables:
        # Check that files have not been uploaded
        objects = list(s3_bucket.objects.all())
        assert not objects
