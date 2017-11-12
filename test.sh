#!/bin/sh

set -e

flake8 cassandra_s3_incremental_backup_watcher
pytest --cov=cassandra_s3_incremental_backup_watcher cassandra_s3_incremental_backup_watcher/test
