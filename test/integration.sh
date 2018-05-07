#!/bin/bash

dest_dir=$(mktemp -d)
trap "rm -rf '$dest_dir'" EXIT

keyspaces=(a b c)
tables=(t1 t2)
sstables_per_run=3

prepare_table_dirs() {
  for table in "${tables[@]}"; do
    table_dir="$table-$(openssl rand -hex 16)"
    for keyspace in "${keyspaces[@]}"; do
      mkdir -p "$dest_dir/$keyspace/$table_dir/backups"
    done

    echo "$table_dir"
  done
}

produce_sstable() {
  local keyspace="$1" table_dir="$2" num="$3"
  local file_type file_name
  for file_type in CompressionInfo.db Data.db Digest.crc32 Filter.db Index.db Statistics.db Summary.db TOC.txt; do
    file_name="$dest_dir/$keyspace/$table_dir/backups/mc-$num-big-$file_type"
    fallocate -l 1M "$file_name"
  done
}

declare -A sstable_nums
produce_sstables() {
  local first_sstable last_sstable
  for keyspace in "${keyspaces[@]}"; do
    for table_dir in "${table_dirs[@]}"; do
      first_sstable=$(( sstable_nums[$keyspace.$table_dir] + 1 ))
      last_sstable=$(( first_sstable + sstables_per_run - 1 ))

      for num in $(seq "$first_sstable" "$last_sstable"); do
        produce_sstable "$keyspace" "$table_dir" "$num"
      done

      (( sstable_nums[$keyspace.$table_dir] = last_sstable + 1 ))
    done
  done
}


mapfile -t table_dirs < <(prepare_table_dirs)

produce_sstables

aws s3 rm --recursive s3://cobli-backups/cassandra/test

cassandra-s3-incremental-backup-watcher "$@" --one-shot --delete "$dest_dir"

cassandra-s3-incremental-backup-watcher "$@" "$dest_dir" &
trap 'pkill -P $$' EXIT

while true; do
  sleep 30
  produce_sstables
done
