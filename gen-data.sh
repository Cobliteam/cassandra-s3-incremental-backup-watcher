#!/bin/bash

set -e

dest_dir=$(mktemp -d)
trap "rm -rf '$dest_dir'" EXIT

keyspaces=(a b c)
tables=(t1 t2)
num_sstables=3

prepare_table_dirs() {
  for table in "${tables[@]}"; do
    table_dir="$table-$(openssl rand -hex 16)"
    for keyspace in "${keyspaces[@]}"; do
      mkdir -p "$dest_dir/$keyspace/$table_dir" || return $?
    done

    echo "$table_dir"
  done
}

produce_sstable() {
  local keyspace="$1" table_dir="$2" num="$3"
  local file_type file_name
  for file_type in CompressionInfo.db Data.db Digest.crc32 Filter.db Index.db Statistics.db Summary.db TOC.txt; do
    file_name="$dest_dir/$keyspace/$table_dir/mc-$num-big-$file_type"
    touch "$file_name" || return $?
  done
}

produce_sstables() {
  for keyspace in "${keyspaces[@]}"; do
    for table_dir in "${table_dirs[@]}"; do
      for num in $(seq "$num_sstables"); do
        produce_sstable "$keyspace" "$table_dir" "$num" || return $?
      done
    done
  done
}


mapfile -t table_dirs < <(prepare_table_dirs)
cassandra-s3-incremental-backup-watcher "$@" "$dest_dir" &

while true; do
  sleep 30
  produce_sstables
done
