#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

if [ $# -lt 3 -o $# -gt 5 ]; then
  echo "Syntax: $0 <db_name> <table_name> <partition_spec> [compression] [threshold] [MAX_FILE_BLOCKS] [max_reduces]"
  exit 1
fi

# Read-only variables
readonly DB_NAME=$1
readonly TABLE_NAME=$2
readonly PART_SPEC=$3
readonly COMPRESSION=${4:-snappy}
readonly THRESHOLD=${5:-0.5}
readonly MAX_FILE_BLOCKS=${6:-1}
readonly MAX_REDUCES=${7:-200}

readonly BLINE='beeline -u "'"$BEELINE_CONNECT"'"'
#readonly IMPALA="impala-shell -i $IMPALA_HOST"

readonly FILECRUSH_DIR=$(readlink -f $(dirname $0)/..)

readonly BACKUP_BASE=filecrush_backup

readonly TMP_FILE=/tmp/crush_partition.tmp
readonly TMP_FILE2=/tmp/crush_partition.tmp2

readonly TIMESTAMP=$(date +%Y%m%d%H%M%S)

# Exported variables
export AVRO_JAR=$(echo /opt/cloudera/parcels/CDH/jars/hive-exec-*.jar)
export HADOOP_CLASSPATH=$AVRO_JAR
export FILECRUSH_JAR=$(echo $FILECRUSH_DIR/target/filecrush-*)
export LIBJARS=$FILECRUSH_JAR,$AVRO_JAR

# Updatable variables
INPUT_DIR=""
INPUT_FORMAT=""

function get_table_details() {
  local db_name=$1
  local table_name=$2
  local partition=$3

  local cmd="describe formatted $db_name.$table_name"
  if [ "$partition" != "all" ]; then
    cmd="$cmd partition ($partition)"
  fi

  $BLINE --outputformat=tsv2 >$TMP_FILE 2>$TMP_FILE2 <<EOF
$cmd;
EOF

  set +e
  grep FAILED: $TMP_FILE2 >/dev/null
  if [ $? == 0 ]; then
    # Command failed
    grep "cannot find field" $TMP_FILE2 >/dev/null
    if [ $? == 0 ]; then
      echo "ERROR: Partition spec ($partition) is invalid." >&2
    else
      grep "Partition not found" $TMP_FILE2 >/dev/null
      if [ $? == 0 ]; then
        echo "ERROR: Partition ($partition) does not exist." >&2
      else
        echo "ERROR: Unknown error:" >&2
        cat $TMP_FILE2 >&2
      fi
    fi
    exit 1
  fi
  set -e

  INPUT_DIR=$(grep Location: $TMP_FILE | awk '{print $2}')
  INPUT_FORMAT=$(get_table_format $(grep InputFormat: $TMP_FILE | awk '{print $2}'))
  rm -f $TMP_FILE
}

function get_input_dir() {
  local db_name=$1
  local table_name=$2
  local partition=$3
  if [ "$INPUT_DIR" == "" ]; then
    get_table_details "$db_name" "$table_name" "$partition"
  fi
  echo "$INPUT_DIR"
}

function get_input_format() {
  local db_name=$1
  local table_name=$2
  local partition=$3
  if [ "$INPUT_FORMAT" == "" ]; then
    get_table_details "$db_name" "$table_name" "$partition"
  fi
  echo "$INPUT_FORMAT"
}

function get_table_format() {
  local input_format=$1

  if [ "$input_format" == "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat" ]; then
    echo "avro"
  elif [ "$input_format" == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat" -o \
    "$input_format" == "parquet.hive.DeprecatedParquetInputFormat" ]; then
    echo "parquet"
  elif [ "$input_format" == "org.apache.hadoop.mapred.SequenceFileInputFormat" ]; then
    echo "sequence"
  elif [ "$input_format" == "org.apache.hadoop.mapred.TextInputFormat" ]; then
    echo "text"
  elif [ "$input_format" == "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat" ]; then
    echo "orc"
  else
    echo "ERROR: Input format $input_format is not supported." >&2
    exit 1
  fi
}

function get_output_dir() {
  local db_name=$1
  local table_name=$2
  local partition=$3
  local sanitized_partition=$(echo "$partition" | sed 's/[^a-zA-Z_=,0-9]//g')
  echo "$BACKUP_BASE/crush_backup_${db_name}_${table_name}_${sanitized_partition}_${TIMESTAMP}"
}

function crush() {
  local db_name=$1
  local table_name=$2
  local partition=$3
  local compress=$4
  local threshold=$5
  local max_tasks=$6
  local max_file_blocks=$7

  local format=$(get_input_format "$db_name" "$table_name" "$partition")
  local input_dir=$(get_input_dir "$db_name" "$table_name" "$partition")

  local sanitized_partition=$(echo "$partition" | sed 's/[^a-zA-Z_=-]//')
  local backup_dir=$(get_output_dir "$db_name" "$table_name" "$partition")
  local log_file="crush_log_${db_name}_${table_name}_${sanitized_partition}_${TIMESTAMP}.log"

  hdfs dfs -mkdir -p $BACKUP_BASE
  hdfs dfs -mkdir $backup_dir

  hadoop jar $FILECRUSH_JAR com.m6d.filecrush.crush.Crush \
    -libjars $LIBJARS \
    $input_dir $backup_dir $TIMESTAMP \
    --clone \
    --remove-empty-files \
    --skip-regex '.*/_.*' \
    --compress $compress \
    --max-file-blocks $max_file_blocks \
    --input-format $format \
    --output-format $format \
    --threshold $threshold \
    --max-tasks $max_tasks \
    --job-name "$(echo -e "FileCrush: $db_name.$table_name\n (Partition: $partition)")" \
    --verbose \
    2>&1 | tee $log_file
}

#function refresh_metadata {
#  local db_name=$1
#  local table_name=$2
#  local partition=$3
#
#  local impala_major_version=$( $IMPALA --version | sed 's/.*cdh\([0-9]*\)\.\([0-9]*\)\..*/\1/' )
#  local impala_minor_version=$( $IMPALA --version | sed 's/.*cdh\([0-9]*\)\.\([0-9]*\)\..*/\2/' )
#
#  set +e
#  $IMPALA --verbose --delimited -q "use $db_name; show tables;" 2> /dev/null | grep "^$table_name$" > /dev/null
#  ret=$?
#  set -e
#
#  if [ $ret == 0 ]; then
#    local cmd="refresh $db_name.$table_name"
#    if [ "$partition" != "all" -a \( "$impala_major_version" -gt "5" -o \( "$impala_major_version" == "5" -a "$impala_minor_version" -ge "8" \) \) ]; then
#      # Refresh partition is only available on CDH 5.8 and later versions
#      cmd="$cmd partition ($partition)"
#    fi
#  else
#    local cmd="invalidate metadata $db_name.$table_name"
#  fi
#
#  echo -e "\n\nRefreshing metadata for table $db_name.$table_name (partition: $partition)\n\n"
#  $IMPALA --verbose -q "$cmd"
#}

function show_stats_header() {
  python -c "
print '%7s %20s %20s %20s %20s' % ('Files', 'Total size (bytes)', 'Median size (bytes)', 'Mean size (bytes)', 'Stddev size (bytes)')
"
}

function show_stats() {
  local dir=$1
  hdfs dfs -ls -R $dir | grep ^- | awk '{print $5}' | python -c "
import numpy as np
import sys
import locale
locale.setlocale(locale.LC_ALL, 'en_US')
def f(n):
  return locale.format('%d', n, grouping=True)

sizes = map(lambda s: int(s), filter(lambda s: s != '', sys.stdin.read().split('\n')))
print '%7s %20s %20s %20s %20s' % (len(sizes), f(np.sum(sizes)), f(np.median(sizes)), f(np.mean(sizes)), f(np.std(sizes)))
"
}

### MAIN ###

echo "$(date) - Gathering stats before compaction"
BEFORE_STATS="$(show_stats $(get_input_dir "$DB_NAME" "$TABLE_NAME" "$PART_SPEC"))"

echo "$(date) - Running the crushing job"
crush "$DB_NAME" "$TABLE_NAME" "$PART_SPEC" "$COMPRESSION" "$THRESHOLD" "$MAX_REDUCES" "$MAX_FILE_BLOCKS"

#echo "$(date) - Refreshing Impala metadata"
#refresh_metadata "$DB_NAME" "$TABLE_NAME" "$PART_SPEC"

echo "$(date) - Gathering final stats"
cat <<EOF


File compaction completed successfully!

Summary:
========
Database name:        $DB_NAME
Table name:           $TABLE_NAME
Partition:            $PART_SPEC
Compression:          $COMPRESSION
Small file threshold: $(printf "%.1f" $(echo 256*$THRESHOLD | bc)) MB
Max reduce tasks:     $MAX_REDUCES

Statistics:
===========
                   $(show_stats_header)
Before compaction: $BEFORE_STATS
After compaction:  $(show_stats $(get_input_dir "$DB_NAME" "$TABLE_NAME" "$PART_SPEC"))

IMPORTANT: PLEASE READ BELOW
============================

You should perform your own check to ensure the compacted data is correct and everything looks good.
The original files that were crushed have been saved to the directory shown below.
AFTER verification, and once you confirm that the data is good, you may delete the backup directory.

Backup directory: hdfs:///user/$USER/$(get_output_dir "$DB_NAME" "$TABLE_NAME" "$PART_SPEC")

EOF

#Backup directory: hdfs:///user/$( klist -l | tail -1 | sed 's/@.*//' )/$( get_output_dir "$DB_NAME" "$TABLE_NAME" "$PART_SPEC" )
#
#EOF
