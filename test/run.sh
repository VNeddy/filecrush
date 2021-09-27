#!/bin/bash

set -u
set -o errexit
set -o pipefail

BLINE='beeline -u "'"$BEELINE_CONNECT"'"'

DB_NAME=aarau1
SRC_BASE_DIR=/user/hive/warehouse/$DB_NAME.db
INPUT_BASE_DIR=/user/aarau1/input
OUTPUT_BASE_DIR=/user/aarau1/output

export AVRO_JAR=/opt/cloudera/parcels/CDH/jars/hive-exec-1.1.0-cdh5.7.2.jar
export HADOOP_CLASSPATH=$AVRO_JAR
export LIBJARS=/home/aarau1/filecrush/target/filecrush-2.2.2-SNAPSHOT.jar,$AVRO_JAR

function get_input_table_dir() {
  local input=$1

  local in_tbl=t_input_$input
  local in_dir=$INPUT_BASE_DIR/$in_tbl
  echo $in_dir
}

function get_output_table_dir() {
  local input=$1
  local clone_mode=$2

  local in_dir=$(get_input_table_dir $input)
  local out_tbl=t_output_$input
  local out_dir=$OUTPUT_BASE_DIR/$out_tbl
  if [ "$clone_mode" == "yes" ]; then
    local tbl_out_dir=$out_dir/$in_dir
  else
    local tbl_out_dir=$out_dir
  fi
  echo $tbl_out_dir
}

function prepare_input() {
  local input=$1
  local big_file=$2

  local src_tbl=t_$input
  local dst_tbl=t_input_$input
  local dst_dir=$INPUT_BASE_DIR/$dst_tbl
  hdfs dfs -rm -R -f -skipTrash $dst_dir
  hdfs dfs -mkdir -p $dst_dir
  hdfs dfs -cp $SRC_BASE_DIR/$src_tbl/* $dst_dir/

  local create_big_file_cmd=""
  if [ "$big_file" == "yes" ]; then
    create_big_file_cmd="set mapreduce.job.reduces = 1; insert into table $dst_tbl select * from $dst_tbl sort by 1;"
  fi

  $BLINE <<EOF
use $DB_NAME;
drop table if exists $dst_tbl;
create external table $dst_tbl like $src_tbl location 'hdfs://$dst_dir';
$create_big_file_cmd
EOF
}

function run() {
  local compress=$1
  local max_file_blocks=$2
  local reducers=$3
  local input=$4
  local clone_mode=$5

  local format=${input##*_}

  local in_tbl=t_input_$input
  local in_dir=$INPUT_BASE_DIR/$in_tbl
  local out_tbl=t_output_$input
  local out_dir=$OUTPUT_BASE_DIR/$out_tbl
  if [ "$clone_mode" == "yes" ]; then
    local tbl_out_dir=$out_dir/$in_dir
  else
    local tbl_out_dir=$out_dir
  fi

  local clone_option=""
  if [ "$clone_mode" = "yes" ]; then
    clone_option="--clone"
  fi

  hdfs dfs -rm -f -R -skipTrash $out_dir .staging "crush*" test tmp input/crushed* 2>/dev/null
  hdfs dfs -mkdir -p $out_dir $tbl_out_dir

  hadoop jar ./target/filecrush-2.2.2-SNAPSHOT.jar com.m6d.filecrush.crush.Crush \
    -Dmapreduce.reduce.maxattempts=1 \
    -Dmapreduce.job.reduces=$reducers \
    -libjars $LIBJARS \
    $in_dir $out_dir 20161016000000 \
    --compress $compress \
    --max-file-blocks $max_file_blocks \
    --input-format $format \
    --output-format $format \
    --threshold 0.007 \
    --verbose \
    $clone_option \
    2>&1 | tee job.log &&
    $BLINE <<EOF

  #    \
  #    --regex '.*/input2\b.*' \
  #    --replacement 'crushed_file-${crush.timestamp}-${crush.task.num}-${crush.file.num}' \
  #    \
  #    --regex '.*/input2\b.*' \
  #    --replacement 'crushed_file-${crush.timestamp}-${crush.task.num}-${crush.file.num}' \
  #    --input-format avro \
  #    --output-format avro \
  #    -libjars $LIBJARS $INPUT_BASE_DIR/input $INPUT_BASE_DIR/output 20161016000000 \
use $DB_NAME;
drop table if exists $out_tbl;
create external table $out_tbl like $in_tbl location 'hdfs://$tbl_out_dir';
EOF

}

if [ $# == 0 ]; then
  echo "Syntax: $0 <compression> <max_file_blocks> <reducers> <input>"
  echo "Syntax: $0 prepare <input> <big_file? yes/no>"
  exit 1
elif [ "$1" == "prepare" ]; then
  prepare_input $2 $3
elif [ "$1" == "get_in_dir" ]; then
  get_input_table_dir $2
elif [ "$1" == "get_out_dir" ]; then
  get_output_table_dir $2 $3
else
  run $1 $2 $3 $4 $5
fi
