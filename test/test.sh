#!/bin/bash

set -u
set -e

BLINE='beeline -u "'"$BEELINE_CONNECT"'"'

function data_digest() {
  local table_name=$1

  $BLINE --silent=true --showHeader=false --outputformat=tsv2 --incremental=true -f <(
    cat <<EOF
use aarau1;
set hive.mapred.mode = nonstrict;
select * from $table_name;
EOF
  ) | sort | sha1sum | awk '{print $1}'
}

function number_of_files() {
  local table_name=$1

  $BLINE --silent=true --showHeader=false --outputformat=tsv2 --incremental=true -f <(
    cat <<EOF
describe formatted aarau1.$table_name;
EOF
  ) | grep Location: | awk '{print $2}' | xargs hdfs dfs -ls | grep ^- | awk '{sum+=$5}END{print NR":"sum}'
}

function test() {
  local compress=$1
  local max_file_blocks=$2
  local reducers=$3
  local input=$4
  local source_digest=$5
  local clone_mode=$6
  local big_files=$7

  echo "Running test [$compress $max_file_blocks $reducers $input clone=$clone_mode bigfiles=$big_files]"
  ./run.sh $compress $max_file_blocks $reducers $input $clone_mode
  if [ $? == 0 ]; then
    if [ "$big_files" == "yes" ]; then
      # Checks that the file exists at the right location and deletes it so it doesn't affect the comparison of the crushed data
      if [ "$clone_mode" == "yes" ]; then
        bf_dir=$(./run.sh get_in_dir $input)
        other_dir=$(./run.sh get_out_dir $input $clone_mode)
      else
        bf_dir=$(./run.sh get_out_dir $input $clone_mode)
        other_dir=$(./run.sh get_in_dir $input)
      fi
      set +e
      hdfs dfs -stat $bf_dir/000000_0_copy_1 >/dev/null 2>&1
      local ret=$?
      hdfs dfs -stat $other_dir/000000_0_copy_1 >/dev/null 2>&1
      local other_ret=$?
      set -e

      # Delete the big file and recalculate the diget
      hdfs dfs -rm -f -skipTrash $bf_dir/000000_0_copy_1 $other_dir/000000_0_copy_1
      source_digest=$(data_digest t_input_$INPUT)
      if [ $ret == 0 -a $other_ret != 0 ]; then
        local bf_status="BF_OK"
      else
        local bf_status="BF_ERROR"
      fi
    else
      local bf_status="-"
    fi
    local target_digest=$(data_digest t_output_$input)
    if [ "$source_digest" == "$target_digest" ]; then
      local status=OK
    else
      local status=MISMATCH
    fi
  else
    local status=FAILED
  fi
  printf "TEST RESULTS: %-10s %-10s %15s %15s %s\n" "status" "bf_status" "input #:size" "output #:size" "parameters"
  printf "TEST RESULTS: %-10s %-10s %15s %15s %s\n" "$status" "$bf_status" $(number_of_files t_input_$input) $(number_of_files t_output_$input) "$compress,$max_file_blocks,$reducers,$input,clone=$clone_mode,bigfiles=$big_files"
}

# Tested: text, sequence, avro, parquet
for BIG_FILES in yes no; do
  for INPUT in text sequence avro parquet; do
    ./run.sh prepare $INPUT $BIG_FILES
    DIRTY_INPUT=no
    SOURCE_DIGEST=$(data_digest t_input_$INPUT)

    # backup input dir
    BKP_DIR=tmpbkp/test_backup_dir.$$
    INPUT_DIR=$(./run.sh get_in_dir $INPUT)
    hdfs dfs -rm -R -f -skipTrash $BKP_DIR
    hdfs dfs -mkdir -p $BKP_DIR
    hdfs dfs -cp "$INPUT_DIR/*" $BKP_DIR/

    for CLONE_MODE in yes no; do
      for COMPRESS in none snappy gzip bzip2 deflate; do
        #for COMPRESS in none; do
        if [ \( "$INPUT" != "avro" -a "$INPUT" != "parquet" \) \
          -o \( "$INPUT" == "avro" -a "$COMPRESS" != "gzip" \) \
          -o \( "$INPUT" == "parquet" -a "$COMPRESS" != "bzip2" -a "$COMPRESS" != "deflate" \) ]; then
          # Reset input if needed
          if [ "$DIRTY_INPUT" == "yes" ]; then
            hdfs dfs -rm -R -f -skipTrash $INPUT_DIR
            hdfs dfs -mkdir -p $INPUT_DIR
            hdfs dfs -cp "$BKP_DIR/*" $INPUT_DIR/
            DIRTY_INPUT=no
          fi
          test $COMPRESS 1 3 $INPUT $SOURCE_DIGEST $CLONE_MODE $BIG_FILES
          if [ "$BIG_FILES" == "yes" -o "$CLONE_MODE" == "yes" ]; then
            DIRTY_INPUT=yes
          fi
        fi
      done
    done
  done
done 2>&1 | tee test_$(date +%Y%m%d%H%M%S).log
