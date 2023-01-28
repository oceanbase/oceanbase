#!/bin/bash
CALL_PATH=`pwd`
PATH_TO_SCRIPT=`readlink -f "$0"`
SCRIPT_DIR=`dirname "$PATH_TO_SCRIPT"`
TOP_DIR=${SCRIPT_DIR}/../..

if [ -d $TOP_DIR/build_debug ]; then
  TEST_BINARY_DIR=$TOP_DIR/build_debug/unittest/data_dictionary
elif [ -d $TOP_DIR/build_release ]; then
  TEST_BINARY_DIR=$TOP_DIR/build_release/unittest/data_dictionary
elif [ -d $TOP_DIR/build_rpm ]; then
  TEST_BINARY_DIR=$TOP_DIR/build_rpm/unittest/data_dictionary
fi

function make_test
{
  cd $TEST_BINARY_DIR &&
  make -j 20 &&
  cd $CALL_PATH
}

function do_test
{
  $TEST_BINARY_DIR/test_schema_to_dict
  $TEST_BINARY_DIR/test_data_dict_struct
  $TEST_BINARY_DIR/test_data_dict_storager_iterator
  $TEST_BINARY_DIR/test_data_dict_meta_info
}

if [ -z $TEST_BINARY_DIR ]; then
  echo "test_binary_dir not found"
else
  make_test
  do_test
fi
