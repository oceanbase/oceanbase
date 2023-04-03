#!/bin/bash

CLANG_PATH=$1/bin
PROJECT_BUILD_DIR=$2

mkdir -p build_dir/lib &&
cd build_dir &&
libtool --quiet --mode=install cp $PROJECT_BUILD_DIR/src/logservice/libobcdc/src/libobcdc.so `pwd`/lib/

if [ $# -eq 3 ]
then
  DEP_DIR=$3
  libtool --quiet --mode=install cp $DEP_DIR/lib/mariadb/libmariadb.so.3 `pwd`/lib/
fi

$CLANG_PATH/clang++  ../obcdc_dlopen.cpp -o cdc_dl -ldl -std=c++11 -fpic &&
LD_LIBRARY_PATH=./lib ./cdc_dl
