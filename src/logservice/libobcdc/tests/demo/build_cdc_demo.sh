#!/bin/bash

CLANG_PATH=$1/bin
PROJECT_BUILD_DIR=$2

mkdir -p `pwd`/lib/ &&
libtool --quiet --mode=install cp $PROJECT_BUILD_DIR/src/logservice/libobcdc/src/libobcdc.so `pwd`/lib/ &&
mkdir -p build_dir &&
cd build_dir &&
$CLANG_PATH/clang++  ../obcdc_dlopen.cpp -o cdc_dl -ldl -std=c++11 -fpic &&
./cdc_dl &&
cd ../
