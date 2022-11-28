#!/bin/bash -x

CLANG_PATH=$1/bin

mkdir -p build_dir &&
cd build_dir
../../copy_obcdc.sh &&
$CLANG_PATH/clang++  ../obcdc_dlopen.cpp -o cdc_dl -ldl -std=c++11 -fpic &&
./cdc_dl &&
cd -
