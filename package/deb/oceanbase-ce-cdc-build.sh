#!/bin/bash

PROJECT_DIR=$1
PROJECT_NAME=$2
VERSION=$3
RELEASE=$4

CURDIR=$PWD
TOP_DIR=`pwd`/../../

echo "[BUILD] args: TOP_DIR=${TOP_DIR} PROJECT_NAME=${PROJECT_NAME} VERSION=${VERSION} RELEASE=${RELEASE}"

cd ${TOP_DIR}
./tools/upgrade/gen_obcdc_compatiable_info.py
./build.sh clean
./build.sh                  \
    deb                     \
    -DBUILD_CDC_ONLY=ON     \
    -DOB_RELEASEID=$RELEASE \
    -DBUILD_NUMBER=$RELEASE \
    --init                  \
    --make deb || exit 1

cd ${TOP_DIR}/build_deb
mv *cdc*.deb *cdc*.ddeb $CURDIR || exit 2
