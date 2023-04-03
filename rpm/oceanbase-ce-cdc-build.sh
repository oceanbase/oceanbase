#!/bin/bash

PROJECT_DIR=$1
PROJECT_NAME=$2
VERSION=$3
RELEASE=$4

CURDIR=$PWD
TOP_DIR=`pwd`/../

echo "[BUILD] args: TOP_DIR=${TOP_DIR} PROJECT_NAME=${PROJECT_NAME} VERSION=${VERSION} RELEASE=${RELEASE}"

cd ${TOP_DIR}
./build.sh clean
./build.sh                  \
    rpm                     \
    -DOB_BUILD_CDC=ON       \
    -DOB_RELEASEID=$RELEASE \
    -DBUILD_NUMBER=$RELEASE \
    --init                  \
    --make rpm || exit 1

cd ${TOP_DIR}/build_rpm
mv *cdc*.rpm $CURDIR || exit 2

