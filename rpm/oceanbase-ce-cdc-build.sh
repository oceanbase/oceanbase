#!/bin/bash

PROJECT_DIR=$1
PROJECT_NAME=$2
VERSION=$3
RELEASE=$4

CURDIR=$PWD
TOP_DIR=`pwd`/../

OB_DISABLE_LSE_OPTION=""
[[ $OB_DISABLE_LSE == "1" ]] && OB_DISABLE_LSE_OPTION="-DOB_DISABLE_LSE=ON"

echo "[BUILD] args: TOP_DIR=${TOP_DIR} PROJECT_NAME=${PROJECT_NAME} VERSION=${VERSION} RELEASE=${RELEASE} ${OB_DISABLE_LSE_OPTION}"

cd ${TOP_DIR}
./tools/upgrade/gen_obcdc_compatiable_info.py
./build.sh clean
./build.sh                  \
    rpm                     \
    -DOB_BUILD_CDC=ON       \
    -DOB_RELEASEID=$RELEASE \
    -DBUILD_NUMBER=$RELEASE \
    ${OB_DISABLE_LSE_OPTION}\
    --init                  \
    --make rpm || exit 1

cd ${TOP_DIR}/build_rpm
mv *cdc*.rpm $CURDIR || exit 2

