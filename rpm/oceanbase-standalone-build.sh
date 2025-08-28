#!/bin/bash
#for taobao abs
# Usage: oceanbase-standalone-build.sh <oceanbasepath> <package> <version> <release>
# Usage: oceanbase-standalone-build.sh

if [ $# -ne 4 ]
then
    exit 1
else
    CURDIR=$PWD
    TOP_DIR=`pwd`/../
    REDHAT=$(grep -Po '(?<=release )\d' /etc/redhat-release)
    ID=$(grep -Po '(?<=^ID=).*' /etc/os-release | tr -d '"')
    if [[ "${ID}"x == "alinux"x ]]; then
	RELEASE="$4.al8"
    else
        RELEASE="$4.el${REDHAT}"
    fi

    export BUILD_NUMBER=${4}
fi

OB_DISABLE_LSE_OPTION=""
[[ $OB_DISABLE_LSE == "1" ]] && OB_DISABLE_LSE_OPTION="-DOB_DISABLE_LSE=ON"

OB_BUILD_STANDALONE_OPTION=""
[[ $OB_BUILD_STANDALONE == "1" ]] && OB_BUILD_STANDALONE_OPTION="-DOB_BUILD_STANDALONE=ON"

OB_BUILD_TEST_PUBKEY_OPTION=""
[[ $OB_USE_TEST_PUBKEY == "1" ]] && OB_BUILD_TEST_PUBKEY_OPTION="-DOB_USE_TEST_PUBKEY=ON"

echo "[BUILD] args: TOP_DIR=${TOP_DIR} RELEASE=${RELEASE} BUILD_NUMBER=${BUILD_NUMBER} ${OB_DISABLE_LSE_OPTION} ${OB_BUILD_STANDALONE_OPTION} ${OB_BUILD_TEST_PUBKEY_OPTION}"

CPU_CORES=`grep -c ^processor /proc/cpuinfo`

cd ${TOP_DIR}

# comment dep_create to prevent t-abs from involking it twice.
./build.sh clean
./build.sh                               \
    rpm                                  \
    -DCPACK_RPM_PACKAGE_RELEASE=$RELEASE \
    -DBUILD_NUMBER=$BUILD_NUMBER         \
    -DUSE_LTO_CACHE=ON                   \
    ${OB_DISABLE_LSE_OPTION}             \
    ${OB_BUILD_STANDALONE_OPTION}        \
    ${OB_BUILD_TEST_PUBKEY_OPTION}       \
    --init                               \
    --make rpm || exit 1

mv build_rpm/*.rpm $CURDIR || exit 2

