#!/bin/bash

PROJECT_DIR=$1
PROJECT_NAME=$2
VERSION=$3
RELEASE=$4

CURDIR=$PWD
TOP_DIR=`pwd`/../

OB_DISABLE_LSE_OPTION=""
[[ $OB_DISABLE_LSE == "1" ]] && OB_DISABLE_LSE_OPTION="-DOB_DISABLE_LSE=ON"

# Open-source builds only accept the libobcdc component.
if [[ -z "${OB_CDC_BUILD_COMPONENTS+x}" ]]; then
    CDC_BUILD_COMPONENTS="libobcdc"
elif [[ -z "${OB_CDC_BUILD_COMPONENTS}" ]]; then
    echo "[BUILD] error: OB_CDC_BUILD_COMPONENTS is set but empty" >&2
    exit 3
else
    CDC_BUILD_COMPONENTS="${OB_CDC_BUILD_COMPONENTS}"
fi

if [[ ",${CDC_BUILD_COMPONENTS}," == *",,"* ]]; then
    echo "[BUILD] error: OB_CDC_BUILD_COMPONENTS contains an empty component" >&2
    exit 3
fi

BUILD_LIBOBCDC=OFF
BUILD_CDC_SERVICE=OFF
IFS=',' read -ra CDC_BUILD_COMPONENT_LIST <<< "${CDC_BUILD_COMPONENTS}"
for component in "${CDC_BUILD_COMPONENT_LIST[@]}"; do
    case "${component}" in
        libobcdc)
            BUILD_LIBOBCDC=ON
            ;;
        cdc_service)
            BUILD_CDC_SERVICE=ON
            ;;
        *)
            echo "[BUILD] error: unsupported CDC component '${component}', expected libobcdc" >&2
            exit 3
            ;;
    esac
done

if [[ "${BUILD_LIBOBCDC}" == "OFF" && "${BUILD_CDC_SERVICE}" == "OFF" ]]; then
    echo "[BUILD] error: no CDC component selected" >&2
    exit 3
fi

if [[ "${BUILD_CDC_SERVICE}" == "ON" ]]; then
    echo "[BUILD] error: cdc_service is unavailable in open-source builds" >&2
    exit 3
fi

echo "[BUILD] args: TOP_DIR=${TOP_DIR} PROJECT_NAME=${PROJECT_NAME} VERSION=${VERSION} RELEASE=${RELEASE} ${OB_DISABLE_LSE_OPTION}"
echo "[BUILD] CDC components: libobcdc=${BUILD_LIBOBCDC} cdc_service=${BUILD_CDC_SERVICE}"

cd ${TOP_DIR}
./tools/upgrade/gen_obcdc_compatiable_info.py
./build.sh clean
./build.sh                  \
    rpm                     \
    -DBUILD_CDC_ONLY=ON     \
    -DENABLE_CDC_COMPILE_OPTIMIZATIONS=ON \
    -DBUILD_LIBOBCDC=${BUILD_LIBOBCDC} \
    -DBUILD_CDC_SERVICE=${BUILD_CDC_SERVICE} \
    -DOB_RELEASEID=$RELEASE \
    -DBUILD_NUMBER=$RELEASE \
    ${OB_DISABLE_LSE_OPTION}\
    --init                  \
    --make rpm || exit 1

cd ${TOP_DIR}/build_rpm
mv *cdc*.rpm $CURDIR || exit 2
