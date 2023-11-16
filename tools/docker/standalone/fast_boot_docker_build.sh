#!/bin/bash
MODE_FLAG=$1  # -L | --local | -R | --remote
REMOTE_VERSION_OR_LOCAL_RPM_PATH=$2
LOCAL_LIB_RPM_PATH=$3

REMOTE_VERSION_OR_LOCAL_RPM_NAME=""
LOCAL_LIB_RPM_NAME=""

TMP_INIT_STORE_PY_SCRIPT="init_store_for_fast_start.tmp.py"
ACTUAL_INIT_STORE_PY_SCRIPT="./fast_boot_docker_build_prepare/init_store_for_fast_start.py"

CWD=$(cd `dirname $0`;pwd)
cd "${CWD}"

function local_rpm_build() {
    if [ ! -e ${REMOTE_VERSION_OR_LOCAL_RPM_PATH} ]; then
        echo "local rpm is not exist"
        exit -1
    fi

    if [ ! -e ${LOCAL_LIB_RPM_PATH} ]; then
        echo "local lib rpm is not exist"
        exit -1
    fi

    cp ${REMOTE_VERSION_OR_LOCAL_RPM_PATH} ./fast_boot_docker_build_prepare
    cp ${REMOTE_VERSION_OR_LOCAL_RPM_PATH} .
    cp ${LOCAL_LIB_RPM_PATH} ./fast_boot_docker_build_prepare
    cp ${LOCAL_LIB_RPM_PATH} .
    REMOTE_VERSION_OR_LOCAL_RPM_NAME=$(basename ${REMOTE_VERSION_OR_LOCAL_RPM_PATH})
    LOCAL_LIB_RPM_NAME=$(basename ${LOCAL_LIB_RPM_PATH})
    
    cd fast_boot_docker_build_prepare && \
    docker build --build-arg LOCAL_RPM="${REMOTE_VERSION_OR_LOCAL_RPM_NAME}" --build-arg LOCAL_LIB_RPM="${LOCAL_LIB_RPM_NAME}" -t raw_observer .
    if [ $? == 0 ]; then
        echo "================== build prepare docker ok ==============="
    else
        echo "================== build prepare docker failed ==============="
        exit -1
    fi

    cd "${CWD}" && mkdir -p ${CWD}/boot/etc
    docker run -it -v ${CWD}/boot:/root/dest raw_observer
    if [ $? == 0 ]; then
        echo "================== prepare docker run ok ==============="
    else
        echo "================== prepare docker run failed ==============="
        rm -rf ${CWD}/boot/etc
        rm -rf ${CWD}/boot/store.tar.gz
        exit -1
    fi

    cd "${CWD}"
    docker build --build-arg LOCAL_RPM="${REMOTE_VERSION_OR_LOCAL_RPM_NAME}" --build-arg LOCAL_LIB_RPM="${LOCAL_LIB_RPM_NAME}" -t observer .
    if [ $? == 0 ]; then
        echo "================== fast boot docker build ok ==============="
    else
        echo "================== fast boot docker build failed ==============="
        exit -1
    fi
}

function remote_rpm_build() {
    cd fast_boot_docker_build_prepare && \
    docker build --build-arg VERSION="${REMOTE_VERSION_OR_LOCAL_RPM_NAME}" -t raw_observer .
    if [ $? == 0 ]; then
        echo "================== build prepare docker ok ==============="
    else
        echo "================== build prepare docker failed ==============="
        exit -1
    fi

    cd "${CWD}" && mkdir -p ${CWD}/boot/etc
    docker run -it -v ${CWD}/boot:/root/dest raw_observer
    if [ $? == 0 ]; then
        echo "================== prepare docker run ok ==============="
    else
        echo "================== prepare docker run failed ==============="
        rm -rf ${CWD}/boot/etc
        rm -rf ${CWD}/boot/store.tar.gz
        exit -1
    fi

    cd "${CWD}"
    docker build --build-arg VERSION="${REMOTE_VERSION_OR_LOCAL_RPM_NAME}" -t observer .
    if [ $? == 0 ]; then
        echo "================== fast boot docker build ok ==============="
    else
        echo "================== fast boot docker build failed ==============="
        exit -1
    fi
}

source ./boot/_env
if [ "x${MODE}" != "xSTANDALONE" ]; then
    echo "please set MODE to STANDALONE for building fast boot docker"
    exit -1
fi
OS=`uname`
cp ${TMP_INIT_STORE_PY_SCRIPT} ${ACTUAL_INIT_STORE_PY_SCRIPT}
if [ "$OS" == 'Darwin' ]; then
    sed -i '' -e "s/@OB_MYSQL_PORT@/${OB_MYSQL_PORT}/g" ${ACTUAL_INIT_STORE_PY_SCRIPT}
    sed -i '' -e "s/@OB_RPC_PORT@/${OB_RPC_PORT}/g" ${ACTUAL_INIT_STORE_PY_SCRIPT}
    sed -i '' -e "s/@OB_TENANT_NAME@/${OB_TENANT_NAME}/g" ${ACTUAL_INIT_STORE_PY_SCRIPT}
else
    sed -i'' -e "s/@OB_MYSQL_PORT@/${OB_MYSQL_PORT}/g" ${ACTUAL_INIT_STORE_PY_SCRIPT}
    sed -i'' -e "s/@OB_RPC_PORT@/${OB_RPC_PORT}/g" ${ACTUAL_INIT_STORE_PY_SCRIPT}
    sed -i'' -e "s/@OB_TENANT_NAME@/${OB_TENANT_NAME}/g" ${ACTUAL_INIT_STORE_PY_SCRIPT}
fi

case $MODE_FLAG in
    -L | --local)
    local_rpm_build
    if [ $? != 0 ]; then
        echo "use local rpm build docker failed"
        exit -1
    fi
    ;;
    -R | --remote)
    remote_rpm_build
    if [ $? != 0 ]; then
        echo "use remote rpm build docker failed"
        exit -1
    fi
    ;;
esac