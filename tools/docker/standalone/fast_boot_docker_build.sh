#!/bin/bash
RPM_VERSION=$1

TMP_INIT_STORE_PY_SCRIPT="init_store_for_fast_start.tmp.py"
ACTUAL_INIT_STORE_PY_SCRIPT="./fast_boot_docker_build_prepare/init_store_for_fast_start.py"

CWD=$(cd `dirname $0`;pwd)
cd "${CWD}"

function fast_boot_docker_build() {
    cd fast_boot_docker_build_prepare && \
    docker build --build-arg VERSION="${RPM_VERSION}" -t raw_observer .
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
    docker build --build-arg VERSION="${RPM_VERSION}" -t oceanbase-ce .
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

fast_boot_docker_build
if [ $? != 0 ]; then
    echo "use local rpm build docker failed"
    exit -1
fi