#!/bin/bash

if [ "$#" -gt 1 ]; then
    exit 1
elif [ "$#" -eq 1 ]; then
    BUILD_ARG="--build-arg VERSION=$1"
else
    BUILD_ARG=""
fi

TMP_INIT_STORE_PY_SCRIPT="init_store_for_fast_start.tmp.py"
ACTUAL_INIT_STORE_PY_SCRIPT="init_store_for_fast_start.py"

CWD=$(cd `dirname $0`;pwd)
cd "${CWD}"

function fast_boot_docker_build() {
    rm -rf boot
    cp -r step_1_boot boot
    docker build --no-cache $BUILD_ARG --build-arg STEP=1 -t raw_observer .
    if [ $? == 0 ]; then
        echo "================== build prepare docker ok ==============="
    else
        echo "================== build prepare docker failed ==============="
        exit -1
    fi
    rm -rf boot

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
    cp -r step_2_boot/* boot
    docker build --no-cache $BUILD_ARG --build-arg STEP=2 -t oceanbase-ce .
    if [ $? == 0 ]; then
        echo "================== docker build ok ==============="
    else
        echo "================== docker build failed ==============="
        exit -1
    fi
}

source ./step_2_boot/_env
OS=`uname`
cp ${TMP_INIT_STORE_PY_SCRIPT} ${ACTUAL_INIT_STORE_PY_SCRIPT}

if [ "$OS" == 'Darwin' ]; then
    sed -i '' -e "s/@OB_SERVER_IP@/${OB_SERVER_IP}/g" ${ACTUAL_INIT_STORE_PY_SCRIPT}
    sed -i '' -e "s/@OB_MYSQL_PORT@/${OB_MYSQL_PORT}/g" ${ACTUAL_INIT_STORE_PY_SCRIPT}
    sed -i '' -e "s/@OB_RPC_PORT@/${OB_RPC_PORT}/g" ${ACTUAL_INIT_STORE_PY_SCRIPT}
    sed -i '' -e "s/@OB_TENANT_NAME@/${OB_TENANT_NAME}/g" ${ACTUAL_INIT_STORE_PY_SCRIPT}
    sed -i '' -e "s/@OB_TENANT_LOWER_CASE_TABLE_NAMES@/${OB_TENANT_LOWER_CASE_TABLE_NAMES}/g" ${ACTUAL_INIT_STORE_PY_SCRIPT}
else
    sed -i'' -e "s/@OB_SERVER_IP@/${OB_SERVER_IP}/g" ${ACTUAL_INIT_STORE_PY_SCRIPT}
    sed -i'' -e "s/@OB_MYSQL_PORT@/${OB_MYSQL_PORT}/g" ${ACTUAL_INIT_STORE_PY_SCRIPT}
    sed -i'' -e "s/@OB_RPC_PORT@/${OB_RPC_PORT}/g" ${ACTUAL_INIT_STORE_PY_SCRIPT}
    sed -i'' -e "s/@OB_TENANT_NAME@/${OB_TENANT_NAME}/g" ${ACTUAL_INIT_STORE_PY_SCRIPT}
    sed -i'' -e "s/@OB_TENANT_LOWER_CASE_TABLE_NAMES@/${OB_TENANT_LOWER_CASE_TABLE_NAMES}/g" ${ACTUAL_INIT_STORE_PY_SCRIPT}
fi

fast_boot_docker_build
if [ $? != 0 ]; then
    echo "use local rpm build docker failed"
    exit -1
fi
