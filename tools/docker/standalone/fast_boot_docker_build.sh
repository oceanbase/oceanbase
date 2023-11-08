#!/bin/bash
CWD=$(cd `dirname $0`;pwd)
cd "${CWD}"

cd fast_boot_docker_build_prepare && \
docker build --build-arg LOCAL_RPM="oceanbase-ce-4.3.0.0-1.alios7.aarch64.rpm" --build-arg LOCAL_LIB_RPM="oceanbase-ce-libs-4.3.0.0-1.alios7.aarch64.rpm" -t raw_observer .
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
docker build --build-arg LOCAL_RPM="oceanbase-ce-4.3.0.0-1.alios7.aarch64.rpm" --build-arg LOCAL_LIB_RPM="oceanbase-ce-libs-4.3.0.0-1.alios7.aarch64.rpm" -t observer .
if [ $? == 0 ]; then
    echo "================== fast boot docker build ok ==============="
else
    echo "================== fast boot docker build failed ==============="
    exit -1
fi