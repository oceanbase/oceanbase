#!/bin/bash

cmake_flags=(
    # CMAKE_BUILD_TYPE
    # values: Release/Debug
    -DCMAKE_BUILD_TYPE=Release

    # CMAKE_VERBOSE_MAKEFILE
    # ON: verbose build output, OFF: silent build output
    -DCMAKE_VERBOSE_MAKEFILE=ON

    -DOB_BUILD_RPM=OFF

    # OB_USE_CLANG
    # ON: use clang, OFF: use gcc
    -DOB_USE_CLANG=ON

    # OB_USE_LLVM_LIBTOOLS
    # ON: use lld & llvm-objcopy, OFF: use ld & objcopy
    -DOB_USE_LLVM_LIBTOOLS=ON

    -DOB_COMPRESS_DEBUG_SECTIONS=OFF
    -DOB_STATIC_LINK_LGPL_DEPS=OFF

    -DOB_USE_CCACHE=OFF
    -DOB_ENABLE_PCH=ON
    -DOB_ENALBE_UNITY=ON
    -DOB_MAX_UNITY_BATCH_SIZE=30
    -DOB_USE_ASAN=OFF

    -DOB_RELEASEID=1
)

# check the depends library
for i in openssl RapidJSON libisal; do
    pkg-config --exists $i
    if [ $? -ne 0 ];then
        echo "Please install $i"
        exit
    fi
done

# use split build directory for gcc/clang (release/debug)
BUILD_DIR=build
eval "export `echo "${cmake_flags[@]}" | sed 's/\-D//g'`"
if [ $OB_USE_CLANG == "ON" ]; then
  BUILD_DIR="${BUILD_DIR}.clang"
elif [ $OB_USE_CLANG == "OFF" ]; then
  BUILD_DIR="${BUILD_DIR}.gcc"
fi

if [ $OB_USE_LLVM_LIBTOOLS == "ON" ]; then
  BUILD_DIR="${BUILD_DIR}.lld"
elif [ $OB_USE_LLVM_LIBTOOLS == "OFF" ]; then
  BUILD_DIR="${BUILD_DIR}.ld"
fi

if [ $CMAKE_BUILD_TYPE == "Release" ];then
  BUILD_DIR="${BUILD_DIR}.release"
elif [ $CMAKE_BUILD_TYPE == "Debug" ];then
  BUILD_DIR="${BUILD_DIR}.debug"
fi
eval "unset `echo "${cmake_flags[@]}" | sed 's/\-D//g' | sed 's/=[0-9a-zA-Z]*/ /g'`"

[ -d $BUILD_DIR ] || mkdir -p $BUILD_DIR
cd $BUILD_DIR

cmake "${cmake_flags[@]}" ..

make -j`nproc` 2>&1 | tee build.log
