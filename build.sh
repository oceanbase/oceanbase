#!/bin/bash

TOPDIR=`readlink -f \`dirname $0\``
BUILD_SH=$TOPDIR/build.sh

DEP_DIR=${TOPDIR}/deps/3rd/usr/local/oceanbase/deps/devel
TOOLS_DIR=${TOPDIR}/deps/3rd/usr/local/oceanbase/devtools
CMAKE_COMMAND="${TOOLS_DIR}/bin/cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=1"

CPU_CORES=`grep -c ^processor /proc/cpuinfo`
KERNEL_RELEASE=`grep -Po 'release [0-9]{1}' /etc/issue 2>/dev/null`

ALL_ARGS=("$@")
BUILD_ARGS=()
MAKE_ARGS=(-j $CPU_CORES)
NEED_MAKE=false
NEED_INIT=false
LLD_OPTION=ON
ASAN_OPTION=ON
STATIC_LINK_LGPL_DEPS_OPTION=ON

echo "$0 ${ALL_ARGS[@]}"

function echo_log() {
  echo -e "[build.sh] $@"
}

function echo_err() {
  echo -e "[build.sh][ERROR] $@" 1>&2
}

function usage
{
    echo -e "Usage:"
    echo -e "\t./build.sh -h"
    echo -e "\t./build.sh init"
    echo -e "\t./build.sh clean"
    echo -e "\t./build.sh [BuildType] [--init] [--make [MakeOptions]]"

    echo -e "\nOPTIONS:"
    echo -e "\tBuildType => debug(default), release, errsim, dissearray, rpm"
    echo -e "\tMakeOptions => Options to make command, default: -j N"

    echo -e "\nExamples:"
    echo -e "\t# Build by debug mode and make with -j24."
    echo -e "\t./build.sh debug --make -j24"

    echo -e "\n\t# Init and build with release mode but not compile."
    echo -e "\t./build.sh release --init"

    echo -e "\n\t# Build with rpm mode and make with default arguments."
    echo -e "\t./build.sh rpm --make"
}

# parse arguments
function parse_args
{
    for i in "${ALL_ARGS[@]}"; do
        if [[ "$i" == "--init" ]]
        then
            NEED_INIT=true
        elif [[ "$i" == "--make" ]]
        then
            NEED_MAKE=make
        elif [[ $NEED_MAKE == false ]]
        then
            BUILD_ARGS+=("$i")
        else
            MAKE_ARGS+=("$i")
        fi
    done

    if [[ "$KERNEL_RELEASE" == "release 6" ]]; then
        echo_log '[NOTICE] lld is disabled in kernel release 6'
        LLD_OPTION="OFF"
    fi
}

# try call command make, if use give --make in command line.
function try_make
{
    if [[ $NEED_MAKE != false ]]
    then
        $NEED_MAKE "${MAKE_ARGS[@]}"
    fi
}

# try call init if --init given.
function try_init
{
    if [[ $NEED_INIT == true ]]
    then
        do_init || exit $?
    fi
}

# create build directory and cd it.
function prepare_build_dir
{
    TYPE=$1
    mkdir -p $TOPDIR/build_$TYPE && cd $TOPDIR/build_$TYPE
}

# dep_create
function do_init
{
    time1_ms=$(echo $[$(date +%s%N)/1000000])
    (cd $TOPDIR/deps/init && bash dep_create.sh)
    if [ $? -ne 0 ]; then
      exit $?
    fi
    time2_ms=$(echo $[$(date +%s%N)/1000000])

    cost_time_ms=$(($time2_ms - $time1_ms))
    cost_time_s=`expr $cost_time_ms / 1000`
    let min=cost_time_s/60
    let sec=cost_time_s%60
    echo_log "use dep_create.sh to create deps cost time: ${min}m${sec}s"

}

# make build directory && cmake && make (if need)
function do_build
{
    if [ ! -f ${TOOLS_DIR}/bin/cmake ]; then
      echo_log "[NOTICE] Your workspace has not initialized dependencies, please append '--init' args to initialize dependencies"
      exit 1
    fi

    TYPE=$1; shift
    prepare_build_dir $TYPE || return
    ${CMAKE_COMMAND} ${TOPDIR} "$@"
    if [ $? -ne 0 ]; then
      echo_err "Failed to generate Makefile"
      exit 1
    fi
}

# clean build directories
function do_clean
{
    echo_log "cleaning..."
    find . -maxdepth 1 -type d -name 'build_*' | grep -v 'build_ccls' | xargs rm -rf
}

# build - configurate project and prepare to compile, by calling make
function build
{

    set -- "${BUILD_ARGS[@]}"
    case "x$1" in
      xrelease)
        do_build "$@" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DOB_USE_LLD=$LLD_OPTION
        ;;
      xrelease_no_unity)
        do_build "$@" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DOB_USE_LLD=$LLD_OPTION -DOB_ENABLE_UNITY=OFF
        ;;
      xrelease_asan)
        do_build "$@" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DOB_USE_LLD=$LLD_OPTION -DOB_USE_ASAN=$ASAN_OPTION
        ;;
      xrelease_coverage)
        do_build "$@" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DOB_USE_LLD=$LLD_OPTION -DWITH_COVERAGE=ON
        ;;
      xdebug)
        do_build "$@" -DCMAKE_BUILD_TYPE=Debug -DOB_USE_LLD=$LLD_OPTION
        ;;
      xdebug_no_unity)
        do_build "$@" -DCMAKE_BUILD_TYPE=Debug -DOB_USE_LLD=$LLD_OPTION -DOB_ENABLE_UNITY=OFF
        ;;
      xccls)
        do_build "$@" -DCMAKE_BUILD_TYPE=Debug -DOB_USE_LLD=$LLD_OPTION -DOB_BUILD_CCLS=ON
        # build soft link for ccls
        ln -sf ${TOPDIR}/build_ccls/compile_commands.json ${TOPDIR}/compile_commands.json
        ;;
      xperf)
        do_build "$@" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DENABLE_AUTO_FDO=ON -DENABLE_THIN_LTO=ON -DOB_USE_LLD=$LLD_OPTION
        ;;
      xdebug_asan)
        do_build "$@" -DCMAKE_BUILD_TYPE=Debug -DOB_USE_LLD=$LLD_OPTION -DOB_USE_ASAN=$ASAN_OPTION
        ;;
      xerrsim_asan)
        do_build "$@" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DOB_ERRSIM=ON -DOB_USE_LLD=$LLD_OPTION -DOB_USE_ASAN=$ASAN_OPTION
        ;;
      xerrsim_debug)
        do_build "$@" -DCMAKE_BUILD_TYPE=Debug -DOB_ERRSIM=ON -DOB_USE_LLD=$LLD_OPTION
        ;;
      xerrsim)
        do_build "$@" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DOB_ERRSIM=ON -DOB_USE_LLD=$LLD_OPTION
        ;;
      xdissearray)
        do_build "$@" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DOB_DIS_SEARRAY=ON -DOB_USE_LLD=$LLD_OPTION
        ;;
      xtrans_module_test)
        do_build "$@" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DTRANS_MODULE_TEST=ON -DOB_USE_LLD=$LLD_OPTION
        ;;
      xenable_latch_diagnose)
        do_build "$@" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DENABLE_LATCH_DIAGNOSE=ON -DOB_USE_LLD=$LLD_OPTION
        ;;
      xenable_memory_diagnosis)
        do_build "$@" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DENABLE_DEBUG_LOG=ON -DENABLE_MEMORY_DIAGNOSIS=ON -DOB_USE_LLD=$LLD_OPTION
        ;;
      xenable_obj_leak_check)
        do_build "$@" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DENABLE_DEBUG_LOG=ON -DENABLE_OBJ_LEAK_CHECK=ON -DOB_USE_LLD=$LLD_OPTION
        ;;
      xrpm)
        STATIC_LINK_LGPL_DEPS_OPTION=OFF
        do_build "$@" -DOB_BUILD_RPM=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo -DOB_USE_LLD=$LLD_OPTION -DENABLE_FATAL_ERROR_HANG=OFF -DENABLE_AUTO_FDO=ON -DENABLE_THIN_LTO=ON -DOB_STATIC_LINK_LGPL_DEPS=$STATIC_LINK_LGPL_DEPS_OPTION
        ;;
      xenable_smart_var_check)
        do_build "$@" -DCMAKE_BUILD_TYPE=Debug -DOB_USE_LLD=$LLD_OPTION -DENABLE_SMART_VAR_CHECK=ON -DOB_ENABLE_AVX2=ON
        ;;
      xcoverage)
        do_build "$@" -DCMAKE_BUILD_TYPE=Debug -DOB_USE_LLD=$LLD_OPTION -DWITH_COVERAGE=ON
        ;;
      xsanity)
        do_build "$@" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DOB_USE_LLD=$LLD_OPTION -DENABLE_SANITY=ON
        ;;
      *)
        BUILD_ARGS=(debug "${BUILD_ARGS[@]}")
        build
        ;;
    esac
}

function main
{
    case "$1" in
        -h)
            usage
            ;;
        init)
            parse_args
            do_init
            ;;
        clean)
            do_clean
            ;;
        *)
            parse_args
            try_init
            build
            try_make
            ;;
    esac
}

main "$@"
