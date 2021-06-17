#!/bin/bash
TOPDIR="$(dirname $(readlink -f "$0"))"
BUILD_SH=${TOPDIR}/build.sh
DEP_DIR=${TOPDIR}/deps/3rd/usr/local/oceanbase/deps/devel
TOOLS_DIR=${TOPDIR}/deps/3rd/usr/local/oceanbase/devtools
CMAKE_COMMAND=${TOOLS_DIR}/bin/cmake
CPU_CORES=`grep -c ^processor /proc/cpuinfo`

ALL_ARGS=("$@")
BUILD_ARGS=()
MAKE_ARGS=(-j $CPU_CORES)
NEED_MAKE=false
NEED_INIT=false


#echo "$0 ${ALL_ARGS[@]}"

function usage
{
    echo -e "Usage:
\t./build.sh -h
\t./build.sh init
\t./build.sh clean
\t./build.sh [BuildType] [--init] [--make [MakeOptions]]

OPTIONS:
    BuildType => debug(default), release, errsim, dissearray, rpm
    MakeOptions => Options to make command, default: -j N

Examples:
\t# Build by debug mode and make with -j24.
\t./build.sh debug --make -j24

\t# Init and build with release mode but not compile.
\t./build.sh release --init

\t# Build with rpm mode and make with default arguments.
\t./build.sh rpm --make
"
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
    (cd $TOPDIR/deps/3rd && bash dep_create.sh)
}

# make build directory && cmake && make (if need)
function do_build
{
    TYPE=$1; shift
    prepare_build_dir $TYPE || return
    ${CMAKE_COMMAND} ${TOPDIR} "$@"
}

# clean build directories
function do_clean
{
    echo "cleaning..."
    find . -maxdepth 1 -type d -name 'build_*' | xargs rm -rf
}

# build - configurate project and prepare to compile, by calling make
function build
{
    set -- "${BUILD_ARGS[@]}"
    case "x$1" in
      xrelease)
        do_build "$@" -DCMAKE_BUILD_TYPE=RelWithDebInfo
        ;;
      xdebug)
        do_build "$@" -DCMAKE_BUILD_TYPE=Debug
        ;;
      xrpm)
        do_build "$@" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DOB_USE_CCACHE=OFF -DOB_COMPRESS_DEBUG_SECTIONS=ON -DOB_STATIC_LINK_LGPL_DEPS=OFF -DOB_ENABLE_PCH=OFF -DOB_ENALBE_UNITY=OFF
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
