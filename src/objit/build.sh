#!/bin/sh

TOPDIR=`dirname $(readlink -f $0)`

if [ "$DEP_DIR" == "" ]
then
    export DEP_DIR=`readlink -f $TOPDIR/rpm/.dep_create`
fi

export CC=${CC:-$DEP_DIR/bin/gcc}
export CXX=${CXX:-$DEP_DIR/bin/g++}
export CMAKE=${CMAKE:-$DEP_DIR/bin/cmake}
echo "CC=$CC"
echo "CXX=$CXX"
echo "CMAKE=$CMAKE"

if [[ "$OBJIT_BUILD" != "" ]]
then
    BUILDDIR=$OBJIT_BUILD
elif [[ `readlink -f $PWD` == $TOPDIR ]]
then
    BUILDDIR=$TOPDIR/build
else
    BUILDDIR=$PWD
fi
echo BUILDDIR=$BUILDDIR

function init {
    if test ! -e $BUILDDIR
    then
        mkdir -p $BUILDDIR || return 2
        echo "Create build directory: $BUILDDIR"
    elif test -d $BUILDDIR -o -L $BUILDDIR
    then
        echo "Use existing directory: $BUILDDIR"
    else
        echo "build directory illegal: $BUILDDIR"
        exit -2
    fi

    if [[ $1 == '1' ]]
    then
        (cd $TOPDIR && dep_create rpm/objit) || exit -3
        (git submodule init && git submodule update) || exit -4
    fi
}

function build {
    (cd $BUILDDIR && $CMAKE -DCMAKE_INSTALL_PREFIX=. $TOPDIR)
}

function usage {
    echo "Usage: build.sh [-h|init|qinit|clean]"
}

case "$1" in
    -h)
        usage
        ;;
    init)
        init 1
        ;;
    qinit)
        init 0
        ;;
    clean)
        echo 'cleaning...'
        echo 'rm -rf build/*'
        rm -rf build/*
        echo 'done'
        ;;
    install)
        (cd $BUILDDIR && make install)
        ;;
    *)
        echo ">>>>>>>>>>>>>>>> BUILD objit <<<<<<<<<<<<<<<<<"
        build "$@"
        echo ">>>>>>>>>>>>>> BUILD objit done <<<<<<<<<<<<<<<"
        ;;
esac
