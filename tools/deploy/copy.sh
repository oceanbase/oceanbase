#!/bin/bash -x
if [ $# -lt 1 ]
then
  echo "Usage ./copy.sh [oceanbase_dev_dir]"
  BUILD_DIR=$(find $PWD/../../ -maxdepth 1 -name 'build_*' -type d | grep -v 'build_ccls' | head -1)
  SOURCE_DIR=$PWD/../../
else
  BUILD_DIR=$1
  SOURCE_DIR=$PWD/../../
fi

BIN_DIR=`pwd`/bin${VER}
LIB_DIR=`pwd`/lib
TOOL_DIR=`pwd`/tools
ETC_DIR=`pwd`/etc
DEBUG_DIR=`pwd`/debug
ADMIN_DIR=`pwd`/admin
if [ $# -lt 2 ]
then
  mkdir -p $BIN_DIR
  mkdir -p $LIB_DIR
  mkdir -p $TOOL_DIR
  mkdir -p $ETC_DIR
  mkdir -p $DEBUG_DIR
  mkdir -p $ADMIN_DIR
  if [ -f $SOURCE_DIR/deps/oblib/src/lib/compress/liblz4_1.0.la ]; then
    libtool --mode=install cp $SOURCE_DIR/deps/oblib/src/lib/compress/liblz4_1.0.la $LIB_DIR
    libtool --mode=install cp $SOURCE_DIR/deps/oblib/src/lib/compress/libnone.la $LIB_DIR
    libtool --mode=install cp $SOURCE_DIR/deps/oblib/src/lib/compress/libsnappy_1.0.la $LIB_DIR
    libtool --mode=install cp $SOURCE_DIR/deps/oblib/src/lib/compress/libzlib_1.0.la $LIB_DIR
  fi
  libtool --mode=install cp $SOURCE_DIR/rpm/.dep_create/lib/libstdc++.so.6 $LIB_DIR
  libtool --mode=install cp $SOURCE_DIR/deps/oblib/src/lib/profile/obperf $TOOL_DIR/
  libtool --mode=install cp $BUILD_DIR/src/observer/observer $BIN_DIR/observer
  libtool --mode=install cp $SOURCE_DIR/src/share/inner_table/sys_package/*.sql $ADMIN_DIR

  libtool --mode=install cp ./usr/lib/oracle/12.2/client64/lib/libclntsh.so.12.1 $LIB_DIR
  libtool --mode=install cp ./usr/lib/oracle/12.2/client64/lib/libclntsh.so.12.1 $LIB_DIR/libclntsh.so
  libtool --mode=install cp ./usr/lib/oracle/12.2/client64/lib/libclntshcore.so.12.1 $LIB_DIR
  libtool --mode=install cp ./usr/lib/oracle/12.2/client64/lib/libnnz12.so $LIB_DIR
  libtool --mode=install cp ./usr/lib/oracle/12.2/client64/lib/libons.so $LIB_DIR
  libtool --mode=install cp ./usr/lib/oracle/12.2/client64/lib/libociei.so $LIB_DIR
  libtool --mode=install cp ./usr/lib/oracle/12.2/client64/lib/libmql1.so $LIB_DIR
  libtool --mode=install cp ./usr/lib/oracle/12.2/client64/lib/libipc1.so $LIB_DIR

  libtool --mode=install cp $SOURCE_DIR/deps/3rd/usr/local/oceanbase/devtools/bin/llvm-symbolizer $TOOL_DIR/

fi
