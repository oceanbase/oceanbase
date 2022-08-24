#!/bin/bash -x
if [ $# -lt 1 ]
then
  echo "Usage ./copy.sh [oceanbase_dev_dir]"
  BUILD_DIR=$(find $PWD/../../ -maxdepth 1 -name 'build_*' -type d | head -1)
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
  cp -f $SOURCE_DIR/deps/3rd/usr/local/oceanbase/deps/devel/lib/libaio.so $LIB_DIR
  cp -f $SOURCE_DIR/deps/3rd/usr/local/oceanbase/deps/devel/lib/libaio.so.1 $LIB_DIR
  cp -f $SOURCE_DIR/deps/3rd/usr/local/oceanbase/deps/devel/lib/mariadb/libmariadb.so $LIB_DIR
  cp -f $SOURCE_DIR/deps/3rd/usr/local/oceanbase/deps/devel/lib/mariadb/libmariadb.so.3 $LIB_DIR
  cp -f $BUILD_DIR/src/observer/observer $BIN_DIR/observer
fi

exit 0
