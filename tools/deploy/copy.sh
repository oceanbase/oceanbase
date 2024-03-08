#!/bin/bash -x
SOURCE_DIR=$(readlink -f "$(dirname ${BASH_SOURCE[0]})/../..")

if [ $# -lt 1 ]
then
  BUILD_DIR=$(find $SOURCE_DIR -maxdepth 1 -name 'build_*' -type d | grep -v 'build_ccls'  | head -1)
  if [[ "$BUILD_DIR" == "" ]]
  then
    echo "Usage ./copy.sh [oceanbase_dev_dir]"
  else
    echo "Choose $BUILD_DIR as build directory of oceanbase."
  fi
else
  BUILD_DIR=$1
fi

BIN_DIR=`pwd`/bin${VER}
LIB_DIR=`pwd`/lib
TOOL_DIR=`pwd`/tools
ETC_DIR=`pwd`/etc
DEBUG_DIR=`pwd`/debug
ADMIN_DIR=`pwd`/admin

function do_install {
  quiet=false
  if [ $# -eq 3 ] && [[ "$3" == "true" ]]
  then
    quiet=true
  fi
  [[ "$quiet" == "false" ]] && echo -n "Installing $1 "
  sources=$(ls $1 2>/dev/null)
  if [[ "$sources" == "" ]]
  then
    [[ "$quiet" == "false" ]] && echo -e "\033[0;31mFAIL\033[0m\nNo such file: $1"
    if [ "$quiet" == "false" ]
    then
      return 1
    else
      return 0
    fi
  fi
  target=$2
  err_msg=$(libtool --mode=install cp $sources $target 2>&1 >/dev/null)
  if [ $? -eq 0 ]
  then
    [[ "$quiet" == "false" ]] && echo -e "\033[0;32mOK\033[0m"
  else
    [[ "$quiet" == "false" ]] && echo -e "\033[0;31mFAIL\033[0m\n$err_msg"
  fi
}

if [ $# -lt 2 ]
then
  mkdir -p $BIN_DIR
  mkdir -p $LIB_DIR
  mkdir -p $TOOL_DIR
  mkdir -p $ETC_DIR
  mkdir -p $DEBUG_DIR
  mkdir -p $ADMIN_DIR
  if [ -f $SOURCE_DIR/deps/oblib/src/lib/compress/liblz4_1.0.la ]; then
    do_install $SOURCE_DIR/deps/oblib/src/lib/compress/liblz4_1.0.la $LIB_DIR
    do_install $SOURCE_DIR/deps/oblib/src/lib/compress/libnone.la $LIB_DIR
    do_install $SOURCE_DIR/deps/oblib/src/lib/compress/libsnappy_1.0.la $LIB_DIR
    do_install $SOURCE_DIR/deps/oblib/src/lib/compress/libzlib_1.0.la $LIB_DIR
  fi
  do_install $BUILD_DIR/src/observer/observer $BIN_DIR/observer
  do_install "$SOURCE_DIR/src/share/inner_table/sys_package/*.sql" $ADMIN_DIR
  do_install $SOURCE_DIR/deps/3rd/usr/local/oceanbase/devtools/bin/llvm-symbolizer $TOOL_DIR/
  do_install $SOURCE_DIR/rpm/.dep_create/lib/libstdc++.so.6 $LIB_DIR true
  do_install $SOURCE_DIR/deps/oblib/src/lib/profile/obperf $TOOL_DIR/ true
  do_install $SOURCE_DIR/deps/3rd/home/admin/oceanbase/bin/obshell $BIN_DIR/obshell true


  do_install ./usr/lib/oracle/12.2/client64/lib/libclntsh.so.12.1 $LIB_DIR true
  do_install ./usr/lib/oracle/12.2/client64/lib/libclntsh.so.12.1 $LIB_DIR/libclntsh.so true
  do_install ./usr/lib/oracle/12.2/client64/lib/libclntshcore.so.12.1 $LIB_DIR true
  do_install ./usr/lib/oracle/12.2/client64/lib/libnnz12.so $LIB_DIR true
  do_install ./usr/lib/oracle/12.2/client64/lib/libons.so $LIB_DIR true
  do_install ./usr/lib/oracle/12.2/client64/lib/libociei.so $LIB_DIR true
  do_install ./usr/lib/oracle/12.2/client64/lib/libmql1.so $LIB_DIR true
  do_install ./usr/lib/oracle/12.2/client64/lib/libipc1.so $LIB_DIR true

fi
