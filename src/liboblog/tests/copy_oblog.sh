#!/bin/bash

OBLOG_DIR=`pwd`/lib
OBLOG_TAILF_DIR=`pwd`

if [ $# -lt 1 ]
then
#  echo "Usage ./copy_oblog.sh [oceanbase_dev_dir]"
#  echo "Eg; ./copy_oblog.sh 1 that means copy from build_debug"
#  echo "Eg: ./copy_oblog.sh 2 that means copy from build_release"

  if [ -d "../../../build_debug" ]
  then
    OCEANBASE_DIR="../../../build_debug"
  elif [ -d "../../../build_release" ]
  then
    OCEANBASE_DIR="../../../build_release"
  fi

else
  #echo $1
  ver_flag=0
  if [ $1 -eq 1 ]
  then
    OCEANBASE_DIR="../../../build_debug"
    ver_flag=1
  elif [ $1 -eq 2 ]
  then
    OCEANBASE_DIR="../../../build_release"
    ver_flag=1
  else
    ver_flag=0
    echo "parameter is invalid"
  fi
fi

echo "copy liboblog.so, oblog_tailf from "$OCEANBASE_DIR

OBLOG_SO="$OCEANBASE_DIR/src/liboblog/src/liboblog.so.1"
OBLOG_TAILF="$OCEANBASE_DIR/src/liboblog/tests/oblog_tailf"

mkdir -p $OBLOG_DIR
[ -f $OBLOG_SO ] && libtool --mode=install cp  $OBLOG_SO $OBLOG_DIR/
[ -f $OBLOG_TAILF ] && libtool --mode=install cp $OBLOG_TAILF $OBLOG_TAILF_DIR

