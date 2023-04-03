#!/bin/bash
#
# AUTHOR: yuchen.wyc
# DATE: 2016-12-28
# DESCRIPTION:
#
set +x
CURDIR="$(dirname $(readlink -f "$0"))"
#export PATH=/usr/local/bin:$PATH
export PATH=$CURDIR/../../../deps/3rd/usr/local/oceanbase/devtools/bin/:$PATH
export BISON_PKGDATADIR=$CURDIR/../../../deps/3rd/usr/local/oceanbase/devtools/share/bison

BISON_VERSION=`bison -V| grep 'bison (GNU Bison)'|awk '{ print  $4;}'`
NEED_VERSION='2.4.1'

if [ "$BISON_VERSION" != "$NEED_VERSION" ]; then
  echo "bison version not match, please use bison-$NEED_VERSION"
  exit 1
fi

# generate pl_parser
bison -v -Werror -d ../../../src/pl/parser/pl_parser_mysql_mode.y -o ../../../src/pl/parser/pl_parser_mysql_mode_tab.c
if [ $? -ne 0 ]
then
    echo Compile error[$?], abort
    exit 1
fi
if [ $? -ne 0 ]
then
    echo Compile error[$?], abort.
    exit 1
fi
flex -o ../../../src/pl/parser/pl_parser_mysql_mode_lex.c ../../../src/pl/parser/pl_parser_mysql_mode.l ../../../src/pl/parser/pl_parser_mysql_mode_tab.h
#./gen_type_name.sh ob_item_type.h >type_name.c

