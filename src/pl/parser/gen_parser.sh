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

bison_parser() {
BISON_OUTPUT="$(bison -v -Werror -d $1 -o $2 2>&1)"
BISON_RETURN="$?"
echo $BISON_OUTPUT
if [ $BISON_RETURN -ne 0 ]
  then
  >&2 echo "Compile error: $BISON_OUTPUT, abort."
  exit 1
fi
if [[ $BISON_OUTPUT == *"conflict"* ]]
then
  >&2 echo "Compile conflict: $BISON_OUTPUT, abort."
  exit 1
fi
}

# generate pl_parser
bison_parser ../../../src/pl/parser/pl_parser_mysql_mode.y ../../../src/pl/parser/pl_parser_mysql_mode_tab.c
flex -o ../../../src/pl/parser/pl_parser_mysql_mode_lex.c ../../../src/pl/parser/pl_parser_mysql_mode.l ../../../src/pl/parser/pl_parser_mysql_mode_tab.h

# TODO  delete the following line at 9.30
rm -rf ../../../src/pl/parser/pl_parser_oracle_mode_lex.c ../../../src/pl/parser/pl_parser_oracle_mode_tab.c ../../../src/pl/parser/pl_parser_oracle_mode_tab.h

if [ -d "../../../close_modules/oracle_pl/pl/parser/" ]; then
  bison_parser ../../../close_modules/oracle_pl/pl/parser/pl_parser_oracle_mode.y ../../../close_modules/oracle_pl/pl/parser/pl_parser_oracle_mode_tab.c
  flex -o ../../../close_modules/oracle_pl/pl/parser/pl_parser_oracle_mode_lex.c ../../../close_modules/oracle_pl/pl/parser/pl_parser_oracle_mode.l ../../../close_modules/oracle_pl/pl/parser/pl_parser_oracle_mode_tab.h
fi

#./gen_type_name.sh ob_item_type.h >type_name.c

