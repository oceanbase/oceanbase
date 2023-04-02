#!/bin/bash
#
# AUTHOR: Zhifeng YANG
# DATE: 2012-10-24
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

# generate mysql sql_parser
bison_parser ../../../src/sql/parser/sql_parser_mysql_mode.y ../../../src/sql/parser/sql_parser_mysql_mode_tab.c
flex -Cfa -B -8 -o ../../../src/sql/parser/sql_parser_mysql_mode_lex.c ../../../src/sql/parser/sql_parser_mysql_mode.l ../../../src/sql/parser/sql_parser_mysql_mode_tab.h

sed "/Setup the input buffer state to scan the given bytes/,/}/{/int i/d}" -i ../../../src/sql/parser/sql_parser_mysql_mode_lex.c
sed "/Setup the input buffer state to scan the given bytes/,/}/{/for ( i = 0; i < _yybytes_len; ++i )/d}" -i ../../../src/sql/parser/sql_parser_mysql_mode_lex.c
sed "/Setup the input buffer state to scan the given bytes/,/}/{s/\tbuf\[i\] = yybytes\[i\]/memcpy(buf, yybytes, _yybytes_len)/g}" -i ../../../src/sql/parser/sql_parser_mysql_mode_lex.c


# generate type name
./gen_type_name.sh ../../../src/objit/include/objit/common/ob_item_type.h > type_name.c
