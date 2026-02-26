#!/bin/bash
#
# AUTHOR: huangrenhuang.hrh
# DATE: 2025-3-3
# DESCRIPTION:
#
set +x
CURDIR="$(dirname $(readlink -f "$0"))"
#export PATH=/usr/local/bin:$PATH
export PATH=$CURDIR/../../../../deps/3rd/usr/local/oceanbase/devtools/bin/:$PATH
export BISON_PKGDATADIR=$CURDIR/../../../../deps/3rd/usr/local/oceanbase/devtools/share/bison
CACHE_MD5_FILE=$CURDIR/_MD5
TEMP_FILE=$(mktemp)

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

cat ./dbms_sched_parser_calendar.y >> $TEMP_FILE
cat ./dbms_sched_parser_calendar.l >> $TEMP_FILE

md5sum_value=$(md5sum "$TEMP_FILE" | awk '{ print $1 }')

function generate_parser {
  #generate dbms_sched_parser_calendar
  bison_parser ./dbms_sched_parser_calendar.y ./dbms_sched_parser_calendar_tab.c
  flex -o ./dbms_sched_parser_calendar_lex.c ./dbms_sched_parser_calendar.l ./dbms_sched_parser_calendar_tab.h

  #./gen_type_name.sh ob_item_type.h >type_name.c
  echo "$md5sum_value" > $CACHE_MD5_FILE
}

echo "generate pl parser without cache"
generate_parser

rm -rf $TEMP_FILE