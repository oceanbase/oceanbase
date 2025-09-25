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

cat ../../../src/pl/parser/pl_parser_mysql_mode.y >> $TEMP_FILE
cat ../../../src/pl/parser/pl_parser_mysql_mode.l >> $TEMP_FILE
if [ -d "../../../close_modules/oracle_pl/pl/parser/" ]; then
  cat ../../../close_modules/oracle_pl/pl/parser/pl_parser_oracle_mode.y >> $TEMP_FILE
  cat ../../../close_modules/oracle_pl/pl/parser/pl_parser_oracle_mode.l >> $TEMP_FILE
fi
if [ -d "../../../close_modules/oracle_pl/pl/wrap/" ]; then
  cat ../../../close_modules/oracle_pl/pl/wrap/pl_wrap_parser.y >> $TEMP_FILE
  cat ../../../close_modules/oracle_pl/pl/wrap/pl_wrap_scanner.l >> $TEMP_FILE
fi

md5sum_value=$(md5sum "$TEMP_FILE" | awk '{ print $1 }')

function generate_parser {
# generate pl_parser
  bison_parser ../../../src/pl/parser/pl_parser_mysql_mode.y ../../../src/pl/parser/pl_parser_mysql_mode_tab.c
  flex -o ../../../src/pl/parser/pl_parser_mysql_mode_lex.c ../../../src/pl/parser/pl_parser_mysql_mode.l ../../../src/pl/parser/pl_parser_mysql_mode_tab.h

  # TODO  delete the following line at 9.30
  rm -rf ../../../src/pl/parser/pl_parser_oracle_mode_lex.c ../../../src/pl/parser/pl_parser_oracle_mode_tab.c ../../../src/pl/parser/pl_parser_oracle_mode_tab.h

  if [ -d "../../../close_modules/oracle_pl/pl/parser" ]; then
    pushd ../../../close_modules/oracle_pl/pl/parser > /dev/null
    bison_parser pl_parser_oracle_mode.y pl_parser_oracle_mode_tab.c
    flex -o pl_parser_oracle_mode_lex.c pl_parser_oracle_mode.l pl_parser_oracle_mode_tab.h
    popd > /dev/null
  fi

  if [ -d "../../../close_modules/oracle_pl/pl/wrap" ]; then
    pushd ../../../close_modules/oracle_pl/pl/wrap > /dev/null
    bison_parser pl_wrap_parser.y pl_wrap_parser.c
    flex pl_wrap_scanner.l
    popd > /dev/null
  fi

  #./gen_type_name.sh ob_item_type.h >type_name.c
  echo "$md5sum_value" > $CACHE_MD5_FILE
}

if [[ -n "$NEED_PARSER_CACHE" && "$NEED_PARSER_CACHE" == "ON" ]]; then
    echo "generate pl parser with cache"
    origin_md5sum_value=$(<$CACHE_MD5_FILE)
    if [[ "$md5sum_value" == "$origin_md5sum_value" ]]; then
      echo "hit the md5 cache"
    else
      generate_parser
    fi
else
    echo "generate pl parser without cache"
    generate_parser
fi

rm -rf $TEMP_FILE