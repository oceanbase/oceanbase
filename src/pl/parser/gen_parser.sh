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

    # generate generic oracle mode tab header in close_modules
    # (needed by pl_non_reserved_keywords_oracle_mode.c which provides get_oracle_pl_*_keywords)
    pushd ../../../close_modules/oracle_pl/pl/parser > /dev/null
    bison_parser pl_parser_oracle_mode.y pl_parser_oracle_mode_tab.c
    popd > /dev/null

    ln -sf ../../../close_modules/oracle_pl/pl/parser/pl_parser_oracle_mode.y ../../../src/pl/parser/pl_parser_oracle_mode.y
    ln -sf ../../../close_modules/oracle_pl/pl/parser/pl_parser_oracle_mode.l ../../../src/pl/parser/pl_parser_oracle_mode.l

    # generate oracle latin1 pl_parser
    ##1.copy lex and yacc files
    cat ../../../src/pl/parser/pl_parser_oracle_mode.y > ../../../src/pl/parser/pl_parser_oracle_latin1_mode.y
    cat ../../../src/pl/parser/pl_parser_oracle_mode.l > ../../../src/pl/parser/pl_parser_oracle_latin1_mode.l
    ##2.replace name
    sed  "s/obpl_oracle_yy/obpl_oracle_latin1_yy/g" -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode.y
    sed  "s/obpl_oracle_yy/obpl_oracle_latin1_yy/g" -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode.l
    sed  "s/pl_parser_oracle_mode/pl_parser_oracle_latin1_mode/g" -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode.y
    sed  "s/pl_parser_oracle_mode/pl_parser_oracle_latin1_mode/g" -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode.l
    sed  "s/obpl_oracle_parse_fatal_error/obpl_oracle_latin1_parse_fatal_error/g" -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode.y
    sed  "s/obpl_oracle_parse_fatal_error/obpl_oracle_latin1_parse_fatal_error/g" -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode.l
    sed  "s/obpl_oracle_fast_parse/obpl_oracle_latin1_fast_parse/g" -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode.y
    sed  "s/obpl_oracle_multi_fast_parse/obpl_oracle_latin1_multi_fast_parse/g" -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode.y
    sed  "s/obpl_oracle_multi_values_parse/obpl_oracle_latin1_multi_values_parse/g" -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode.y
    sed  "s/obpl_oracle_read_sql_construct/obpl_oracle_latin1_read_sql_construct/g" -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode.y
    ##3.do not need to replace multi_byte_space、multi_byte_comma、multi_byte_left_parenthesis、multi_byte_right_parenthesis code
    sed  "s/multi_byte_space              \[\\\u3000\]/multi_byte_space              \[\\\x20]/g" -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode.l
    sed  "s/multi_byte_comma              \[\\\uff0c\]/multi_byte_comma              \[\\\x2c]/g" -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode.l
    sed  "s/multi_byte_left_parenthesis   \[\\\uff08\]/multi_byte_left_parenthesis   \[\\\x28]/g" -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode.l
    sed  "s/multi_byte_right_parenthesis  \[\\\uff09\]/multi_byte_right_parenthesis  \[\\\x29]/g" -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode.l
    echo "LATIN1_CHAR [\x80-\xFF]" > ../../../src/pl/parser/latin1.txt
    sed '/following character status will be rewrite by gen_parse.sh according to connection character/d' -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode.l
    sed '/multi_byte_connect_char       \/\*According to connection character to set by gen_parse.sh\*\//r ../../../src/pl/parser/latin1.txt' -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode.l
    sed '/multi_byte_connect_char       \/\*According to connection character to set by gen_parse.sh\*\//d' -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode.l
    sed 's/multi_byte_connect_char/LATIN1_CHAR/g' -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode.l
    sed  "s/oracle_pl_non_reserved_keyword_lookup/oracle_pl_latin1_non_reserved_keyword_lookup/g" -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode.l
    sed  "s/oracle_pl_keyword_lookup/oracle_pl_latin1_keyword_lookup/g" -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode.l
    ##4.generate oracle latin1 parser files
    bison_parser ../../../src/pl/parser/pl_parser_oracle_latin1_mode.y ../../../src/pl/parser/pl_parser_oracle_latin1_mode_tab.c
    flex -o ../../../src/pl/parser/pl_parser_oracle_latin1_mode_lex.c ../../../src/pl/parser/pl_parser_oracle_latin1_mode.l ../../../src/pl/parser/pl_parser_oracle_latin1_mode_tab.h
    ##5.replace other info
    sed "/Setup the input buffer state to scan the given bytes/,/}/{/int i/d}" -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode_lex.c
    sed "/Setup the input buffer state to scan the given bytes/,/}/{/for ( i = 0; i < _yybytes_len; ++i )/d}" -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode_lex.c
    sed "/Setup the input buffer state to scan the given bytes/,/}/{s/\tbuf\[i\] = yybytes\[i\]/memcpy(buf, yybytes, _yybytes_len)/g}" -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode_lex.c
    sed "/obpl_oracle_latin1_yylex_init is special because it creates the scanner itself/,/Initialization is the same as for the non-reentrant scanner/{s/return 1/return errno/g}" -i ../../../src/pl/parser/pl_parser_oracle_latin1_mode_lex.c
    cat ../../../close_modules/oracle_pl/pl/parser/pl_non_reserved_keywords_oracle_mode.c > ../../../src/pl/parser/pl_non_reserved_keywords_oracle_latin1_mode.c
    sed 's/#include "pl\/parser\/pl_parser_oracle_mode_tab.h"/#include "pl\/parser\/pl_parser_oracle_latin1_mode_tab.h"/g' -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_latin1_mode.c
    sed  "s/non_reserved_keywords_oracle_mode.c is for …/pl_non_reserved_keywords_oracle_latin1_mode.c is auto generated by gen_parser.sh/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_latin1_mode.c
    sed  "s/oracle_pl_non_reserved_keyword_lookup/oracle_pl_latin1_non_reserved_keyword_lookup/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_latin1_mode.c
    sed  "s/create_oracle_pl_trie_tree/create_oracle_pl_latin1_trie_tree/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_latin1_mode.c
    sed  "s/oracle_pl_reserved_keyword_lookup/oracle_pl_latin1_reserved_keyword_lookup/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_latin1_mode.c
    sed  "s/create_oracle_pl_reserved_trie_tree/create_oracle_pl_latin1_reserved_trie_tree/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_latin1_mode.c
    sed  "s/oracle_pl_keyword_lookup/oracle_pl_latin1_keyword_lookup/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_latin1_mode.c
    sed  "s/get_oracle_pl_reserved_keywords/get_oracle_pl_latin1_reserved_keywords/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_latin1_mode.c
    sed  "s/get_oracle_pl_non_reserved_keywords/get_oracle_pl_latin1_non_reserved_keywords/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_latin1_mode.c
    sed  "s/init_oracle_pl_keywords_tree/init_oracle_pl_latin1_keywords_tree/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_latin1_mode.c
    ##6.clean useless files
    rm -f ../../../src/pl/parser/latin1.txt
    rm -f ../../../src/pl/parser/pl_parser_oracle_latin1_mode.l
    rm -f ../../../src/pl/parser/pl_parser_oracle_latin1_mode.y

    # generate oracle utf8 pl_parser
    ##1.copy lex and yacc files
    cat ../../../src/pl/parser/pl_parser_oracle_mode.y > ../../../src/pl/parser/pl_parser_oracle_utf8_mode.y
    cat ../../../src/pl/parser/pl_parser_oracle_mode.l > ../../../src/pl/parser/pl_parser_oracle_utf8_mode.l
    ##2.replace name
    sed  "s/obpl_oracle_yy/obpl_oracle_utf8_yy/g" -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode.y
    sed  "s/obpl_oracle_yy/obpl_oracle_utf8_yy/g" -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode.l
    sed  "s/pl_parser_oracle_mode/pl_parser_oracle_utf8_mode/g" -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode.y
    sed  "s/pl_parser_oracle_mode/pl_parser_oracle_utf8_mode/g" -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode.l
    sed  "s/obpl_oracle_parse_fatal_error/obpl_oracle_utf8_parse_fatal_error/g" -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode.y
    sed  "s/obpl_oracle_parse_fatal_error/obpl_oracle_utf8_parse_fatal_error/g" -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode.l
    sed  "s/obpl_oracle_fast_parse/obpl_oracle_utf8_fast_parse/g" -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode.y
    sed  "s/obpl_oracle_multi_fast_parse/obpl_oracle_utf8_multi_fast_parse/g" -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode.y
    sed  "s/obpl_oracle_multi_values_parse/obpl_oracle_utf8_multi_values_parse/g" -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode.y
    sed  "s/obpl_oracle_read_sql_construct/obpl_oracle_utf8_read_sql_construct/g" -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode.y
    ##3.add multi_byte_space、multi_byte_comma、multi_byte_left_parenthesis、multi_byte_right_parenthesis code.
    sed  "s/multi_byte_space              \[\\\u3000\]/multi_byte_space              ([\\\xe3\][\\\x80\][\\\x80])/g" -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode.l
    sed  "s/multi_byte_comma              \[\\\uff0c\]/multi_byte_comma              ([\\\xef\][\\\xbc\][\\\x8c])/g" -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode.l
    sed  "s/multi_byte_left_parenthesis   \[\\\uff08\]/multi_byte_left_parenthesis   ([\\\xef\][\\\xbc\][\\\x88])/g" -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode.l
    sed  "s/multi_byte_right_parenthesis  \[\\\uff09\]/multi_byte_right_parenthesis  ([\\\xef\][\\\xbc\][\\\x89])/g" -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode.l
    echo "U      [\x80-\xbf]
U_1_1  [\x80]
U_1_2  [\x81-\xbf]
U_1_3  [\x80-\xbb]
U_1_4  [\xbc]
U_1_5  [\xbd-\xbf]
U_1_6  [\x80-\x87]
U_1_7  [\x8a-\x8b]
U_1_8  [\x8d-\xbf]
U_2    [\xc2-\xdf]
U_3    [\xe0-\xe2]
U_3_1  [\xe3]
U_3_2  [\xe4-\xee]
U_3_3  [\xef]
U_4    [\xf0-\xf4]
u_except_space ({U_3_1}{U_1_2}{U}|{U_3_1}{U_1_1}{U_1_2})
u_except_comma_parenthesis ({U_3_3}{U_1_3}{U}|{U_3_3}{U_1_4}{U_1_6}|{U_3_3}{U_1_4}{U_1_7}|{U_3_3}{U_1_4}{U_1_8}|{U_3_3}{U_1_5}{U})
UTF8_CHAR ({U_2}{U}|{U_3}{U}{U}|{u_except_space}|{U_3_2}{U}{U}|{u_except_comma_parenthesis}|{U_4}{U}{U}{U})" > ../../../src/pl/parser/pl_parser_oracle_utf8_mode.txt
    sed '/following character status will be rewrite by gen_parse.sh according to connection character/d' -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode.l
    sed '/multi_byte_connect_char       \/\*According to connection character to set by gen_parse.sh\*\//r ../../../src/pl/parser/pl_parser_oracle_utf8_mode.txt' -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode.l
    sed '/multi_byte_connect_char       \/\*According to connection character to set by gen_parse.sh\*\//d' -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode.l
    sed 's/space            \[ \\t\\n\\r\\f\]/space            (\[ \\t\\n\\r\\f\]|{multi_byte_space})/g' -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode.l
    sed 's/multi_byte_connect_char/UTF8_CHAR/g' -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode.l
    sed  "s/oracle_pl_non_reserved_keyword_lookup/oracle_pl_utf8_non_reserved_keyword_lookup/g" -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode.l
    sed  "s/oracle_pl_keyword_lookup/oracle_pl_utf8_keyword_lookup/g" -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode.l
    ##4.generate oracle utf8 parser files
    bison_parser ../../../src/pl/parser/pl_parser_oracle_utf8_mode.y ../../../src/pl/parser/pl_parser_oracle_utf8_mode_tab.c
    flex -o ../../../src/pl/parser/pl_parser_oracle_utf8_mode_lex.c ../../../src/pl/parser/pl_parser_oracle_utf8_mode.l ../../../src/pl/parser/pl_parser_oracle_utf8_mode_tab.h
    ##5.replace other info
    sed "/Setup the input buffer state to scan the given bytes/,/}/{/int i/d}" -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode_lex.c
    sed "/Setup the input buffer state to scan the given bytes/,/}/{/for ( i = 0; i < _yybytes_len; ++i )/d}" -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode_lex.c
    sed "/Setup the input buffer state to scan the given bytes/,/}/{s/\tbuf\[i\] = yybytes\[i\]/memcpy(buf, yybytes, _yybytes_len)/g}" -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode_lex.c
    sed "/obpl_oracle_utf8_yylex_init is special because it creates the scanner itself/,/Initialization is the same as for the non-reentrant scanner/{s/return 1/return errno/g}" -i ../../../src/pl/parser/pl_parser_oracle_utf8_mode_lex.c
    cat ../../../close_modules/oracle_pl/pl/parser/pl_non_reserved_keywords_oracle_mode.c > ../../../src/pl/parser/pl_non_reserved_keywords_oracle_utf8_mode.c
    sed 's/#include "pl\/parser\/pl_parser_oracle_mode_tab.h"/#include "pl\/parser\/pl_parser_oracle_utf8_mode_tab.h"/g' -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_utf8_mode.c
    sed  "s/non_reserved_keywords_oracle_mode.c is for …/pl_non_reserved_keywords_oracle_utf8_mode.c is auto generated by gen_parser.sh/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_utf8_mode.c
    sed  "s/oracle_pl_non_reserved_keyword_lookup/oracle_pl_utf8_non_reserved_keyword_lookup/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_utf8_mode.c
    sed  "s/create_oracle_pl_trie_tree/create_oracle_pl_utf8_trie_tree/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_utf8_mode.c
    sed  "s/oracle_pl_reserved_keyword_lookup/oracle_pl_utf8_reserved_keyword_lookup/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_utf8_mode.c
    sed  "s/create_oracle_pl_reserved_trie_tree/create_oracle_pl_utf8_reserved_trie_tree/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_utf8_mode.c
    sed  "s/oracle_pl_keyword_lookup/oracle_pl_utf8_keyword_lookup/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_utf8_mode.c
    sed  "s/get_oracle_pl_reserved_keywords/get_oracle_pl_utf8_reserved_keywords/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_utf8_mode.c
    sed  "s/get_oracle_pl_non_reserved_keywords/get_oracle_pl_utf8_non_reserved_keywords/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_utf8_mode.c
    sed  "s/init_oracle_pl_keywords_tree/init_oracle_pl_utf8_keywords_tree/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_utf8_mode.c
    ##6.clean useless files
    rm -f ../../../src/pl/parser/pl_parser_oracle_utf8_mode.txt
    rm -f ../../../src/pl/parser/pl_parser_oracle_utf8_mode.l
    rm -f ../../../src/pl/parser/pl_parser_oracle_utf8_mode.y

    # generate oracle gbk pl_parser
    ##1.copy lex and yacc files
    cat ../../../src/pl/parser/pl_parser_oracle_mode.y > ../../../src/pl/parser/pl_parser_oracle_gbk_mode.y
    cat ../../../src/pl/parser/pl_parser_oracle_mode.l > ../../../src/pl/parser/pl_parser_oracle_gbk_mode.l
    ##2.replace name
    sed  "s/obpl_oracle_yy/obpl_oracle_gbk_yy/g" -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode.y
    sed  "s/obpl_oracle_yy/obpl_oracle_gbk_yy/g" -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode.l
    sed  "s/pl_parser_oracle_mode/pl_parser_oracle_gbk_mode/g" -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode.y
    sed  "s/pl_parser_oracle_mode/pl_parser_oracle_gbk_mode/g" -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode.l
    sed  "s/obpl_oracle_parse_fatal_error/obpl_oracle_gbk_parse_fatal_error/g" -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode.y
    sed  "s/obpl_oracle_parse_fatal_error/obpl_oracle_gbk_parse_fatal_error/g" -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode.l
    sed  "s/obpl_oracle_fast_parse/obpl_oracle_gbk_fast_parse/g" -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode.y
    sed  "s/obpl_oracle_multi_fast_parse/obpl_oracle_gbk_multi_fast_parse/g" -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode.y
    sed  "s/obpl_oracle_multi_values_parse/obpl_oracle_gbk_multi_values_parse/g" -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode.y
    sed  "s/obpl_oracle_read_sql_construct/obpl_oracle_gbk_read_sql_construct/g" -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode.y
    ##3.add multi_byte_space、multi_byte_comma、multi_byte_left_parenthesis、multi_byte_right_parenthesis code.
    sed  "s/multi_byte_space              \[\\\u3000\]/multi_byte_space              ([\\\xa1][\\\xa1])/g" -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode.l
    sed  "s/multi_byte_comma              \[\\\uff0c\]/multi_byte_comma              ([\\\xa3][\\\xac])/g" -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode.l
    sed  "s/multi_byte_left_parenthesis   \[\\\uff08\]/multi_byte_left_parenthesis   ([\\\xa3][\\\xa8])/g" -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode.l
    sed  "s/multi_byte_right_parenthesis  \[\\\uff09\]/multi_byte_right_parenthesis  ([\\\xa3][\\\xa9])/g" -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode.l
    echo "GB_1   [\x81-\xfe]
GB_1_1 [\x81-\xa0]
GB_1_2 [\xa1]
GB_1_3 [\xa2]
GB_1_4 [\xa3]
GB_1_5 [\xa4-\xfe]
GB_2   [\x40-\xfe]
GB_2_1 [\x40-\xa0]
GB_2_2 [\xa2-\xfe]
GB_2_3 [\x40-\xa7]
GB_2_4 [\xaa-\xab]
GB_2_5 [\xad-\xfe]
GB_3   [\x30-\x39]
g_except_space ({GB_1_2}{GB_2_1}|{GB_1_2}{GB_2_2})
g_except_comma_parenthesis ({GB_1_4}{GB_2_3}|{GB_1_4}{GB_2_4}|{GB_1_4}{GB_2_5})
GB_CHAR ({GB_1_1}{GB_2}|{g_except_space}|{GB_1_3}{GB_2}|{g_except_comma_parenthesis}|{GB_1_5}{GB_2}|{GB_1}{GB_3}{GB_1}{GB_3})" > ../../../src/pl/parser/pl_parser_oracle_gbk_mode.txt
    sed '/following character status will be rewrite by gen_parse.sh according to connection character/d' -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode.l
    sed '/multi_byte_connect_char       \/\*According to connection character to set by gen_parse.sh\*\//r ../../../src/pl/parser/pl_parser_oracle_gbk_mode.txt' -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode.l
    sed '/multi_byte_connect_char       \/\*According to connection character to set by gen_parse.sh\*\//d' -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode.l
    sed 's/space            \[ \\t\\n\\r\\f\]/space            (\[ \\t\\n\\r\\f\]|{multi_byte_space})/g' -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode.l
    sed 's/multi_byte_connect_char/GB_CHAR/g' -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode.l
    sed  "s/oracle_pl_non_reserved_keyword_lookup/oracle_pl_gbk_non_reserved_keyword_lookup/g" -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode.l
    sed  "s/oracle_pl_keyword_lookup/oracle_pl_gbk_keyword_lookup/g" -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode.l
    ##4.generate oracle gbk parser files
    bison_parser ../../../src/pl/parser/pl_parser_oracle_gbk_mode.y ../../../src/pl/parser/pl_parser_oracle_gbk_mode_tab.c
    flex -o ../../../src/pl/parser/pl_parser_oracle_gbk_mode_lex.c ../../../src/pl/parser/pl_parser_oracle_gbk_mode.l ../../../src/pl/parser/pl_parser_oracle_gbk_mode_tab.h
    ##5.replace other info
    sed "/Setup the input buffer state to scan the given bytes/,/}/{/int i/d}" -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode_lex.c
    sed "/Setup the input buffer state to scan the given bytes/,/}/{/for ( i = 0; i < _yybytes_len; ++i )/d}" -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode_lex.c
    sed "/Setup the input buffer state to scan the given bytes/,/}/{s/\tbuf\[i\] = yybytes\[i\]/memcpy(buf, yybytes, _yybytes_len)/g}" -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode_lex.c
    sed "/obpl_oracle_gbk_yylex_init is special because it creates the scanner itself/,/Initialization is the same as for the non-reentrant scanner/{s/return 1/return errno/g}" -i ../../../src/pl/parser/pl_parser_oracle_gbk_mode_lex.c
    cat ../../../close_modules/oracle_pl/pl/parser/pl_non_reserved_keywords_oracle_mode.c > ../../../src/pl/parser/pl_non_reserved_keywords_oracle_gbk_mode.c
    sed 's/#include "pl\/parser\/pl_parser_oracle_mode_tab.h"/#include "pl\/parser\/pl_parser_oracle_gbk_mode_tab.h"/g' -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_gbk_mode.c
    sed  "s/non_reserved_keywords_oracle_mode.c is for …/pl_non_reserved_keywords_oracle_gbk_mode.c is auto generated by gen_parser.sh/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_gbk_mode.c
    sed  "s/oracle_pl_non_reserved_keyword_lookup/oracle_pl_gbk_non_reserved_keyword_lookup/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_gbk_mode.c
    sed  "s/create_oracle_pl_trie_tree/create_oracle_pl_gbk_trie_tree/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_gbk_mode.c
    sed  "s/oracle_pl_reserved_keyword_lookup/oracle_pl_gbk_reserved_keyword_lookup/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_gbk_mode.c
    sed  "s/create_oracle_pl_reserved_trie_tree/create_oracle_pl_gbk_reserved_trie_tree/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_gbk_mode.c
    sed  "s/oracle_pl_keyword_lookup/oracle_pl_gbk_keyword_lookup/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_gbk_mode.c
    sed  "s/get_oracle_pl_reserved_keywords/get_oracle_pl_gbk_reserved_keywords/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_gbk_mode.c
    sed  "s/get_oracle_pl_non_reserved_keywords/get_oracle_pl_gbk_non_reserved_keywords/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_gbk_mode.c
    sed  "s/init_oracle_pl_keywords_tree/init_oracle_pl_gbk_keywords_tree/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_gbk_mode.c
    ##6.clean useless files
    rm -f ../../../src/pl/parser/pl_parser_oracle_gbk_mode.txt
    rm -f ../../../src/pl/parser/pl_parser_oracle_gbk_mode.l
    rm -f ../../../src/pl/parser/pl_parser_oracle_gbk_mode.y

    # generate oracle hkscs pl_parser
    ##1.copy lex and yacc files
    cat ../../../src/pl/parser/pl_parser_oracle_mode.y > ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.y
    cat ../../../src/pl/parser/pl_parser_oracle_mode.l > ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.l
    ##2.replace name
    sed  "s/obpl_oracle_yy/obpl_oracle_hkscs_yy/g" -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.y
    sed  "s/obpl_oracle_yy/obpl_oracle_hkscs_yy/g" -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.l
    sed  "s/pl_parser_oracle_mode/pl_parser_oracle_hkscs_mode/g" -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.y
    sed  "s/pl_parser_oracle_mode/pl_parser_oracle_hkscs_mode/g" -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.l
    sed  "s/obpl_oracle_parse_fatal_error/obpl_oracle_hkscs_parse_fatal_error/g" -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.y
    sed  "s/obpl_oracle_parse_fatal_error/obpl_oracle_hkscs_parse_fatal_error/g" -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.l
    sed  "s/obpl_oracle_fast_parse/obpl_oracle_hkscs_fast_parse/g" -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.y
    sed  "s/obpl_oracle_multi_fast_parse/obpl_oracle_hkscs_multi_fast_parse/g" -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.y
    sed  "s/obpl_oracle_multi_values_parse/obpl_oracle_hkscs_multi_values_parse/g" -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.y
    sed  "s/obpl_oracle_read_sql_construct/obpl_oracle_hkscs_read_sql_construct/g" -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.y
    ##3.add multi_byte_space、multi_byte_comma、multi_byte_left_parenthesis、multi_byte_right_parenthesis code.
    sed  "s/multi_byte_space              \[\\\u3000\]/multi_byte_space              ([\\\xa1][\\\x40])/g" -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.l
    sed  "s/multi_byte_comma              \[\\\uff0c\]/multi_byte_comma              ([\\\xa1][\\\x41])/g" -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.l
    sed  "s/multi_byte_left_parenthesis   \[\\\uff08\]/multi_byte_left_parenthesis   ([\\\xa1][\\\x5d])/g" -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.l
    sed  "s/multi_byte_right_parenthesis  \[\\\uff09\]/multi_byte_right_parenthesis  ([\\\xa1][\\\x5e])/g" -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.l
    echo "HK_1   [\x81-\xfe]
HK_1_1 [\x81-\xa0]
HK_1_2 [\xa1]
HK_1_3 [\xa2-\xfe]
HK_2fb   [\x40-\x7e]
HK_2fb_1 [\x42-\x5c]
HK_2fb_2 [\x5f-\xa1]
HK_2sb   [\xa1-\xfe]
g_except_space_comma_parenthesis ({HK_1_2}{HK_2fb_1}|{HK_1_2}{HK_2fb_2})
HK_CHAR ({HK_1_1}{HK_2fb}|{HK_1_1}{HK_2sb}|{g_except_space_comma_parenthesis}|{HK_1_2}{HK_2sb}|{HK_1_3}{HK_2fb}|{HK_1_3}{HK_2sb})" > ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.txt
    sed '/following character status will be rewrite by gen_parse.sh according to connection character/d' -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.l
    sed '/multi_byte_connect_char       \/\*According to connection character to set by gen_parse.sh\*\//r ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.txt' -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.l
    sed '/multi_byte_connect_char       \/\*According to connection character to set by gen_parse.sh\*\//d' -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.l
    sed 's/space            \[ \\t\\n\\r\\f\]/space            (\[ \\t\\n\\r\\f\]|{multi_byte_space})/g' -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.l
    sed 's/multi_byte_connect_char/HK_CHAR/g' -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.l
    sed  "s/oracle_pl_non_reserved_keyword_lookup/oracle_pl_hkscs_non_reserved_keyword_lookup/g" -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.l
    sed  "s/oracle_pl_keyword_lookup/oracle_pl_hkscs_keyword_lookup/g" -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.l
    ##4.generate oracle hkscs parser files
    bison_parser ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.y ../../../src/pl/parser/pl_parser_oracle_hkscs_mode_tab.c
    flex -o ../../../src/pl/parser/pl_parser_oracle_hkscs_mode_lex.c ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.l ../../../src/pl/parser/pl_parser_oracle_hkscs_mode_tab.h
    ##5.replace other info
    sed "/Setup the input buffer state to scan the given bytes/,/}/{/int i/d}" -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode_lex.c
    sed "/Setup the input buffer state to scan the given bytes/,/}/{/for ( i = 0; i < _yybytes_len; ++i )/d}" -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode_lex.c
    sed "/Setup the input buffer state to scan the given bytes/,/}/{s/\tbuf\[i\] = yybytes\[i\]/memcpy(buf, yybytes, _yybytes_len)/g}" -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode_lex.c
    sed "/obpl_oracle_hkscs_yylex_init is special because it creates the scanner itself/,/Initialization is the same as for the non-reentrant scanner/{s/return 1/return errno/g}" -i ../../../src/pl/parser/pl_parser_oracle_hkscs_mode_lex.c
    cat ../../../close_modules/oracle_pl/pl/parser/pl_non_reserved_keywords_oracle_mode.c > ../../../src/pl/parser/pl_non_reserved_keywords_oracle_hkscs_mode.c
    sed 's/#include "pl\/parser\/pl_parser_oracle_mode_tab.h"/#include "pl\/parser\/pl_parser_oracle_hkscs_mode_tab.h"/g' -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_hkscs_mode.c
    sed  "s/non_reserved_keywords_oracle_mode.c is for …/pl_non_reserved_keywords_oracle_hkscs_mode.c is auto generated by gen_parser.sh/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_hkscs_mode.c
    sed  "s/oracle_pl_non_reserved_keyword_lookup/oracle_pl_hkscs_non_reserved_keyword_lookup/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_hkscs_mode.c
    sed  "s/create_oracle_pl_trie_tree/create_oracle_pl_hkscs_trie_tree/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_hkscs_mode.c
    sed  "s/oracle_pl_reserved_keyword_lookup/oracle_pl_hkscs_reserved_keyword_lookup/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_hkscs_mode.c
    sed  "s/create_oracle_pl_reserved_trie_tree/create_oracle_pl_hkscs_reserved_trie_tree/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_hkscs_mode.c
    sed  "s/oracle_pl_keyword_lookup/oracle_pl_hkscs_keyword_lookup/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_hkscs_mode.c
    sed  "s/get_oracle_pl_reserved_keywords/get_oracle_pl_hkscs_reserved_keywords/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_hkscs_mode.c
    sed  "s/get_oracle_pl_non_reserved_keywords/get_oracle_pl_hkscs_non_reserved_keywords/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_hkscs_mode.c
    sed  "s/init_oracle_pl_keywords_tree/init_oracle_pl_hkscs_keywords_tree/g" -i ../../../src/pl/parser/pl_non_reserved_keywords_oracle_hkscs_mode.c
    ##6.clean useless files
    rm -f ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.txt
    rm -f ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.l
    rm -f ../../../src/pl/parser/pl_parser_oracle_hkscs_mode.y

    rm -rf ../../../src/pl/parser/pl_parser_oracle_mode.y
    rm -rf ../../../src/pl/parser/pl_parser_oracle_mode.l
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