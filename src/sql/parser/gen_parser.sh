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
sed "/obsql_mysql_yylex_init is special because it creates the scanner itself/,/Initialization is the same as for the non-reentrant scanner/{s/return 1/return errno/g}" -i ../../../src/sql/parser/sql_parser_mysql_mode_lex.c


if [ -d "../../../close_modules/oracle_parser/sql/parser" ]; then

ln -sf ../../../close_modules/oracle_parser/sql/parser/sql_parser_oracle_mode.y ../../../src/sql/parser/sql_parser_oracle_mode.y
ln -sf ../../../close_modules/oracle_parser/sql/parser/sql_parser_oracle_mode.l ../../../src/sql/parser/sql_parser_oracle_mode.l

# generate oracle latin1 sql_parser(do not support multi_byte_space、multi_byte_comma、multi_byte_left_parenthesis、multi_byte_right_parenthesis)
##1.copy lex and yacc files
cat ../../../src/sql/parser/sql_parser_oracle_mode.y > ../../../src/sql/parser/sql_parser_oracle_latin1_mode.y
cat ../../../src/sql/parser/sql_parser_oracle_mode.l > ../../../src/sql/parser/sql_parser_oracle_latin1_mode.l
##2.replace name
sed  "s/obsql_oracle_yy/obsql_oracle_latin1_yy/g" -i ../../../src/sql/parser/sql_parser_oracle_latin1_mode.y
sed  "s/obsql_oracle_yy/obsql_oracle_latin1_yy/g" -i ../../../src/sql/parser/sql_parser_oracle_latin1_mode.l
sed  "s/sql_parser_oracle_mode/sql_parser_oracle_latin1_mode/g" -i ../../../src/sql/parser/sql_parser_oracle_latin1_mode.y
sed  "s/sql_parser_oracle_mode/sql_parser_oracle_latin1_mode/g" -i ../../../src/sql/parser/sql_parser_oracle_latin1_mode.l
sed  "s/obsql_oracle_parser_fatal_error/obsql_oracle_latin1_parser_fatal_error/g" -i ../../../src/sql/parser/sql_parser_oracle_latin1_mode.y
sed  "s/obsql_oracle_parser_fatal_error/obsql_oracle_latin1_parser_fatal_error/g" -i ../../../src/sql/parser/sql_parser_oracle_latin1_mode.l
sed  "s/obsql_oracle_fast_parse/obsql_oracle_latin1_fast_parse/g" -i ../../../src/sql/parser/sql_parser_oracle_latin1_mode.y
sed  "s/obsql_oracle_multi_fast_parse/obsql_oracle_latin1_multi_fast_parse/g" -i ../../../src/sql/parser/sql_parser_oracle_latin1_mode.y
sed  "s/obsql_oracle_multi_values_parse/obsql_oracle_latin1_multi_values_parse/g" -i ../../../src/sql/parser/sql_parser_oracle_latin1_mode.y
##3.do not need to replace multi_byte_space、multi_byte_comma、multi_byte_left_parenthesis、multi_byte_right_parenthesis code
sed  "s/multi_byte_space              \[\\\u3000\]/multi_byte_space              \[\\\x20]/g" -i ../../../src/sql/parser/sql_parser_oracle_latin1_mode.l
sed  "s/multi_byte_comma              \[\\\uff0c\]/multi_byte_comma              \[\\\x2c]/g" -i ../../../src/sql/parser/sql_parser_oracle_latin1_mode.l
sed  "s/multi_byte_left_parenthesis   \[\\\uff08\]/multi_byte_left_parenthesis   \[\\\x28]/g" -i ../../../src/sql/parser/sql_parser_oracle_latin1_mode.l
sed  "s/multi_byte_right_parenthesis  \[\\\uff09\]/multi_byte_right_parenthesis  \[\\\x29]/g" -i ../../../src/sql/parser/sql_parser_oracle_latin1_mode.l
echo "LATIN1_CHAR [\x80-\xFF]" > ../../../src/sql/parser/latin1.txt
sed '/following character status will be rewrite by gen_parse.sh according to connection character/d' -i ../../../src/sql/parser/sql_parser_oracle_latin1_mode.l
sed '/multi_byte_connect_char       \/\*According to connection character to set by gen_parse.sh\*\//r ../../../src/sql/parser/latin1.txt' -i ../../../src/sql/parser/sql_parser_oracle_latin1_mode.l
sed '/multi_byte_connect_char       \/\*According to connection character to set by gen_parse.sh\*\//d' -i ../../../src/sql/parser/sql_parser_oracle_latin1_mode.l
sed 's/multi_byte_connect_char/LATIN1_CHAR/g' -i ../../../src/sql/parser/sql_parser_oracle_latin1_mode.l
sed -i '/<hint>{multi_byte_space}/,+5d' sql_parser_oracle_latin1_mode.l
sed -i '/<hint>{multi_byte_comma}/,+35d' sql_parser_oracle_latin1_mode.l
sed -i '/{multi_byte_comma}/,+23d' sql_parser_oracle_latin1_mode.l
sed -i '/{multi_byte_space}/,+4d' sql_parser_oracle_latin1_mode.l
##4.generate oracle latin1 parser files
bison_parser ../../../src/sql/parser/sql_parser_oracle_latin1_mode.y ../../../src/sql/parser/sql_parser_oracle_latin1_mode_tab.c
flex -o ../../../src/sql/parser/sql_parser_oracle_latin1_mode_lex.c ../../../src/sql/parser/sql_parser_oracle_latin1_mode.l ../../../src/sql/parser/sql_parser_oracle_latin1_mode_tab.h
##5.replace other info
sed "/Setup the input buffer state to scan the given bytes/,/}/{/int i/d}" -i ../../../src/sql/parser/sql_parser_oracle_latin1_mode_lex.c
sed "/Setup the input buffer state to scan the given bytes/,/}/{/for ( i = 0; i < _yybytes_len; ++i )/d}" -i ../../../src/sql/parser/sql_parser_oracle_latin1_mode_lex.c
sed "/Setup the input buffer state to scan the given bytes/,/}/{s/\tbuf\[i\] = yybytes\[i\]/memcpy(buf, yybytes, _yybytes_len)/g}" -i ../../../src/sql/parser/sql_parser_oracle_latin1_mode_lex.c
sed "/obsql_oracle_latin1_yylex_init is special because it creates the scanner itself/,/Initialization is the same as for the non-reentrant scanner/{s/return 1/return errno/g}" -i ../../../src/sql/parser/sql_parser_oracle_latin1_mode_lex.c
cat ../../../src/sql/parser/non_reserved_keywords_oracle_mode.c > ../../../src/sql/parser/non_reserved_keywords_oracle_latin1_mode.c
sed '/#include "ob_non_reserved_keywords.h"/a\#include "sql/parser/sql_parser_oracle_latin1_mode_tab.h\"'  -i ../../../src/sql/parser/non_reserved_keywords_oracle_latin1_mode.c
sed  "s/non_reserved_keywords_oracle_mode.c is for …/non_reserved_keywords_oracle_latin1_mode.c is auto generated by gen_parser.sh/g" -i ../../../src/sql/parser/non_reserved_keywords_oracle_latin1_mode.c
##6.clean useless files
rm -f ../../../src/sql/parser/latin1.txt
rm -f ../../../src/sql/parser/sql_parser_oracle_latin1_mode.l
rm -f ../../../src/sql/parser/sql_parser_oracle_latin1_mode.y

# generate oracle utf8 sql_parser(support multi_byte_space、multi_byte_comma、multi_byte_left_parenthesis、multi_byte_right_parenthesis)
##1.copy lex and yacc files
cat ../../../src/sql/parser/sql_parser_oracle_mode.y > ../../../src/sql/parser/sql_parser_oracle_utf8_mode.y
cat ../../../src/sql/parser/sql_parser_oracle_mode.l > ../../../src/sql/parser/sql_parser_oracle_utf8_mode.l
##2.replace name
sed  "s/obsql_oracle_yy/obsql_oracle_utf8_yy/g" -i ../../../src/sql/parser/sql_parser_oracle_utf8_mode.y
sed  "s/obsql_oracle_yy/obsql_oracle_utf8_yy/g" -i ../../../src/sql/parser/sql_parser_oracle_utf8_mode.l
sed  "s/sql_parser_oracle_mode/sql_parser_oracle_utf8_mode/g" -i ../../../src/sql/parser/sql_parser_oracle_utf8_mode.y
sed  "s/sql_parser_oracle_mode/sql_parser_oracle_utf8_mode/g" -i ../../../src/sql/parser/sql_parser_oracle_utf8_mode.l
sed  "s/obsql_oracle_parser_fatal_error/obsql_oracle_utf8_parser_fatal_error/g" -i ../../../src/sql/parser/sql_parser_oracle_utf8_mode.y
sed  "s/obsql_oracle_parser_fatal_error/obsql_oracle_utf8_parser_fatal_error/g" -i ../../../src/sql/parser/sql_parser_oracle_utf8_mode.l
sed  "s/obsql_oracle_fast_parse/obsql_oracle_utf8_fast_parse/g" -i ../../../src/sql/parser/sql_parser_oracle_utf8_mode.y
sed  "s/obsql_oracle_multi_fast_parse/obsql_oracle_utf8_multi_fast_parse/g" -i ../../../src/sql/parser/sql_parser_oracle_utf8_mode.y
sed  "s/obsql_oracle_multi_values_parse/obsql_oracle_utf8_multi_values_parse/g" -i ../../../src/sql/parser/sql_parser_oracle_utf8_mode.y
##3.add multi_byte_space、multi_byte_comma、multi_byte_left_parenthesis、multi_byte_right_parenthesis code.
sed  "s/multi_byte_space              \[\\\u3000\]/multi_byte_space              ([\\\xe3\][\\\x80\][\\\x80])/g" -i ../../../src/sql/parser/sql_parser_oracle_utf8_mode.l
sed  "s/multi_byte_comma              \[\\\uff0c\]/multi_byte_comma              ([\\\xef\][\\\xbc\][\\\x8c])/g" -i ../../../src/sql/parser/sql_parser_oracle_utf8_mode.l
sed  "s/multi_byte_left_parenthesis   \[\\\uff08\]/multi_byte_left_parenthesis   ([\\\xef\][\\\xbc\][\\\x88])/g" -i ../../../src/sql/parser/sql_parser_oracle_utf8_mode.l
sed  "s/multi_byte_right_parenthesis  \[\\\uff09\]/multi_byte_right_parenthesis  ([\\\xef\][\\\xbc\][\\\x89])/g" -i ../../../src/sql/parser/sql_parser_oracle_utf8_mode.l
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
UTF8_CHAR ({U_2}{U}|{U_3}{U}{U}|{u_except_space}|{U_3_2}{U}{U}|{u_except_comma_parenthesis}|{U_4}{U}{U}{U})" > ../../../src/sql/parser/utf8.txt
sed '/following character status will be rewrite by gen_parse.sh according to connection character/d' -i ../../../src/sql/parser/sql_parser_oracle_utf8_mode.l
sed '/multi_byte_connect_char       \/\*According to connection character to set by gen_parse.sh\*\//r ../../../src/sql/parser/utf8.txt' -i ../../../src/sql/parser/sql_parser_oracle_utf8_mode.l
sed '/multi_byte_connect_char       \/\*According to connection character to set by gen_parse.sh\*\//d' -i ../../../src/sql/parser/sql_parser_oracle_utf8_mode.l
sed 's/space            \[ \\t\\n\\r\\f\]/space            (\[ \\t\\n\\r\\f\]|{multi_byte_space})/g' -i ../../../src/sql/parser/sql_parser_oracle_utf8_mode.l
sed 's/multi_byte_connect_char/UTF8_CHAR/g' -i ../../../src/sql/parser/sql_parser_oracle_utf8_mode.l
##4.generate oracle utf8 parser files
bison_parser ../../../src/sql/parser/sql_parser_oracle_utf8_mode.y ../../../src/sql/parser/sql_parser_oracle_utf8_mode_tab.c
flex -o ../../../src/sql/parser/sql_parser_oracle_utf8_mode_lex.c ../../../src/sql/parser/sql_parser_oracle_utf8_mode.l ../../../src/sql/parser/sql_parser_oracle_utf8_mode_tab.h
##5.replace other info
sed "/Setup the input buffer state to scan the given bytes/,/}/{/int i/d}" -i ../../../src/sql/parser/sql_parser_oracle_utf8_mode_lex.c
sed "/Setup the input buffer state to scan the given bytes/,/}/{/for ( i = 0; i < _yybytes_len; ++i )/d}" -i ../../../src/sql/parser/sql_parser_oracle_utf8_mode_lex.c
sed "/Setup the input buffer state to scan the given bytes/,/}/{s/\tbuf\[i\] = yybytes\[i\]/memcpy(buf, yybytes, _yybytes_len)/g}" -i ../../../src/sql/parser/sql_parser_oracle_utf8_mode_lex.c
sed "/obsql_oracle_utf8_yylex_init is special because it creates the scanner itself/,/Initialization is the same as for the non-reentrant scanner/{s/return 1/return errno/g}" -i ../../../src/sql/parser/sql_parser_oracle_utf8_mode_lex.c
cat ../../../src/sql/parser/non_reserved_keywords_oracle_mode.c > ../../../src/sql/parser/non_reserved_keywords_oracle_utf8_mode.c
sed '/#include "ob_non_reserved_keywords.h"/a\#include "sql/parser/sql_parser_oracle_utf8_mode_tab.h\"'  -i ../../../src/sql/parser/non_reserved_keywords_oracle_utf8_mode.c
sed  "s/non_reserved_keywords_oracle_mode.c is for …/non_reserved_keywords_oracle_utf8_mode.c is auto generated by gen_parser.sh/g" -i ../../../src/sql/parser/non_reserved_keywords_oracle_utf8_mode.c
##6.clean useless files
rm -f ../../../src/sql/parser/utf8.txt
rm -f ../../../src/sql/parser/sql_parser_oracle_utf8_mode.l
rm -f ../../../src/sql/parser/sql_parser_oracle_utf8_mode.y

# generate oracle gbk sql_parser(support multi_byte_space、multi_byte_comma、multi_byte_left_parenthesis、multi_byte_right_parenthesis)
##1.copy lex and yacc files
cat ../../../src/sql/parser/sql_parser_oracle_mode.y > ../../../src/sql/parser/sql_parser_oracle_gbk_mode.y
cat ../../../src/sql/parser/sql_parser_oracle_mode.l > ../../../src/sql/parser/sql_parser_oracle_gbk_mode.l
##2.replace name
sed  "s/obsql_oracle_yy/obsql_oracle_gbk_yy/g" -i ../../../src/sql/parser/sql_parser_oracle_gbk_mode.y
sed  "s/obsql_oracle_yy/obsql_oracle_gbk_yy/g" -i ../../../src/sql/parser/sql_parser_oracle_gbk_mode.l
sed  "s/sql_parser_oracle_mode/sql_parser_oracle_gbk_mode/g" -i ../../../src/sql/parser/sql_parser_oracle_gbk_mode.y
sed  "s/sql_parser_oracle_mode/sql_parser_oracle_gbk_mode/g" -i ../../../src/sql/parser/sql_parser_oracle_gbk_mode.l
sed  "s/obsql_oracle_parser_fatal_error/obsql_oracle_gbk_parser_fatal_error/g" -i ../../../src/sql/parser/sql_parser_oracle_gbk_mode.y
sed  "s/obsql_oracle_parser_fatal_error/obsql_oracle_gbk_parser_fatal_error/g" -i ../../../src/sql/parser/sql_parser_oracle_gbk_mode.l
sed  "s/obsql_oracle_fast_parse/obsql_oracle_gbk_fast_parse/g" -i ../../../src/sql/parser/sql_parser_oracle_gbk_mode.y
sed  "s/obsql_oracle_multi_fast_parse/obsql_oracle_gbk_multi_fast_parse/g" -i ../../../src/sql/parser/sql_parser_oracle_gbk_mode.y
sed  "s/obsql_oracle_multi_values_parse/obsql_oracle_gbk_multi_values_parse/g" -i ../../../src/sql/parser/sql_parser_oracle_gbk_mode.y
##3.add multi_byte_space、multi_byte_comma、multi_byte_left_parenthesis、multi_byte_right_parenthesis code.
sed  "s/multi_byte_space              \[\\\u3000\]/multi_byte_space              ([\\\xa1][\\\xa1])/g" -i ../../../src/sql/parser/sql_parser_oracle_gbk_mode.l
sed  "s/multi_byte_comma              \[\\\uff0c\]/multi_byte_comma              ([\\\xa3][\\\xac])/g" -i ../../../src/sql/parser/sql_parser_oracle_gbk_mode.l
sed  "s/multi_byte_left_parenthesis   \[\\\uff08\]/multi_byte_left_parenthesis   ([\\\xa3][\\\xa8])/g" -i ../../../src/sql/parser/sql_parser_oracle_gbk_mode.l
sed  "s/multi_byte_right_parenthesis  \[\\\uff09\]/multi_byte_right_parenthesis  ([\\\xa3][\\\xa9])/g" -i ../../../src/sql/parser/sql_parser_oracle_gbk_mode.l
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
GB_CHAR ({GB_1_1}{GB_2}|{g_except_space}|{GB_1_3}{GB_2}|{g_except_comma_parenthesis}|{GB_1_5}{GB_2}|{GB_1}{GB_3}{GB_1}{GB_3})" > ../../../src/sql/parser/gbk.txt
sed '/following character status will be rewrite by gen_parse.sh according to connection character/d' -i ../../../src/sql/parser/sql_parser_oracle_gbk_mode.l
sed '/multi_byte_connect_char       \/\*According to connection character to set by gen_parse.sh\*\//r ../../../src/sql/parser/gbk.txt' -i ../../../src/sql/parser/sql_parser_oracle_gbk_mode.l
sed '/multi_byte_connect_char       \/\*According to connection character to set by gen_parse.sh\*\//d' -i ../../../src/sql/parser/sql_parser_oracle_gbk_mode.l
sed 's/space            \[ \\t\\n\\r\\f\]/space            (\[ \\t\\n\\r\\f\]|{multi_byte_space})/g' -i ../../../src/sql/parser/sql_parser_oracle_gbk_mode.l
sed 's/multi_byte_connect_char/GB_CHAR/g' -i ../../../src/sql/parser/sql_parser_oracle_gbk_mode.l
##4.generate oracle gbk parser files
bison_parser ../../../src/sql/parser/sql_parser_oracle_gbk_mode.y ../../../src/sql/parser/sql_parser_oracle_gbk_mode_tab.c
flex -o ../../../src/sql/parser/sql_parser_oracle_gbk_mode_lex.c ../../../src/sql/parser/sql_parser_oracle_gbk_mode.l ../../../src/sql/parser/sql_parser_oracle_gbk_mode_tab.h
##5.replace other info
sed "/Setup the input buffer state to scan the given bytes/,/}/{/int i/d}" -i ../../../src/sql/parser/sql_parser_oracle_gbk_mode_lex.c
sed "/Setup the input buffer state to scan the given bytes/,/}/{/for ( i = 0; i < _yybytes_len; ++i )/d}" -i ../../../src/sql/parser/sql_parser_oracle_gbk_mode_lex.c
sed "/Setup the input buffer state to scan the given bytes/,/}/{s/\tbuf\[i\] = yybytes\[i\]/memcpy(buf, yybytes, _yybytes_len)/g}" -i ../../../src/sql/parser/sql_parser_oracle_gbk_mode_lex.c
sed "/obsql_oracle_gbk_yylex_init is special because it creates the scanner itself/,/Initialization is the same as for the non-reentrant scanner/{s/return 1/return errno/g}" -i ../../../src/sql/parser/sql_parser_oracle_gbk_mode_lex.c
cat ../../../src/sql/parser/non_reserved_keywords_oracle_mode.c > ../../../src/sql/parser/non_reserved_keywords_oracle_gbk_mode.c
sed '/#include "ob_non_reserved_keywords.h"/a\#include "sql/parser/sql_parser_oracle_gbk_mode_tab.h\"'  -i ../../../src/sql/parser/non_reserved_keywords_oracle_gbk_mode.c
sed  "s/non_reserved_keywords_oracle_mode.c is for …/non_reserved_keywords_oracle_gbk_mode.c is auto generated by gen_parser.sh/g" -i ../../../src/sql/parser/non_reserved_keywords_oracle_gbk_mode.c
##6.clean useless files
rm -f ../../../src/sql/parser/gbk.txt
rm -f ../../../src/sql/parser/sql_parser_oracle_gbk_mode.l
rm -f ../../../src/sql/parser/sql_parser_oracle_gbk_mode.y

rm -rf ../../../src/sql/parser/sql_parser_oracle_mode.y
rm -rf ../../../src/sql/parser/sql_parser_oracle_mode.l

fi

# generate type name
./gen_type_name.sh ../../../src/objit/include/objit/common/ob_item_type.h > type_name.c
