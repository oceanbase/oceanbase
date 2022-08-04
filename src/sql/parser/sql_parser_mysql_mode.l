/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

/* unput() change the yyin but it doesn't change ParserResult->input_sql_.
// use unput() function may have unexpected result while copy string.
*/
%option noyywrap nounput noinput nodefault case-insensitive
%option noyyalloc noyyrealloc noyyfree
%option reentrant bison-bridge bison-locations
%option prefix="obsql_mysql_yy"
%option header-file="../../../src/sql/parser/sql_parser_mysql_mode_lex.h"
%{
#include "sql_parser_base.h"
#include "sql_parser_mysql_mode_tab.h"

extern void obsql_mysql_yyerror(YYLTYPE *yylloc, ParseResult *p, char *s,...);
extern void obsql_mysql_parser_fatal_error(yyconst char *msg, yyscan_t yyscanner);
#define yyerror obsql_mysql_yyerror
#define YY_FATAL_ERROR(msg) obsql_mysql_parser_fatal_error(msg , yyscanner)
%}

%x hint
%x in_c_comment
%x log_level
%x sq
%x dq
%x bt
/* the adq is used to process dq in ANSI_QUOTES sql_mode*/
%x adq

U  [\x80-\xbf]
U_2  [\xc2-\xdf]
U_3  [\xe0-\xef]
U_4  [\xf0-\xf4]
GB_1 [\x81-\xfe]
GB_2 [\x40-\xfe]
GB_3 [\x30-\x39]
UTF8_GB_CHAR ({U_2}{U}|{U_3}{U}{U}|{U_4}{U}{U}{U}|{GB_1}{GB_2}|{GB_1}{GB_3}{GB_1}{GB_3})
space            [ \t\n\r\f]
non_newline      [^\n\r]
sql_comment      ("--"{space}+{non_newline}*)|(#{non_newline}*)
whitespace       ({space}+|{sql_comment})
select_hint_begin       (select{space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))
update_hint_begin       (update{space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))
delete_hint_begin       (delete{space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))
insert_hint_begin       (insert{space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))
replace_hint_begin      (replace{space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))
load_data_hint_begin    (load{space}+data{space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))
hint_hint_begin      (hint{space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))
common_hint_begin (\/\*\+({space}*hint{space}+)?)
c_cmt_begin      \/\*
c_cmt_end        \*+\/
comment          ({sql_comment})
identifier        (([A-Za-z0-9$_]|{UTF8_GB_CHAR})*)
log_level_stmt   ([A-Za-z][A-Za-z0-9_,\.:\* ]*)
system_variable  (@@[A-Za-z_][A-Za-z0-9_]*)
user_variable    (@[A-Za-z0-9_\.$]*)|(@[`'\"][`'\"A-Za-z0-9_\.$/%]*)
version_num      ([0-9]+\.+[0-9]*)
int_num          [0-9]+
client_version   \([0-9]+(\.[0-9]+)*\)

quote         '
sqbegin       {quote}
sqend         {quote}
sqdouble      {quote}{quote}
sqcontent     [^\\\n\r']+
qescape       [\\](.|\n)
sqnewline     {quote}{whitespace}{quote}

dquote         \"
dqbegin       {dquote}
dqend         {dquote}
dqdouble      {dquote}{dquote}
dqcontent     [^\\\n\r"]+
adqcontent    [^"]+
dqnewline     {dquote}{whitespace}{dquote}

backtick      `
btbegin       {backtick}
btend         {backtick}
btdouble      {backtick}{backtick}
btcontent     [^`]+

mysql_compatible_comment_with_version \/\*\![0-9]{5}
mysql_compatible_comment_without_version \/\*\!
mysql_compatible_comment_end \*\/

rowidPattern  (WITH{whitespace}ROWID)

%%
ACCESS                        { REPUT_TOKEN_NEG_SIGN(ACCESS); }
ACCESSIBLE                    { REPUT_TOKEN_NEG_SIGN(ACCESSIBLE); }
ADD                           { REPUT_TOKEN_NEG_SIGN(ADD); }
AGAINST                       { REPUT_TOKEN_NEG_SIGN(AGAINST); }
ALTER                         { REPUT_TOKEN_NEG_SIGN(ALTER); }
ALWAYS                        { REPUT_TOKEN_NEG_SIGN(ALWAYS); }
AND                           { REPUT_TOKEN_NEG_SIGN(AND); }
ALL                           { REPUT_TOKEN_NEG_SIGN(ALL); }
AS                            { REPUT_TOKEN_NEG_SIGN(AS); }
ASENSITIVE                    { REPUT_TOKEN_NEG_SIGN(ASENSITIVE); }
ASC                           { REPUT_TOKEN_NEG_SIGN(ASC); }
BETWEEN                       { REPUT_TOKEN_NEG_SIGN(BETWEEN); }
BEFORE                        { REPUT_TOKEN_NEG_SIGN(BEFORE); }
BIGINT                        { REPUT_TOKEN_NEG_SIGN(BIGINT); }
BINARY                        { REPUT_TOKEN_NEG_SIGN(BINARY); }
BLOB                          { REPUT_TOKEN_NEG_SIGN(BLOB); }
BOTH                          { REPUT_TOKEN_NEG_SIGN(BOTH); }
BY                            { REPUT_TOKEN_NEG_SIGN(BY); }
CALL                          { REPUT_TOKEN_NEG_SIGN(CALL); }
CASCADE                       { REPUT_TOKEN_NEG_SIGN(CASCADE); }
CASE                          { REPUT_TOKEN_NEG_SIGN(CASE); }
CHANGE                        { REPUT_TOKEN_NEG_SIGN(CHANGE); }
CHAR                          { REPUT_TOKEN_NEG_SIGN(CHARACTER); }
CHARACTER                     { REPUT_TOKEN_NEG_SIGN(CHARACTER); }
CHECK                         { REPUT_TOKEN_NEG_SIGN(CHECK); }
CIPHER                        { REPUT_TOKEN_NEG_SIGN(CIPHER); }
CONDITION                     { REPUT_TOKEN_NEG_SIGN(CONDITION); }
CONSTRAINT                    { REPUT_TOKEN_NEG_SIGN(CONSTRAINT); }
CONTINUE                      { REPUT_TOKEN_NEG_SIGN(CONTINUE); }
CONVERT                       { REPUT_TOKEN_NEG_SIGN(CONVERT); }
COLLATE                       { REPUT_TOKEN_NEG_SIGN(COLLATE); }
COLUMN                        { REPUT_TOKEN_NEG_SIGN(COLUMN); }
COLUMNS                       { REPUT_TOKEN_NEG_SIGN(COLUMNS); }
CREATE                        { REPUT_TOKEN_NEG_SIGN(CREATE); }
CROSS                         { REPUT_TOKEN_NEG_SIGN(CROSS); }
CYCLE                         { REPUT_TOKEN_NEG_SIGN(CYCLE); }
CURRENT_DATE                  { REPUT_TOKEN_NEG_SIGN(CURRENT_DATE); }
CURRENT_TIME                  { REPUT_TOKEN_NEG_SIGN(CURRENT_TIME); }
CURRENT_TIMESTAMP             { REPUT_TOKEN_NEG_SIGN(CURRENT_TIMESTAMP); }
CURRENT_USER                  { REPUT_TOKEN_NEG_SIGN(CURRENT_USER); }
{rowidPattern}                { REPUT_TOKEN_NEG_SIGN(WITH_ROWID); }
CURSOR                        { REPUT_TOKEN_NEG_SIGN(CURSOR); }
DAY_HOUR                      { REPUT_TOKEN_NEG_SIGN(DAY_HOUR); }
DAY_MICROSECOND               { REPUT_TOKEN_NEG_SIGN(DAY_MICROSECOND); }
DAY_MINUTE                    { REPUT_TOKEN_NEG_SIGN(DAY_MINUTE); }
DAY_SECOND                    { REPUT_TOKEN_NEG_SIGN(DAY_SECOND); }
DATABASE                      { REPUT_TOKEN_NEG_SIGN(DATABASE); }
DATABASES                     { REPUT_TOKEN_NEG_SIGN(DATABASES); }
DEC                           { REPUT_TOKEN_NEG_SIGN(NUMBER); }
DECIMAL                       { REPUT_TOKEN_NEG_SIGN(DECIMAL); }
DECLARE                       { REPUT_TOKEN_NEG_SIGN(DECLARE); }
DEFAULT                       { REPUT_TOKEN_NEG_SIGN(DEFAULT); }
DELAYED                       { REPUT_TOKEN_NEG_SIGN(DELAYED); }
DELETE                        { REPUT_TOKEN_NEG_SIGN(DELETE); }
DESC                          { REPUT_TOKEN_NEG_SIGN(DESC); }
DESCRIBE                      { REPUT_TOKEN_NEG_SIGN(DESCRIBE); }
DETERMINISTIC                 { REPUT_TOKEN_NEG_SIGN(DETERMINISTIC); }
DIV                           { REPUT_TOKEN_NEG_SIGN(DIV); }
DISTINCT                      { REPUT_TOKEN_NEG_SIGN(DISTINCT); }
DISTINCTROW                   { REPUT_TOKEN_NEG_SIGN(DISTINCT); }
DOUBLE                        { REPUT_TOKEN_NEG_SIGN(DOUBLE); }
DROP                          { REPUT_TOKEN_NEG_SIGN(DROP); }
DUAL                          { REPUT_TOKEN_NEG_SIGN(DUAL); }
EACH                          { REPUT_TOKEN_NEG_SIGN(EACH); }
ENCLOSED                      { REPUT_TOKEN_NEG_SIGN(ENCLOSED); }
ELSE                          { REPUT_TOKEN_NEG_SIGN(ELSE); }
ELSEIF                        { REPUT_TOKEN_NEG_SIGN(ELSEIF); }
ESCAPED                       { REPUT_TOKEN_NEG_SIGN(ESCAPED); }
EXISTS                        { REPUT_TOKEN_NEG_SIGN(EXISTS); }
EXIT                          { REPUT_TOKEN_NEG_SIGN(EXIT); }
EXPLAIN                       { REPUT_TOKEN_NEG_SIGN(EXPLAIN); }
FETCH                         { REPUT_TOKEN_NEG_SIGN(FETCH); }
FIELDS                        { REPUT_TOKEN_NEG_SIGN(FIELDS); }
FOREIGN                       { REPUT_TOKEN_NEG_SIGN(FOREIGN); }
FLOAT                         { REPUT_TOKEN_NEG_SIGN(FLOAT); }
FLOAT4                        { REPUT_TOKEN_NEG_SIGN(FLOAT4); }
FLOAT8                        { REPUT_TOKEN_NEG_SIGN(FLOAT8); }
FOR                           { REPUT_TOKEN_NEG_SIGN(FOR); }
FORCE                         { REPUT_TOKEN_NEG_SIGN(FORCE); }
FROM                          { REPUT_TOKEN_NEG_SIGN(FROM); }
FULL                          { REPUT_TOKEN_NEG_SIGN(FULL); }
GET                           { REPUT_TOKEN_NEG_SIGN(GET); }
GENERATED                     { REPUT_TOKEN_NEG_SIGN(GENERATED); }
GRANT                         { REPUT_TOKEN_NEG_SIGN(GRANT); }
GROUP                         { REPUT_TOKEN_NEG_SIGN(GROUP); }
HAVING                        { REPUT_TOKEN_NEG_SIGN(HAVING); }
HELP                          { REPUT_TOKEN_NEG_SIGN(HELP); }
HIGH_PRIORITY                 { REPUT_TOKEN_NEG_SIGN(HIGH_PRIORITY); }
HOUR_MICROSECOND              { REPUT_TOKEN_NEG_SIGN(HOUR_MICROSECOND); }
HOUR_MINUTE                   { REPUT_TOKEN_NEG_SIGN(HOUR_MINUTE); }
HOUR_SECOND                   { REPUT_TOKEN_NEG_SIGN(HOUR_SECOND); }
ID                            { REPUT_TOKEN_NEG_SIGN(ID); }
IF                            { REPUT_TOKEN_NEG_SIGN(IF); }
IN                            { REPUT_TOKEN_NEG_SIGN(IN); }
INDEX                         { REPUT_TOKEN_NEG_SIGN(INDEX); }
INNER                         { REPUT_TOKEN_NEG_SIGN(INNER); }
INFILE                        { REPUT_TOKEN_NEG_SIGN(INFILE); }
INOUT                         { REPUT_TOKEN_NEG_SIGN(INOUT); }
INSENSITIVE                   { REPUT_TOKEN_NEG_SIGN(INSENSITIVE); }
INT                           { REPUT_TOKEN_NEG_SIGN(INTEGER); }
INT1                          { REPUT_TOKEN_NEG_SIGN(INT1); }
INT2                          { REPUT_TOKEN_NEG_SIGN(INT2); }
INT3                          { REPUT_TOKEN_NEG_SIGN(INT3); }
INT4                          { REPUT_TOKEN_NEG_SIGN(INT4); }
INT8                          { REPUT_TOKEN_NEG_SIGN(INT8); }
INTEGER                       { REPUT_TOKEN_NEG_SIGN(INTEGER); }
INTERVAL { 
  ParseResult *p = (ParseResult *)yyextra;
  REPUT_NEG_SIGN(p); 
  return INTERVAL; 
}
INSERT                        { REPUT_TOKEN_NEG_SIGN(INSERT); }
INTO                          { REPUT_TOKEN_NEG_SIGN(INTO); }
IO_AFTER_GTIDS                { REPUT_TOKEN_NEG_SIGN(IO_AFTER_GTIDS); }
IO_BEFORE_GTIDS               { REPUT_TOKEN_NEG_SIGN(IO_BEFORE_GTIDS); }
IS                            { REPUT_TOKEN_NEG_SIGN(IS); }
ISSUER                        { REPUT_TOKEN_NEG_SIGN(ISSUER); }
ITERATE                       { REPUT_TOKEN_NEG_SIGN(ITERATE); }
JOIN                          { REPUT_TOKEN_NEG_SIGN(JOIN); }
KEY                           { REPUT_TOKEN_NEG_SIGN(KEY); }
KEYS                          { REPUT_TOKEN_NEG_SIGN(KEYS); }
KILL                          { REPUT_TOKEN_NEG_SIGN(KILL); }
LANGUAGE                      { REPUT_TOKEN_NEG_SIGN(LANGUAGE); }
LEADING                       { REPUT_TOKEN_NEG_SIGN(LEADING); }
LEAVE                         { REPUT_TOKEN_NEG_SIGN(LEAVE); }
LEFT                          { REPUT_TOKEN_NEG_SIGN(LEFT); }
LIMIT                         { REPUT_TOKEN_NEG_SIGN(LIMIT); }
LIKE                          { REPUT_TOKEN_NEG_SIGN(LIKE); }
LINEAR                        { REPUT_TOKEN_NEG_SIGN(LINEAR); }
LINES                         { REPUT_TOKEN_NEG_SIGN(LINES); }
LIST                          { REPUT_TOKEN_NEG_SIGN(BISON_LIST); }
LOAD                          { REPUT_TOKEN_NEG_SIGN(LOAD); }
LOCAL                         { REPUT_TOKEN_NEG_SIGN(LOCAL); }
LOCALTIME                     { REPUT_TOKEN_NEG_SIGN(LOCALTIME); }
LOCALTIMESTAMP                { REPUT_TOKEN_NEG_SIGN(LOCALTIMESTAMP); }
LOCK                          { REPUT_TOKEN_NEG_SIGN(LOCK_); }
LONG                          { REPUT_TOKEN_NEG_SIGN(LONG); }
LONGBLOB                      { REPUT_TOKEN_NEG_SIGN(LONGBLOB); }
LONGTEXT                      { REPUT_TOKEN_NEG_SIGN(LONGTEXT); }
LOOP                          { REPUT_TOKEN_NEG_SIGN(LOOP); }
LOW_PRIORITY                  { REPUT_TOKEN_NEG_SIGN(LOW_PRIORITY); }
MASTER_BIND                   { REPUT_TOKEN_NEG_SIGN(MASTER_BIND); }
MASTER_SSL_VERIFY_SERVER_CERT { REPUT_TOKEN_NEG_SIGN(MASTER_SSL_VERIFY_SERVER_CERT); }
MATCH                         { REPUT_TOKEN_NEG_SIGN(MATCH); }
MAXVALUE                      { REPUT_TOKEN_NEG_SIGN(MAXVALUE); }
MEDIUMBLOB                    { REPUT_TOKEN_NEG_SIGN(MEDIUMBLOB); }
MEDIUMINT                     { REPUT_TOKEN_NEG_SIGN(MEDIUMINT); }
MERGE                         { REPUT_TOKEN_NEG_SIGN(MERGE); }
MEDIUMTEXT                    { REPUT_TOKEN_NEG_SIGN(MEDIUMTEXT); }
MIDDLEINT                     { REPUT_TOKEN_NEG_SIGN(MIDDLEINT); }
MINUTE_MICROSECOND            { REPUT_TOKEN_NEG_SIGN(MINUTE_MICROSECOND); }
MINUTE_SECOND                 { REPUT_TOKEN_NEG_SIGN(MINUTE_SECOND); }
MOD                           { REPUT_TOKEN_NEG_SIGN(MOD); }
MODE                          { REPUT_TOKEN_NEG_SIGN(MODE); }
MODIFIES                      { REPUT_TOKEN_NEG_SIGN(MODIFIES); }
NATURAL                       { REPUT_TOKEN_NEG_SIGN(NATURAL); }
NO_WRITE_TO_BINLOG            { REPUT_TOKEN_NEG_SIGN(NO_WRITE_TO_BINLOG); }
NUMERIC                       { REPUT_TOKEN_NEG_SIGN(NUMBER); }
ON                            { REPUT_TOKEN_NEG_SIGN(ON); }
OPTION                        { REPUT_TOKEN_NEG_SIGN(OPTION); }
OPTIMIZE                      { REPUT_TOKEN_NEG_SIGN(OPTIMIZE); }
OPTIONALLY                    { REPUT_TOKEN_NEG_SIGN(OPTIONALLY); }
OR                            { REPUT_TOKEN_NEG_SIGN(OR); }
ORDER                         { REPUT_TOKEN_NEG_SIGN(ORDER); }
OUT                           { REPUT_TOKEN_NEG_SIGN(OUT); }
OUTER                         { REPUT_TOKEN_NEG_SIGN(OUTER); }
OUTFILE                       { REPUT_TOKEN_NEG_SIGN(OUTFILE); }
PARSER                        { REPUT_TOKEN_NEG_SIGN(PARSER); }
PROCEDURE                     { REPUT_TOKEN_NEG_SIGN(PROCEDURE); }
PURGE                         { REPUT_TOKEN_NEG_SIGN(PURGE); }
PARTITION                     { REPUT_TOKEN_NEG_SIGN(PARTITION); }
PRECISION                     { REPUT_TOKEN_NEG_SIGN(PRECISION); }
PRIMARY                       { REPUT_TOKEN_NEG_SIGN(PRIMARY); }
PUBLIC                        { REPUT_TOKEN_NEG_SIGN(PUBLIC); }
RANGE                         { REPUT_TOKEN_NEG_SIGN(RANGE); }
READ                          { REPUT_TOKEN_NEG_SIGN(READ); }
READ_WRITE                    { REPUT_TOKEN_NEG_SIGN(READ_WRITE); }
READS                         { REPUT_TOKEN_NEG_SIGN(READS); }
REAL                          { REPUT_TOKEN_NEG_SIGN(REAL); }
RECURSIVE                     { REPUT_TOKEN_NEG_SIGN(RECURSIVE); }
RELEASE                       { REPUT_TOKEN_NEG_SIGN(RELEASE); }
REFERENCES                    { REPUT_TOKEN_NEG_SIGN(REFERENCES); }
REGEXP                        { REPUT_TOKEN_NEG_SIGN(REGEXP); }
RENAME                        { REPUT_TOKEN_NEG_SIGN(RENAME); }
REPLACE                       { REPUT_TOKEN_NEG_SIGN(REPLACE); }
REPEAT                        { REPUT_TOKEN_NEG_SIGN(REPEAT); }
REQUIRE                       { REPUT_TOKEN_NEG_SIGN(REQUIRE); }
RESIGNAL                      { REPUT_TOKEN_NEG_SIGN(RESIGNAL); }
RESTRICT                      { REPUT_TOKEN_NEG_SIGN(RESTRICT); }
RETURN                        { REPUT_TOKEN_NEG_SIGN(RETURN); }
REVOKE                        { REPUT_TOKEN_NEG_SIGN(REVOKE); }
RIGHT                         { REPUT_TOKEN_NEG_SIGN(RIGHT); }
RLIKE                         { REPUT_TOKEN_NEG_SIGN(REGEXP); }
ROWS                          { REPUT_TOKEN_NEG_SIGN(ROWS); }
SECOND_MICROSECOND            { REPUT_TOKEN_NEG_SIGN(SECOND_MICROSECOND); }
SELECT                        { REPUT_TOKEN_NEG_SIGN(SELECT); }
SCHEMA                        { REPUT_TOKEN_NEG_SIGN(SCHEMA); }
SCHEMAS                       { REPUT_TOKEN_NEG_SIGN(SCHEMAS); }
SEPARATOR                     { REPUT_TOKEN_NEG_SIGN(SEPARATOR); }
SET                           { REPUT_TOKEN_NEG_SIGN(SET); }
SENSITIVE                     { REPUT_TOKEN_NEG_SIGN(SENSITIVE); }
SHOW                          { REPUT_TOKEN_NEG_SIGN(SHOW); }
SIGNAL                        { REPUT_TOKEN_NEG_SIGN(SIGNAL); }
SMALLINT                      { REPUT_TOKEN_NEG_SIGN(SMALLINT); }
SPATIAL                       { REPUT_TOKEN_NEG_SIGN(SPATIAL); }
SPECIFIC                      { REPUT_TOKEN_NEG_SIGN(SPECIFIC); }
SQL                           { REPUT_TOKEN_NEG_SIGN(SQL); }
SQLEXCEPTION                  { REPUT_TOKEN_NEG_SIGN(SQLEXCEPTION); }
SQLSTATE                      { REPUT_TOKEN_NEG_SIGN(SQLSTATE); }
SQLWARNING                    { REPUT_TOKEN_NEG_SIGN(SQLWARNING); }
SQL_BIG_RESULT                { REPUT_TOKEN_NEG_SIGN(SQL_BIG_RESULT); }
SQL_CALC_FOUND_ROWS           { REPUT_TOKEN_NEG_SIGN(SQL_CALC_FOUND_ROWS); }
SQL_SMALL_RESULT              { REPUT_TOKEN_NEG_SIGN(SQL_SMALL_RESULT); }
SSL                           { REPUT_TOKEN_NEG_SIGN(SSL); }
STARTING                      { REPUT_TOKEN_NEG_SIGN(STARTING); }
STORED                        { REPUT_TOKEN_NEG_SIGN(STORED); }
STRAIGHT_JOIN                 { REPUT_TOKEN_NEG_SIGN(STRAIGHT_JOIN); }
SUBJECT                       { REPUT_TOKEN_NEG_SIGN(SUBJECT); }
SYSDATE                       { REPUT_TOKEN_NEG_SIGN(SYSDATE); }
TERMINATED                    { REPUT_TOKEN_NEG_SIGN(TERMINATED); }
TEXT                          { REPUT_TOKEN_NEG_SIGN(TEXT); }
TINYBLOB                      { REPUT_TOKEN_NEG_SIGN(TINYBLOB); }
TINYINT                       { REPUT_TOKEN_NEG_SIGN(TINYINT); }
TINYTEXT                      { REPUT_TOKEN_NEG_SIGN(TINYTEXT); }
TABLE                         { REPUT_TOKEN_NEG_SIGN(TABLE); }
TABLEGROUP                    { REPUT_TOKEN_NEG_SIGN(TABLEGROUP); }
THEN                          { REPUT_TOKEN_NEG_SIGN(THEN); }
TO                            { REPUT_TOKEN_NEG_SIGN(TO); }
TRAILING                      { REPUT_TOKEN_NEG_SIGN(TRAILING); }
TRIGGER                       { REPUT_TOKEN_NEG_SIGN(TRIGGER); }
UNDO                          { REPUT_TOKEN_NEG_SIGN(UNDO); }
UNION                         { REPUT_TOKEN_NEG_SIGN(UNION); }
UNIQUE                        { REPUT_TOKEN_NEG_SIGN(UNIQUE); }
UNLOCK                        { REPUT_TOKEN_NEG_SIGN(UNLOCK); }
UNSIGNED                      { REPUT_TOKEN_NEG_SIGN(UNSIGNED); }
UPDATE                        { REPUT_TOKEN_NEG_SIGN(UPDATE); }
USAGE                         { REPUT_TOKEN_NEG_SIGN(USAGE); }
USE                           { REPUT_TOKEN_NEG_SIGN(USE); }
USING                         { REPUT_TOKEN_NEG_SIGN(USING); }
UTC_DATE                      { REPUT_TOKEN_NEG_SIGN(UTC_DATE); }
UTC_TIME                      { REPUT_TOKEN_NEG_SIGN(UTC_TIME); }
UTC_TIMESTAMP                 { REPUT_TOKEN_NEG_SIGN(UTC_TIMESTAMP); }
VALUES                        { REPUT_TOKEN_NEG_SIGN(VALUES); }
VARBINARY                     { REPUT_TOKEN_NEG_SIGN(VARBINARY); }
VARCHAR                       { REPUT_TOKEN_NEG_SIGN(VARCHAR); }
VARCHARACTER                  { REPUT_TOKEN_NEG_SIGN(VARCHAR); }
VARYING                       { REPUT_TOKEN_NEG_SIGN(VARYING); }
VIRTUAL                       { REPUT_TOKEN_NEG_SIGN(VIRTUAL); }
WHERE                         { REPUT_TOKEN_NEG_SIGN(WHERE); }
WHEN                          { REPUT_TOKEN_NEG_SIGN(WHEN); }
WHILE                         { REPUT_TOKEN_NEG_SIGN(WHILE); }
WINDOW                        { REPUT_TOKEN_NEG_SIGN(WINDOW); }
WITH                          { REPUT_TOKEN_NEG_SIGN(WITH); }
WRITE                         { REPUT_TOKEN_NEG_SIGN(WRITE); }
XOR                           { REPUT_TOKEN_NEG_SIGN(XOR); }
X509                          { REPUT_TOKEN_NEG_SIGN(X509); }
YEAR_MONTH                    { REPUT_TOKEN_NEG_SIGN(YEAR_MONTH); }
ZEROFILL                      { REPUT_TOKEN_NEG_SIGN(ZEROFILL); }
@@global                      { REPUT_TOKEN_NEG_SIGN(GLOBAL_ALIAS); }
@@session                     { REPUT_TOKEN_NEG_SIGN(SESSION_ALIAS); }
@@local                       { REPUT_TOKEN_NEG_SIGN(SESSION_ALIAS); }

_UTF8                         { REPUT_TOKEN_NEG_SIGN(_UTF8); }
_UTF8MB4                      { REPUT_TOKEN_NEG_SIGN(_UTF8MB4); }
_GBK                          { REPUT_TOKEN_NEG_SIGN(_GBK); }
_GB18030                      { REPUT_TOKEN_NEG_SIGN(_GB18030); }
_BINARY                       { REPUT_TOKEN_NEG_SIGN(_BINARY); }
_UTF16                        { REPUT_TOKEN_NEG_SIGN(_UTF16); }
STRONG                        { REPUT_TOKEN_NEG_SIGN(STRONG); }
WEAK                          { REPUT_TOKEN_NEG_SIGN(WEAK); }
FROZEN                        { REPUT_TOKEN_NEG_SIGN(FROZEN); }
EXCEPT                        { REPUT_TOKEN_NEG_SIGN(EXCEPT); }
MINUS                         { REPUT_TOKEN_NEG_SIGN(MINUS); }
INTERSECT                     { REPUT_TOKEN_NEG_SIGN(INTERSECT); }
ISNULL                        { REPUT_TOKEN_NEG_SIGN(ISNULL); }
NOT {
  int32_t token_ret = NOT; /*fast parameterize don't care NOT or NOT2*/
  if (!(IS_FAST_PARAMETERIZE)) {
    ObSQLMode mode = ((ParseResult *)yyextra)->sql_mode_;
    bool is_high_not_procedence = false;
      IS_HIGH_NOT_PRECEDENCE(mode, is_high_not_procedence);
    if (is_high_not_procedence) {
      token_ret = NOT2;
    } else {
      token_ret = NOT;
    }
  }
  return token_ret;
}
NULL {
  check_value(yylval);
  ParseResult *p = (ParseResult *)yyextra;
  malloc_new_node(yylval->node, p->malloc_pool_, T_NULL, 0);
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    return NULLX;
  }
}

{int_num} {
  int32_t token_ret = INTNUM;
  ParseNode *node = NULL;
  check_value(yylval);
  ParseResult *p = (ParseResult *)yyextra;
  malloc_new_node(node, p->malloc_pool_, T_INT, 0);
  yylval->node = node;
  int err_no = 0;
  COPY_NUM_STRING(p, node);
  // we treated '-  12' as a const node, that is to say node->str_value_ = '-  12'
  // however `strtoll` can't parse '-  12' to a int value, for there are spaces between neg sign and numbers
  // so here, we have to strip spaces
  PARSE_INT_STR_MYSQL(node, p->malloc_pool_, err_no);
  node->raw_text_ = node->str_value_;
  node->text_len_ = node->str_len_;
  p->minus_ctx_.is_cur_numeric_ = true;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    setup_token_pos_info(node, yylloc->first_column - 1, node->text_len_);
    return token_ret;
  }
}

<hint>{int_num} {
  ParseResult *p = (ParseResult *)yyextra;
  if (!p->is_ignore_token_) {
    if (IS_FAST_PARAMETERIZE) {
      COPY_WRITE();
    } else {
      int32_t token_ret = INTNUM;
      ParseNode *node = NULL;
      ParseResult *p = (ParseResult *)yyextra;
      malloc_new_node(node, p->malloc_pool_, T_INT, 0);
      check_value(yylval);
      yylval->node = node;
      node->str_value_ = parse_strdup(yytext, p->malloc_pool_, &(node->str_len_));
      check_malloc(node->str_value_);
      int err_no = 0;
      node->value_ = ob_strntoll(node->str_value_, node->str_len_, 10, NULL, &err_no);
      if (ERANGE == err_no)
      {
        /* if out of range, seem it as must NUMERIC type, now is double */
        node->type_ = T_NUMBER;
        token_ret = DECIMAL_VAL;
      }
      return token_ret;
    }
  }
}

[0-9]+E[-+]?[0-9]+ |
[0-9]+"."[0-9]*E[-+]?[0-9]+ |
"."[0-9]+E[-+]?[0-9]+ {
  ParseNode *node = NULL;
  ParseResult *p = (ParseResult *)yyextra;
  malloc_new_node(node, p->malloc_pool_, T_DOUBLE, 0);
  check_value(yylval);
  yylval->node = node;
  COPY_NUM_STRING(p, node);
  node->raw_text_ = node->str_value_;
  node->text_len_ = node->str_len_;
  p->minus_ctx_.is_cur_numeric_ = true;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    return DECIMAL_VAL;
  }
}

[0-9]+"."[0-9]* |
"."[0-9]+ {
  ParseNode *node = NULL;
  ParseResult *p = (ParseResult *)yyextra;
  malloc_new_node(node, p->malloc_pool_, T_NUMBER/* should be T_NUMBER,*/, 0);
  check_value(yylval);
  yylval->node = node;
  COPY_NUM_STRING(p, node);
  node->raw_text_ = node->str_value_;
  node->text_len_ = node->str_len_;
  p->minus_ctx_.is_cur_numeric_ = true;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    return DECIMAL_VAL;
  }
}

TRUE {
  check_value(yylval);
  ParseResult *p = (ParseResult *)yyextra;
  malloc_new_node(yylval->node, p->malloc_pool_, T_BOOL, 0);
  yylval->node->value_ = 1;
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    return BOOL_VALUE;
  }
}

FALSE {
  check_value(yylval);
  ParseResult *p = (ParseResult *)yyextra;
  malloc_new_node(yylval->node, p->malloc_pool_, T_BOOL, 0);
  yylval->node->value_ = 0;
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    return BOOL_VALUE;
  }
}

{sqbegin} {
  BEGIN(sq);
  ParseResult *p = (ParseResult *)yyextra;
  p->start_col_ = yylloc->first_column;
  check_value(yylval);
  malloc_new_node(yylval->node, p->malloc_pool_, T_VARCHAR, 0);
  yylval->node->str_len_ = 0;
  p->last_well_formed_len_ = 0;
  yylval->node->str_value_ = NULL;
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    COPY_WRITE();
  }
}

<sq>{sqend} { // fast parameterize copy text here, don't return token
  BEGIN(INITIAL);
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  yylloc->first_column = p->start_col_;
  FORMAT_STR_NODE(yylval->node);
  yylval->node->text_len_ = yylloc->last_column - p->start_col_ + 1;
  COPY_STRING(p->input_sql_ + p->start_col_ - 1, yylval->node->text_len_, yylval->node->raw_text_);
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    setup_token_pos_info(yylval->node, yylloc->first_column, yylval->node->text_len_ - 2);
    return STRING_VALUE;
  }
}

<sq>{sqdouble} {
  check_value(yylval);
  COPY_STR_NODE_TO_TMP_LITERAL(yylval->node);
  ((ParseResult *)yyextra)->tmp_literal_[yylval->node->str_len_++] = '\'';
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    COPY_WRITE();
  }
}

<sq>{sqcontent} {
  check_value(yylval);
  STORE_STR_CONTENT(yylval->node);
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    COPY_WRITE();
  }
}

<sq>[\n\r] {
  check_value(yylval);
  ++yylineno;
  STORE_STR_CONTENT(yylval->node);
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    COPY_WRITE();
  }
}

<sq>{qescape} {
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  COPY_STR_NODE_TO_TMP_LITERAL(yylval->node);
  bool is_real_escape = true;
  CHECK_REAL_ESCAPE(is_real_escape);
  ObSQLMode mode = p->sql_mode_;
  bool is_no_backslash_escapes = false;
  IS_NO_BACKSLASH_ESCAPES(mode, is_no_backslash_escapes);
  if (!is_real_escape || is_no_backslash_escapes) {
    HANDLE_FALSE_ESCAPE(p);
  } else {
    HANDLE_ESCAPE(p);
  }
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    COPY_WRITE();
  }
}

<sq>{sqnewline} {
  /*TODO fast parameterize*/
  /*
     In case of two adjacent string literal, such as " 'a' 'b' ", the two string will be
     concatenate into 'ab'. However, the string 'a' will used as the column name if it appears
     in the select list, which means we must save it rather than just skipping the 'sqnewline'.

     One solution is to do this in the yacc and let the lexer produce all strings as individual
     tokens. However, it will generate ambiguity in the yacc according to our grammar definition.
     Instead, we remember the first string as a child of the 'T_VARCHAR' node which represents
     " 'a' 'b' ", whose str_value_ is 'ab'. This will save us from modifying our grammar and a
     a lot of troubles.
   */
  check_value(yylval);
  ADD_YYLINENO(yytext, yyleng);
  COPY_STR_NODE_TO_TMP_LITERAL(yylval->node);
  if (0 == yylval->node->num_child_) {
    ParseResult *p = (ParseResult *)yyextra;
    char *tmp_literal = p->tmp_literal_;
    tmp_literal[yylval->node->str_len_] = '\0';
    yylval->node->children_ = (ParseNode **)parse_malloc(sizeof(ParseNode *), p->malloc_pool_);
    if (OB_UNLIKELY(NULL == yylval->node->children_)) {
      p->extra_errno_ = OB_PARSER_ERR_NO_MEMORY;
      yyerror(yylloc, yyextra, "No more space for mallocing '%s'\n", yytext);
      return ERROR;
    }

    malloc_new_node(yylval->node->children_[0], p->malloc_pool_, T_CONCAT_STRING, 0);
    (*yylval->node->children_)->str_value_ = parse_strndup(tmp_literal, yylval->node->str_len_ + 1,
                                            p->malloc_pool_);
    check_malloc((*yylval->node->children_)->str_value_);
    (*yylval->node->children_)->str_len_ = yylval->node->str_len_;
    yylval->node->num_child_ = 1;
  }
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    COPY_WRITE();
  }
}

<sq><<EOF>>  {
  yyerror(yylloc, yyextra, "unterminated quoted string\n");
  return PARSER_SYNTAX_ERROR;
}

{dqbegin} {
  ParseResult *p = (ParseResult *)yyextra;
  ObSQLMode mode = p->sql_mode_;
  bool is_ansi_quotes = false;
  IS_ANSI_QUOTES(mode, is_ansi_quotes);
  if (is_ansi_quotes) {
    BEGIN(adq); 
    if (IS_FAST_PARAMETERIZE) {
      COPY_WRITE();
    } else {
      check_value(yylval);
      p->start_col_ = yylloc->first_column;
      char **tmp_literal = &p->tmp_literal_;
      if (*tmp_literal == NULL)
      {
        *tmp_literal = (char*) parse_malloc(p->input_sql_len_ + 1, p->malloc_pool_);
        check_malloc(*tmp_literal);
      }
      malloc_new_node(yylval->node, p->malloc_pool_, T_IDENT, 0);
      yylval->node->str_len_ = 0;
    }
  } else {
    BEGIN(dq);
    p->start_col_ = yylloc->first_column;
    char **tmp_literal = &p->tmp_literal_;
    if (*tmp_literal == NULL)
    {
      *tmp_literal = (char*) parse_malloc(p->input_sql_len_ + 1, p->malloc_pool_);
      check_malloc(*tmp_literal);
    }
    malloc_new_node(yylval->node, p->malloc_pool_, T_VARCHAR, 0);
    yylval->node->str_len_ = 0;
    p->last_well_formed_len_ = 0;
    if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
      return OUTLINE_DEFAULT_TOKEN;
    }
  }
}

<dq>{dqend} {
  BEGIN(INITIAL);
  ParseResult *p = (ParseResult *)yyextra;
  yylloc->first_column = p->start_col_;
  char *tmp_literal = p->tmp_literal_;
  check_value(yylval);
  tmp_literal[yylval->node->str_len_] = '\0';
  yylval->node->str_value_ = parse_strndup(tmp_literal, yylval->node->str_len_ + 1, p->malloc_pool_);
  check_malloc(yylval->node->str_value_);
  yylval->node->text_len_ = yylloc->last_column - p->start_col_ + 1;
  yylval->node->raw_text_ = parse_strndup(p->input_sql_ + p->start_col_ - 1, yylval->node->text_len_, p->malloc_pool_);
  check_malloc(yylval->node->raw_text_);
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    setup_token_pos_info(yylval->node, yylloc->first_column, yylval->node->str_len_);
    return STRING_VALUE;
  }
}

<dq>{dqdouble} {
  check_value(yylval);
  ((ParseResult *)yyextra)->tmp_literal_[yylval->node->str_len_++] = '\"';
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    return OUTLINE_DEFAULT_TOKEN;
  }
}

<dq>{dqcontent} {
  check_value(yylval);
  memmove(((ParseResult *)yyextra)->tmp_literal_ + yylval->node->str_len_, yytext, yyleng);
  yylval->node->str_len_ += yyleng;
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    return OUTLINE_DEFAULT_TOKEN;
  }
}

<dq>[\n\r] {
  check_value(yylval);
  ++yylineno;
  memmove(((ParseResult *)yyextra)->tmp_literal_ + yylval->node->str_len_, yytext, yyleng);
  yylval->node->str_len_ += yyleng;
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    return OUTLINE_DEFAULT_TOKEN;
  }
}

<dq>{qescape} {
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  bool is_real_escape = true;
  CHECK_REAL_ESCAPE(is_real_escape);
  //check sql_mode
  ObSQLMode mode = p->sql_mode_;
  bool is_no_backslash_escapes = false;
  IS_NO_BACKSLASH_ESCAPES(mode, is_no_backslash_escapes);
  if (!is_real_escape || is_no_backslash_escapes) {
    HANDLE_FALSE_ESCAPE(p);
  } else {
    HANDLE_ESCAPE(p);
  }
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    return OUTLINE_DEFAULT_TOKEN;
  }
}


<dq>{dqnewline} {
  /* see 'sqnewline' */
  check_value(yylval);
  ADD_YYLINENO(yytext, yyleng);
  if (0 == yylval->node->num_child_) {
    ParseResult *p = (ParseResult *)yyextra;
    char *tmp_literal = p->tmp_literal_;
    tmp_literal[yylval->node->str_len_] = '\0';

    yylval->node->children_ = (ParseNode **)parse_malloc(sizeof(ParseNode *), p->malloc_pool_);
    if (OB_UNLIKELY(NULL == yylval->node->children_)) {
      p->extra_errno_ = OB_PARSER_ERR_NO_MEMORY;
      yyerror(yylloc, yyextra, "No more space for mallocing '%s'\n", yytext);
      return ERROR;
    }

    malloc_new_node(yylval->node->children_[0], p->malloc_pool_, T_CONCAT_STRING, 0);
    (*yylval->node->children_)->str_value_ = parse_strndup(tmp_literal, yylval->node->str_len_ + 1,
                                            p->malloc_pool_);
    check_malloc((*yylval->node->children_)->str_value_);
    (*yylval->node->children_)->str_len_ = yylval->node->str_len_;
    yylval->node->num_child_ = 1;
  }
  if (IS_FAST_PARAMETERIZE && !IS_NEED_PARAMETERIZE) {
    return OUTLINE_DEFAULT_TOKEN;
  }
}

<dq><<EOF>>  {
  yyerror(yylloc, yyextra, "unterminated doublequoted string\n");
  return PARSER_SYNTAX_ERROR;
}

<adq>{dqdouble} {
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  } else {
    check_value(yylval);
    ((ParseResult *)yyextra)->tmp_literal_[yylval->node->str_len_++] = '"';
  }
}

<adq>{adqcontent} {
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  } else {
    check_value(yylval);
    memmove(((ParseResult *)yyextra)->tmp_literal_ + yylval->node->str_len_, yytext, yyleng);
    yylval->node->str_len_ += yyleng;
  }
}

<adq>{dqend} {
  BEGIN(INITIAL);
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  } else {
    ParseResult *p = (ParseResult *)yyextra;
    check_value(yylval);
    yylloc->first_column = p->start_col_;
    char *tmp_literal = p->tmp_literal_;
    tmp_literal[yylval->node->str_len_] = '\0';

    char *dup_value = NULL;
    if (p->is_not_utf8_connection_) {
      dup_value = parse_str_convert_utf8(p->charset_info_, tmp_literal, p->malloc_pool_, &(yylval->node->str_len_), &(p->extra_errno_));
      check_identifier_convert_result(p->extra_errno_);
      //fprintf(stderr, "parse identifier result, str=%s, str_len=%ld\n", node->str_value_, node->str_len_);
    } else {
      dup_value = parse_strndup(tmp_literal, yylval->node->str_len_ + 1, p->malloc_pool_);
    }
    check_malloc(dup_value);
    yylval->node->str_value_ = dup_value;
    setup_token_pos_info(yylval->node, yylloc->first_column, yylval->node->str_len_);
    return NAME_OB;
  }
}

<adq><<EOF>> {
  yyerror(yylloc, yyextra, "unterminated backtick string\n");
  return PARSER_SYNTAX_ERROR;
}

{btbegin} {
  BEGIN(bt); /*fast parameterize don't handle connent in ``*/
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  } else {
    check_value(yylval);
    ParseResult *p = (ParseResult *)yyextra;
    p->start_col_ = yylloc->first_column;
    char **tmp_literal = &p->tmp_literal_;
    if (NULL == *tmp_literal)
    {
      *tmp_literal = (char *)parse_malloc(p->input_sql_len_ + 1, p->malloc_pool_);
      check_malloc(*tmp_literal);
    }
    malloc_new_node(yylval->node, p->malloc_pool_, T_IDENT, 0);
    yylval->node->str_len_ = 0;
  }
}

<bt>{btdouble} {
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  } else {
    check_value(yylval);
    ((ParseResult *)yyextra)->tmp_literal_[yylval->node->str_len_++] = '`';
  }
}
<bt>{btcontent} {
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  } else {
    check_value(yylval);
    memmove(((ParseResult *)yyextra)->tmp_literal_ + yylval->node->str_len_, yytext, yyleng);
    yylval->node->str_len_ += yyleng;
  }
}

<bt>{btend} {
  BEGIN(INITIAL);
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  } else {
    ParseResult *p = (ParseResult *)yyextra;
    check_value(yylval);
    yylloc->first_column = p->start_col_;
    char *tmp_literal = p->tmp_literal_;
    tmp_literal[yylval->node->str_len_] = '\0';
    CHECK_STR_LEN_MATCH(tmp_literal, yylval->node->str_len_);

    char *dup_value = NULL;
    if (p->is_not_utf8_connection_) {
      dup_value = parse_str_convert_utf8(p->charset_info_, tmp_literal, p->malloc_pool_, &(yylval->node->str_len_), &(p->extra_errno_));
      check_identifier_convert_result(p->extra_errno_);
      //fprintf(stderr, "parse identifier result, str=%s, str_len=%ld\n", node->str_value_, node->str_len_);
    } else {
      dup_value = parse_strndup(tmp_literal, yylval->node->str_len_ + 1, p->malloc_pool_);
    }
    check_malloc(dup_value);
    yylval->node->str_value_ = dup_value;
    setup_token_pos_info(yylval->node, yylloc->first_column, yylval->node->str_len_);
    return NAME_OB;
  }
}

<bt><<EOF>>  {
  yyerror(yylloc, yyextra, "unterminated backtick string\n");
  return PARSER_SYNTAX_ERROR;
}

X'([0-9A-F])*'|0X([0-9A-F])+ {
  char *src = yytext + 2;
  size_t len = yyleng - 2;
  if ('\'' == src[len - 1]) {
    --len;
  }
  ParseNode *node = NULL;
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  malloc_new_node(node, p->malloc_pool_, T_HEX_STRING, 0);
  if (len > 0) {
    int64_t dest_len = ob_parse_binary_len(len);
    char *dest = (char *)parse_malloc(dest_len, p->malloc_pool_);
    check_malloc(dest);
    ob_parse_binary(src, len, dest);
    node->str_value_ = dest;
    node->str_len_ = dest_len;
  }
  else
  {
    node->str_value_ = NULL;
    node->str_len_ = 0;
  }
  yylval->node = node;
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  yylval->node->is_copy_raw_text_ = 1;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    return HEX_STRING_VALUE;
  }
}

B'([01])*'|0B([01])+ {
  char* src = yytext + 2;
  size_t len = yyleng - 2;
  if(src[len - 1] == '\'')
  {
    --len;
  }
  ParseNode *node = NULL;
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  malloc_new_node(node, p->malloc_pool_, T_HEX_STRING, 0);
  if (len > 0)
  {
    int64_t dest_len = ob_parse_bit_string_len(len);
    char *dest = (char*)parse_malloc(dest_len, p->malloc_pool_);
    check_malloc(dest);
    ob_parse_bit_string(src, len, dest);
    node->str_value_ = dest;
    node->str_len_ = dest_len;
  }
  else
  {
    node->str_value_ = NULL;
    node->str_len_ = 0;
  }
  yylval->node = node;
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  yylval->node->is_copy_raw_text_ = 1;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    return HEX_STRING_VALUE;
  }
}

Date{whitespace}?'[^']*' {
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  malloc_time_node_s(p->malloc_pool_, T_DATE);
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  yylval->node->is_copy_raw_text_ = 1;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
   return DATE_VALUE;
  }
}

Time{whitespace}?'[^']*' {
  ParseResult *p = (ParseResult *)yyextra;
  malloc_time_node_s(p->malloc_pool_, T_TIME);
  check_value(yylval);
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  yylval->node->is_copy_raw_text_ = 1;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    return DATE_VALUE;
  }
}

Timestamp{whitespace}?'[^']*' {
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  malloc_time_node_s(p->malloc_pool_, T_TIMESTAMP);
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  yylval->node->is_copy_raw_text_ = 1;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    return DATE_VALUE;
  }
}
Date{whitespace}?\"[^\"]*\" {
  ParseResult *p = (ParseResult *)yyextra;
  malloc_time_node_d(p->malloc_pool_, T_DATE);
  check_value(yylval);
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  yylval->node->is_copy_raw_text_ = 1;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    return DATE_VALUE;
  }
}

Time{whitespace}?\"[^\"]*\" {
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  malloc_time_node_d(p->malloc_pool_, T_TIME);
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  yylval->node->is_copy_raw_text_ = 1;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    return DATE_VALUE;
  }
}

Timestamp{whitespace}?\"[^\"]*\" {
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  malloc_time_node_d(p->malloc_pool_, T_TIMESTAMP);
  COPY_STRING(p->input_sql_ + yylloc->first_column - 1, yyleng, yylval->node->raw_text_);
  yylval->node->text_len_ = yyleng;
  yylval->node->is_copy_raw_text_ = 1;
  if (IS_FAST_PARAMETERIZE) {
    STORE_PARAM_NODE();
  } else {
    return DATE_VALUE;
  }
}

\/\*HINT\+[^*]+\*\/    {
  if (!(IS_FAST_PARAMETERIZE)) {
    int64_t out_len = 0;
    char *dest = NULL;
    check_value(yylval);
    /* ParseNode *node = new_node(((ParseResult *)yyextra)->malloc_pool_, T_HINT, 0); */
    ParseNode *node = NULL;
    ParseResult *p = (ParseResult *)yyextra;
    malloc_new_node(node, p->malloc_pool_, T_HINT, 0);
    dest = parse_strdup(yytext + 3, p->malloc_pool_, &out_len);
    check_malloc(dest);
    dest[out_len - 2] = '\0';

    node->str_value_ = dest;
    node->str_len_ = out_len - 1;

    yylval->node = node;
  }
  return HINT_VALUE;
}

{select_hint_begin} {
  BEGIN(hint);
  ParseResult *p = (ParseResult *)yyextra;
  p->is_ignore_token_ = p->is_ignore_hint_;
  return SELECT_HINT_BEGIN;
}

{update_hint_begin} {
  BEGIN(hint);
  ParseResult *p = (ParseResult *)yyextra;
  p->is_ignore_token_ = p->is_ignore_hint_;
  return UPDATE_HINT_BEGIN;
}
{delete_hint_begin} {
  BEGIN(hint);
  ParseResult *p = (ParseResult *)yyextra;
  p->is_ignore_token_ = p->is_ignore_hint_;
  return DELETE_HINT_BEGIN;
}
{insert_hint_begin} {
  BEGIN(hint);
  ParseResult *p = (ParseResult *)yyextra;
  p->is_ignore_token_ = p->is_ignore_hint_;
  return INSERT_HINT_BEGIN;
}
{replace_hint_begin} {
  BEGIN(hint);
  ParseResult *p = (ParseResult *)yyextra;
  p->is_ignore_token_ = p->is_ignore_hint_;
  return REPLACE_HINT_BEGIN;
}
{hint_hint_begin} {
  BEGIN(hint);
  ParseResult *p = (ParseResult *)yyextra;
  p->is_ignore_token_ = p->is_ignore_hint_;
  return HINT_HINT_BEGIN;
}
{load_data_hint_begin} {
  BEGIN(hint);
  ParseResult *p = (ParseResult *)yyextra;
  p->is_ignore_token_ = p->is_ignore_hint_;
  return LOAD_DATA_HINT_BEGIN;
}
<hint>{c_cmt_end} {
  BEGIN(INITIAL);
  return HINT_END;
}
<hint>[ \t\r\n] {
  ParseResult *p = (ParseResult *)yyextra;
  if (IS_FAST_PARAMETERIZE && !p->is_ignore_token_) {
    COPY_WRITE();
  }
}
<hint>INDEX { return INDEX_HINT; }
<hint>USE_NL { return USE_NL; }
<hint>NO_USE_NL { return NO_USE_NL; }
<hint>USE_BNL { return USE_BNL; }
<hint>NO_USE_BNL { return NO_USE_BNL; }
<hint>USE_NL_MATERIALIZATION { return USE_NL_MATERIALIZATION; }
<hint>NO_USE_NL_MATERIALIZATION { return NO_USE_NL_MATERIALIZATION; }
<hint>FROZEN_VERSION { return FROZEN_VERSION; }
<hint>TOPK { return TOPK; }
<hint>QUERY_TIMEOUT { return QUERY_TIMEOUT; }
<hint>READ_CONSISTENCY  { return READ_CONSISTENCY; }
<hint>WEAK  { return WEAK; }
<hint>STRONG  { return STRONG; }
<hint>FROZEN  { return FROZEN; }
<hint>NONE  { return NONE; }
<hint>DEFAULT  { return DEFAULT; }
<hint>MAX_CONCURRENT { return MAX_CONCURRENT; }
<hint>PARALLEL { return PARALLEL; }
<hint>NO_PARALLEL { return NO_PARALLEL; }
<hint>AUTO { return AUTO; }
<hint>FORCE { return FORCE; }
<hint>[(),.@]  {
  ParseResult *p = (ParseResult *)yyextra;
  if (!p->is_ignore_token_) {
    if (IS_FAST_PARAMETERIZE) {
      if (!p->is_ignore_token_) {
        COPY_WRITE();
      }
    } else {
      return yytext[0];
    }
  }
}
<hint>HOTSPOT  { return HOTSPOT; }
<hint>LOG_LEVEL  { return LOG_LEVEL; }
<hint>LEADING  { return LEADING_HINT; }
<hint>ORDERED  { return ORDERED; }
<hint>NO_REWRITE { return NO_REWRITE; }
<hint>FULL  { return FULL_HINT; }
<hint>USE_MERGE  { return USE_MERGE; }
<hint>NO_USE_MERGE  { return NO_USE_MERGE; }
<hint>USE_HASH  { return USE_HASH; }
<hint>NO_USE_HASH  { return NO_USE_HASH; }
<hint>USE_PLAN_CACHE  { return USE_PLAN_CACHE; }
<hint>USE_JIT { return USE_JIT; }
<hint>NO_USE_JIT { return NO_USE_JIT; }
<hint>USE_HASH_AGGREGATION  { return USE_HASH_AGGREGATION; }
<hint>NO_USE_HASH_AGGREGATION  { return NO_USE_HASH_AGGREGATION; }
<hint>USE_LATE_MATERIALIZATION  { return USE_LATE_MATERIALIZATION; }
<hint>NO_USE_LATE_MATERIALIZATION  { return NO_USE_LATE_MATERIALIZATION; }
<hint>TRACE_LOG { return TRACE_LOG; }
<hint>USE_PX { return USE_PX; }
<hint>LOAD_BATCH_SIZE { return LOAD_BATCH_SIZE; }
<hint>TRACING { return TRACING; }
<hint>FORCE_REFRESH_LOCATION_CACHE { return FORCE_REFRESH_LOCATION_CACHE; }
<hint>STAT { return STAT; }
<hint>NO_USE_PX { return NO_USE_PX; }
<hint>PX_JOIN_FILTER { return PX_JOIN_FILTER; }
<hint>NO_PX_JOIN_FILTER { return NO_PX_JOIN_FILTER; }
<hint>QB_NAME { return QB_NAME; }
<hint>TRANS_PARAM { return TRANS_PARAM; }
<hint>PQ_DISTRIBUTE { return PQ_DISTRIBUTE; }
<hint>PQ_MAP { return PQ_MAP; }
<hint>PRANDOM_LOCAL { return RANDOM_LOCAL; }
<hint>BROADCAST { return BROADCAST; }
<hint>PARTITION { return PARTITION; }
<hint>HASH { return HASH; }
<hint>[-] { return NEG_SIGN; }
<hint>MERGE { return MERGE_HINT; }
<hint>NO_MERGE { return NO_MERGE_HINT; }
<hint>NO_EXPAND { return NO_EXPAND; }
<hint>USE_CONCAT { return USE_CONCAT; }
<hint>UNNEST { return UNNEST; }
<hint>NO_UNNEST { return NO_UNNEST; }
<hint>PLACE_GROUP_BY { return PLACE_GROUP_BY; }
<hint>NO_PLACE_GROUP_BY { return NO_PLACE_GROUP_BY; }
<hint>NO_PRED_DEDUCE { return NO_PRED_DEDUCE; }
<hint>ENABLE_PARALLEL_DML  { return ENABLE_PARALLEL_DML; }
<hint>DISABLE_PARALLEL_DML  { return DISABLE_PARALLEL_DML; }
<hint>{identifier} {
  if (!(IS_FAST_PARAMETERIZE)) {
    check_value(yylval);
    ParseResult *p = (ParseResult *)yyextra;
    ParseNode *node = NULL;
    malloc_new_node(node, p->malloc_pool_, T_IDENT, 0);
    yylval->node = node;
    int64_t out_len = 0;
    char *dup_value = NULL;
    if (p->is_not_utf8_connection_) {
      dup_value = parse_str_convert_utf8(p->charset_info_, yytext, p->malloc_pool_, &out_len, &(p->extra_errno_));
      check_identifier_convert_result(p->extra_errno_);
    } else {
      dup_value = parse_strdup(yytext, p->malloc_pool_, &out_len);
    }
    check_malloc(dup_value);
    node->str_value_ = str_tolower(dup_value, strlen(dup_value));
    node->str_len_ = out_len;
    setup_token_pos_info(node, yylloc->first_column - 1, yylval->node->str_len_);
  }
  return NAME_OB;
}

<hint>{quote}  {
  ParseResult *p = (ParseResult *)yyextra;
  if (!p->is_ignore_token_) {
    BEGIN(log_level);
    if (IS_FAST_PARAMETERIZE) {
      COPY_WRITE();
    } else {
      return yytext[0];
    }
  }
}

{mysql_compatible_comment_without_version} {
  // if is a mysql comment without version. For example, /*!any sql str*/
  // mysql_comment without version, processed as common sql str;
  // place before `c_cmt_begin` to avoid (the '/*!') being hidden by '/*')
  ParseResult *p = (ParseResult *)yyextra;
  p->mysql_compatible_comment_ = true;
}

{c_cmt_begin} {
BEGIN(in_c_comment);
#ifdef SQL_PARSER_COMPILATION
  ParseResult *p = (ParseResult *)yyextra;
  if (false == p->stop_add_comment_) {
    TokenPosInfo info;
    info.token_off_ = yylloc->first_column - 1;
    info.token_len_ = -1;
    if (OB_UNLIKELY(OB_PARSER_SUCCESS != add_comment_list(p, &info))) {
      YY_FATAL_ERROR("add comment to comment list failed");
    }
  }
#endif
}

<in_c_comment>{c_cmt_end} {
  ((ParseResult *)yyextra)->has_encount_comment_ = true;
  BEGIN(INITIAL);
#ifdef SQL_PARSER_COMPILATION
  ParseResult *p = (ParseResult *)yyextra;
  p->has_encount_comment_ = true;
  if (false == p->stop_add_comment_) {
    if (OB_UNLIKELY(0 >= p->comment_cnt_)) {
      YY_FATAL_ERROR("comment_cnt_ cannot less than 0 in comment end");
    } else {
      TokenPosInfo *info = &(p->comment_list_[p->comment_cnt_-1]);
      info->token_len_ = yylloc->last_column - info->token_off_;
    }
  }
#endif
}

{mysql_compatible_comment_end} {
  //for mysql compatible comment:
  // only "*/" should be matched, duplicated '*' (e.g., "***/") will report a error.
  ParseResult *p = (ParseResult *)yyextra;
  if (p->mysql_compatible_comment_){
    p->mysql_compatible_comment_ = false;
    BEGIN(INITIAL);
  } else {
    // The sql could be "select */*!xxx*/ from t1;". We can't directly raise a syntax
    // error here. We should treat the "*/" as '*' and '/' by return '*' and unput '/';

    // yyless will change the yytext and yyleng.
    char c_ret = yytext[0];
    yyless(1);
    p->yycolumn_ = p->yycolumn_ - 1;
    return c_ret;
  }
}

<in_c_comment><<EOF>>  {
  yyerror(yylloc, yyextra, "unterminated log_level string\n");
  return PARSER_SYNTAX_ERROR;
}
<in_c_comment>[\n] { yylineno++; }
<in_c_comment>. {}

<log_level>{log_level_stmt} {
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  } else {
    check_value(yylval);
    ParseNode *node = NULL;
    ParseResult *p = (ParseResult *)yyextra;
    malloc_new_node(node, p->malloc_pool_, T_LOG_LEVEL, 0);
    yylval->node = node;
    char *dup_value = NULL;
    int64_t out_len = 0;
    dup_value = parse_strdup(yytext, p->malloc_pool_, &out_len);
    check_malloc(dup_value);
    node->str_value_ = str_tolower(dup_value, strlen(dup_value));
    node->str_len_ = out_len;
    return STRING_VALUE;
  }
}

<log_level>{quote}  {
  BEGIN(hint);
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  } else {
    return yytext[0];
  }
}

<log_level><<EOF>>  {
  yyerror(yylloc, yyextra, "unterminated log_level string\n");
  return PARSER_SYNTAX_ERROR;
}

<log_level>. {}

<hint><<EOF>>  {
  yyerror(yylloc, yyextra, "unterminated hint string\n");
  return PARSER_SYNTAX_ERROR;
}
<hint>. {}

{comment} {
  ((ParseResult *)yyextra)->has_encount_comment_ = true;
/* ignore */ }

[-+&~|^/%*(),.:!] {
  if (IS_FAST_PARAMETERIZE) {
    ParseResult *p = (ParseResult *)yyextra;
    // for 'select -   -1 from dual'
    // the first neg sign is not copied when it is parsed
    // but when the second neg sign is parsed, the first neg sign must be put where is should be
    if ('-' == yytext[0] && IS_NEED_PARAMETERIZE) {
      REPUT_NEG_SIGN(p);
      p->minus_ctx_.pos_ = p->no_param_sql_len_;
      p->minus_ctx_.raw_sql_offset_ = yylloc->first_column - 1;
      p->minus_ctx_.has_minus_ = true;
    } else {
      COPY_WRITE();
      REPUT_NEG_SIGN(p);
    }
  } else {
    return yytext[0];
  }
}

[;] {
  if (IS_FAST_PARAMETERIZE) {
    ParseResult *p = (ParseResult *)yyextra;
    if (p->is_batched_multi_enabled_split_) {
      RM_MULTI_STMT_END_P(p);
    } else {
      COPY_WRITE();
    }
    return END_P;
  } else {
    return DELIMITER;
  }
}

"||" {
  int ret = CNNOP; /*fast parameterize don't care CNNOP or OR_OP*/
  if (!(IS_FAST_PARAMETERIZE)) {
    ObSQLMode mode = ((ParseResult *)yyextra)->sql_mode_;
    bool is_pipes_as_concat = false;
      IS_PIPES_AS_CONCAT(mode, is_pipes_as_concat);
    if (is_pipes_as_concat) {
      ret = CNNOP;
    } else {
      ret = OR_OP;
    }
  }
  return ret;
}

"&&" {return AND_OP;}
"="  {return COMP_EQ;}
":="  {return SET_VAR;}
"<=>"  {return COMP_NSEQ;}
">=" {return COMP_GE;}
">"  {return COMP_GT;}
"<=" {return COMP_LE;}
"<"  {return COMP_LT;}
"!="|"<>" {return COMP_NE;}
"<<"  {return SHIFT_LEFT;}
">>"  {return SHIFT_RIGHT;}
"->"  {return JSON_EXTRACT;}
"->>"  {return JSON_EXTRACT_UNQUOTED;}

"?" {
  //use for outline now, means any value
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  malloc_new_node(yylval->node, p->malloc_pool_, T_QUESTIONMARK, 0);
  yylval->node->value_ = p->question_mark_ctx_.count_++;
  p->question_mark_ctx_.by_ordinal_ = true;
  if (OB_UNLIKELY(p->question_mark_ctx_.by_name_)) {
    YY_FATAL_ERROR("Ordinal binding and Named binding cannot be combined\n");
  }
  if (IS_FAST_PARAMETERIZE) {
    yylval->node->raw_text_ = parse_strdup(yytext, p->malloc_pool_, &(yylval->node->text_len_));
    check_malloc(yylval->node->raw_text_);
    STORE_PARAM_NODE();
  } else {
    return QUESTIONMARK;
  }
}

":"{int_num} {
  ParseResult *p = (ParseResult *)yyextra;
  check_value(yylval);
  malloc_new_node(yylval->node, p->malloc_pool_, T_QUESTIONMARK, 0);
  yylval->node->value_ = get_question_mark(&p->question_mark_ctx_, p->malloc_pool_, yytext);
  if (IS_FAST_PARAMETERIZE) {
    yylval->node->raw_text_ = parse_strdup(yytext, p->malloc_pool_, &(yylval->node->text_len_));
    check_malloc(yylval->node->raw_text_);
    STORE_PARAM_NODE();
  } else {
    return QUESTIONMARK;
  }
}

{system_variable} {
  if (!(IS_FAST_PARAMETERIZE)) {
    /* ParseNode *node = new_node(((ParseResult *)yyextra)->malloc_pool_, T_SYSTEM_VARIABLE, 0); */
    ParseNode *node = NULL;
    ParseResult *p = (ParseResult *)yyextra;
    check_value(yylval);
    malloc_new_node(node, p->malloc_pool_, T_SYSTEM_VARIABLE, 0);
    yylval->node = node;
    /* skip '@@' */
    node->str_value_ = parse_strdup(yytext + 2, p->malloc_pool_, &(node->str_len_));
    check_malloc(node->str_value_);
    //node->value_ = 2;
    node->value_ = 0;
  } else {
    ParseResult *p = (ParseResult *)yyextra;
    REPUT_NEG_SIGN(p);
  }
  return SYSTEM_VARIABLE;
}

{user_variable} {
  /* ParseNode *node = new_node(p->malloc_pool_, T_TEMP_VARIABLE, 0); */
  if (!(IS_FAST_PARAMETERIZE)) {
    ParseNode *node = NULL;
    ParseResult *p = (ParseResult *)yyextra;
    check_value(yylval);
    malloc_new_node(node, p->malloc_pool_, T_USER_VARIABLE_IDENTIFIER, 0);
    yylval->node = node;
    /* skip '@' and quotes like '/"/` */
    node->str_value_ = parse_strdup(yytext + 1, p->malloc_pool_, &(node->str_len_));
    check_malloc(node->str_value_);
    if (NULL != node->str_value_
        && node->str_len_ > 1
    		&& *(yytext + 1) == *(yytext + node->str_len_)
    		&& (*(yytext + 1) == '\'' || *(yytext + 1) == '\"' || *(yytext + 1) == '`')) {
			node->str_value_ += 1;
			node->str_len_ -= 2;
    } else if (NULL != node->str_value_
               && (*(yytext + 1) == '\'' || *(yytext + 1) == '\"' || *(yytext + 1) == '`')) {
      yyerror(yylloc, yyextra, "unterminated user variable name\n");
      return PARSER_SYNTAX_ERROR;
    }
    check_value(node->str_value_);
  } else {
    ParseResult *p = (ParseResult *)yyextra;
    REPUT_NEG_SIGN(p);
  }
  return USER_VARIABLE;
}

{identifier} {
  const NonReservedKeyword *word = NULL;
  if (IS_FAST_PARAMETERIZE) {
    ParseResult *p = (ParseResult *)yyextra;
    REPUT_NEG_SIGN(p);
    if (strcasecmp("nowait", yytext) == 0
        || strcasecmp("no_wait", yytext) == 0) {
      char num[2] = "0";
      STORE_UNIT_TYPE_NODE(num);
    } else {
      return NAME_OB;
    }
  } else {
    int ret = NAME_OB;
    if (NULL == (word = mysql_non_reserved_keyword_lookup(yytext)))
    {
      check_value(yylval);
      ParseNode *node = NULL;
      ParseResult *p = (ParseResult *)yyextra;
      malloc_new_node(node, p->malloc_pool_, T_IDENT, 0);
      yylval->node = node;
      if (p->is_not_utf8_connection_) {
        node->str_value_ = parse_str_convert_utf8(p->charset_info_, yytext, p->malloc_pool_, &(node->str_len_), &(p->extra_errno_));
        check_identifier_convert_result(p->extra_errno_);
        //fprintf(stderr, "parse identifier result, str=%s, str_len=%ld\n", node->str_value_, node->str_len_);
      } else {
        node->str_value_ = parse_strdup(yytext, p->malloc_pool_, &(node->str_len_));
      }
      check_malloc(node->str_value_);
      setup_token_pos_info(node, yylloc->first_column - 1, node->str_len_);
      ret = NAME_OB;
      //CHECK_NODE_STRING_VALUE_ASCII(ret, node->str_value_, node->str_len_);
    } else {
      yylval->non_reserved_keyword = word;
      ret = word->keyword_type;
    }
    return ret;
  }
}

{mysql_compatible_comment_with_version} {
  // comment with version: /*!50600 any sql str*/
  // comment without version: /*!any sql str*/
  // we do not add a start_condition, since some sql string need to be processed in INITIAL state.
  // instead of a new start_condition, we use a extra field (mysql_compatible_comment_) to mark the adaptive comment.
  ParseResult *p = (ParseResult *)yyextra;
  p->mysql_compatible_comment_ = true;
}

[\n] {
  yylineno ++;
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  }
}

[ \t\r] {
  if (IS_FAST_PARAMETERIZE) {
    COPY_WRITE();
  }
}
"--"[ \t].*;

<<EOF>> {return END_P;}
. {
  ParseResult *p = (ParseResult *)yyextra;
  if (p->input_sql_len_ == yylloc->first_column) {
    return END_P;
  } else {
    yyerror(yylloc, yyextra, "mystery charactor '%c'\n", *yytext);
    return PARSER_SYNTAX_ERROR;
  }
}
%%

void *yyalloc(size_t bytes, void *yyscanner)
{
  void *ptr_ret = NULL;
  ParseResult *p = yyget_extra(yyscanner);
  if (OB_UNLIKELY(NULL == p)) {
  } else {
    ptr_ret = parse_malloc(bytes, p->malloc_pool_);
  }
  return ptr_ret;
}

void *yyrealloc(void *ptr, size_t bytes, void *yyscanner)
{
  void *ptr_ret = NULL;
  ParseResult *p = yyget_extra(yyscanner);
  if (OB_UNLIKELY(NULL == p)) {
  } else {
    ptr_ret = parse_realloc(ptr, bytes, p->malloc_pool_);
  }
  return ptr_ret;
}

void yyfree(void *ptr, void *yyscanner)
{
  UNUSED(yyscanner);
  /* Do nothing -- we leave it to the garbage collector. */
  parse_free(ptr);
}
