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

#include <stdio.h>
#include "lib/alloc/alloc_assist.h"
#include "sql/parser/ob_non_reserved_keywords.h"
#include "pl/parser/pl_parser_mysql_mode_tab.h"

static t_node *mysql_pl_none_reserved_keywords_root = NULL;

/* List of non-reserved keywords */
/*一开始会对这些word建立一颗trie树，对于每次的查找来言，树是固定的
 *若新添加的keyword含有除大小写字母、'_'和数字以外的其它字符，请联系@叶提修改这颗树。
 *实现不需要保证字典序，但是原则上还是保证字典序，方便维护和查找*/
static const NonReservedKeyword Mysql_pl_none_reserved_keywords[] =
{
  {"after", AFTER},
  {"begin", BEGIN_KEY},
  {"before", BEFORE},
  {"binary_integer", BINARY_INTEGER},
  {"body", BODY},
  {"by", BY},
  {"case", CASE},
  {"catalog_name", CATALOG_NAME},
  {"class_origin", CLASS_ORIGIN},
  {"close", CLOSE},
  {"column_name", COLUMN_NAME},
  {"comment", COMMENT},
  {"commit", COMMIT},
  {"condition", CONDITION},
  {"constraint_catalog", CONSTRAINT_CATALOG},
  {"constraint_name", CONSTRAINT_NAME},
  {"constraint_schema", CONSTRAINT_SCHEMA},
  {"continue", CONTINUE},
  //{"count", COUNT},
  {"current_user", CURRENT_USER},
  {"cursor", CURSOR},
  {"cursor_name", CURSOR_NAME},
  {"declare", DECLARE},
  {"default", DEFAULT},
  {"definer", DEFINER},
  {"delete", DELETE},
  {"deterministic", DETERMINISTIC},
  {"each", EACH},
  {"else", ELSE},
  {"elseif", ELSEIF},
  {"end", END_KEY},
  {"exists", EXISTS},
  {"exit", EXIT},
  {"extend", EXTEND},
  {"for", FOR},
  {"found", FOUND},
  {"from", FROM},
  {"handler", HANDLER},
  {"if", IF},
  {"in", IN},
  {"insert", INSERT},
  {"is", IS},
  {"inout", INOUT},
  {"iterate", ITERATE},
  {"leave", LEAVE},
  {"limit", LIMIT},
  {"long", LONG},
  {"loop", LOOP},
  {"message_text", MESSAGE_TEXT},
  {"mysql_errno", MYSQL_ERRNO},
  {"national", NATIONAL},
  {"nchar", NCHAR},
  {"next", NEXT},
  {"not", NOT},
  {"nvarchar", NVARCHAR},
  {"of", OF},
  {"on", ON},
  {"open", OPEN},
  {"out", OUT},
  {"package", PACKAGE},
  {"precedes", PRECEDES},
  {"table_name", TABLE_NAME},
  {"then", THEN},
  {"type", TYPE},
  {"record", RECORD},
  {"repeat", REPEAT},
  {"resignal", RESIGNAL},
  {"return", RETURN},
  {"returns", RETURNS},
  {"rollback", ROLLBACK},
  {"row", ROW},
  {"rowtype", ROWTYPE},
  {"role", ROLE},
  {"schema_name", SCHEMA_NAME},
  {"signal", SIGNAL},
  {"sqlexception", SQLEXCEPTION},
  {"sqlstate", SQLSTATE},
  {"sqlwarning", SQLWARNING},
  {"subclass_origin", SUBCLASS_ORIGIN},
  {"until", UNTIL},
  {"update", UPDATE},
  {"user", USER},
  {"using", USING},
  {"when", WHEN},
  {"while", WHILE},
  {"language", LANGUAGE},
  {"sql", SQL},
  {"no", NO},
  {"contains", CONTAINS},
  {"reads", READS},
  {"modifies", MODIFIES},
  {"data", DATA},
  {"constraint_origin", CONSTRAINT_ORIGIN},
  {"invoker", INVOKER},
  {"security", SECURITY},
  {"tinyint", TINYINT},
  {"smallint", SMALLINT},
  {"mediumint", MEDIUMINT},
  {"middleint", MEDIUMINT},
  {"integer", INTEGER},
  {"int", INTEGER},
  {"int1", TINYINT},
  {"int2", SMALLINT},
  {"int3", MEDIUMINT},
  {"int4", INTEGER},
  {"int8", BIGINT},
  {"bigint", BIGINT},
  {"fetch", FETCH},
  {"float", FLOAT},
  {"float4", FLOAT},
  {"float8", DOUBLE},
  {"follows", FOLLOWS},
  {"double", DOUBLE},
  {"precision", PRECISION},
  {"dec", DEC},
  {"decimal", DECIMAL},
  {"numeric", NUMERIC},
  {"real", DOUBLE},
  {"bit", BIT},
  {"datetime", DATETIME},
  {"timestamp", TIMESTAMP},
  {"time", TIME},
  {"date", DATE},
  {"year", YEAR},
  {"character", CHARACTER},
  {"char", CHARACTER},
  {"text", TEXT},
  {"value", VALUE},
  {"varchar", VARCHAR},
  {"varcharacter", VARCHAR},
  {"varying", VARYING},
  {"varbinary", VARBINARY},
  {"unsigned", UNSIGNED},
  {"signed", SIGNED},
  {"zerofill", ZEROFILL},
  {"collate", COLLATE},
  {"charset", CHARSET},
  {"bool", BOOL},
  {"boolean", BOOLEAN},
  {"enum", ENUM},
  {"tinytext", TINYTEXT},
  {"mediumtext", MEDIUMTEXT},
  {"longtext", LONGTEXT},
  {"blob", BLOB},
  {"tinyblob", TINYBLOB},
  {"mediumblob", MEDIUMBLOB},
  {"longblob", LONGBLOB},
  {"fixed", FIXED},
  {"authid", AUTHID},
  {"or", OR},
  {"replace", REPLACE},
  {"pragma", PRAGMA},
  {"interface", INTERFACE},
  {"c", C},
};

const NonReservedKeyword *mysql_pl_non_reserved_keyword_lookup(const char *word)
{
  return find_word(word, mysql_pl_none_reserved_keywords_root, Mysql_pl_none_reserved_keywords);
}

//return 0 if succ, return 1 if fail
int create_mysql_pl_trie_tree()
{
  int ret = 0;
  if (0 != (ret = create_trie_tree(Mysql_pl_none_reserved_keywords, LENGTH_OF(Mysql_pl_none_reserved_keywords), &mysql_pl_none_reserved_keywords_root))) {
    (void)printf("ERROR create trie tree failed! \n");
  }
  return ret;
}

void  __attribute__((constructor)) init_mysql_pl_non_reserved_keywords_tree()
{
  int ret = 0;
  if (0 != (ret = create_mysql_pl_trie_tree())) {
    (void)printf("ERROR build mysql_pl_non_reserved_keywords tree failed=>%d", ret);
  }
}
