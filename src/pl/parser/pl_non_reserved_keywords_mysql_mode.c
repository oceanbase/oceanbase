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
#include "sql/parser/ob_non_reserved_keywords.h"
#include "pl/parser/pl_parser_mysql_mode_tab.h"

static t_node *MYSQL_PL_RESERVED_KEYWORDS_ROOT = NULL;
static t_node *MYSQL_PL_NON_RESERVED_KEYWORDS_ROOT = NULL;

/* List of non-reserved keywords */
/*一开始会对这些word建立一颗trie树，对于每次的查找来言，树是固定的
 *若新添加的keyword含有除大小写字母、'_'和数字以外的其它字符，请联系@叶提修改这颗树。
 *实现不需要保证字典序，但是原则上还是保证字典序，方便维护和查找*/
static const NonReservedKeyword MYSQL_PL_NON_RESERVED_KEYWORDS[] =
{
  {"after", AFTER},
  {"at", AT},
  {"begin", BEGIN_KEY},
  {"binary_integer", BINARY_INTEGER},
  {"body", BODY},
  {"catalog_name", CATALOG_NAME},
  {"class_origin", CLASS_ORIGIN},
  {"close", CLOSE},
  {"column_name", COLUMN_NAME},
  {"comment", COMMENT},
  {"compile", COMPILE},
  {"completion", COMPLETION},
  {"constraint_catalog", CONSTRAINT_CATALOG},
  {"constraint_name", CONSTRAINT_NAME},
  {"constraint_schema", CONSTRAINT_SCHEMA},
  {"cursor_name", CURSOR_NAME},
  {"definer", DEFINER},
  {"disable", DISABLE},
  {"enable", ENABLE},
  {"end", END_KEY},
  {"ends", ENDS},
  {"event", EVENT},
  {"every", EVERY},
  {"extend", EXTEND},
  {"found", FOUND},
  {"handler", HANDLER},
  {"message_text", MESSAGE_TEXT},
  {"mysql_errno", MYSQL_ERRNO},
  {"national", NATIONAL},
  {"nchar", NCHAR},
  {"next", NEXT},
  {"nvarchar", NVARCHAR},
  {"of", OF},
  {"open", OPEN},
  {"package", PACKAGE},
  {"precedes", PRECEDES},
  {"properties", PROPERTIES},
  {"table_name", TABLE_NAME},
  {"type", TYPE},
  {"record", RECORD},
  {"returns", RETURNS},
  {"reuse", REUSE},
  {"row", ROW},
  {"rowtype", ROWTYPE},
  {"role", ROLE},
  {"schedule", SCHEDULE},
  {"schema_name", SCHEMA_NAME},
  {"starts", STARTS},
  {"subclass_origin", SUBCLASS_ORIGIN},
  {"preserve", PRESERVE},
  {"user", USER},
  {"language", LANGUAGE},
  {"no", NO},
  {"contains", CONTAINS},
  {"data", DATA},
  {"constraint_origin", CONSTRAINT_ORIGIN},
  {"invoker", INVOKER},
  {"security", SECURITY},
  {"follows", FOLLOWS},
  {"bit", BIT},
  {"datetime", DATETIME},
  {"timestamp", TIMESTAMP},
  {"time", TIME},
  {"date", DATE},
  {"year", YEAR},
  {"settings", SETTINGS},
  {"month", MONTH},
  {"day", DAY},
  {"hour", HOUR},
  {"minute", MINUTE},
  {"second", SECOND},
  {"text", TEXT},
  {"value", VALUE},
  {"signed", SIGNED},
  {"bool", BOOL},
  {"boolean", BOOLEAN},
  {"enum", ENUM},
  {"fixed", FIXED},
  {"authid", AUTHID},
  {"pragma", PRAGMA},
  {"interface", INTERFACE},
  {"c", C},
  {"submit", SUBMIT},
  {"job", JOB},
  {"cancel", CANCEL},
  {"xa", XA},
  {"recover", RECOVER},
  {"polygon", POLYGON},
  {"multipoint", MULTIPOINT},
  {"point", POINT},
  {"linestring", LINESTRING},
  {"geometry", GEOMETRY},
  {"multilinestring", MULTILINESTRING},
  {"multipolygon", MULTIPOLYGON},
  {"geometrycollection", GEOMETRYCOLLECTION},
  {"geomcollection", GEOMCOLLECTION},
  {"roaringbitmap", ROARINGBITMAP},
  {"interval", INTERVAL},
  {"to", TO},
  {"serial", SERIAL},
};

static const ReservedKeyword MYSQL_PL_RESERVED_KEYWORDS[] =
{
  {"before", BEFORE},
  {"by", BY},
  {"case", CASE},
  {"commit", COMMIT},
  {"condition", CONDITION},
  {"continue", CONTINUE},
  {"current_user", CURRENT_USER},
  {"cursor", CURSOR},
  {"declare", DECLARE},
  {"default", DEFAULT},
  {"delete", DELETE},
  {"deterministic", DETERMINISTIC},
  {"each", EACH},
  {"else", ELSE},
  {"elseif", ELSEIF},
  {"exists", EXISTS},
  {"exit", EXIT},
  {"for", FOR},
  {"from", FROM},
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
  {"not", NOT},
  {"on", ON},
  {"out", OUT},
  {"then", THEN},
  {"repeat", REPEAT},
  {"resignal", RESIGNAL},
  {"return", RETURN},
  {"rollback", ROLLBACK},
  {"signal", SIGNAL},
  {"sqlexception", SQLEXCEPTION},
  {"sqlstate", SQLSTATE},
  {"sqlwarning", SQLWARNING},
  {"until", UNTIL},
  {"update", UPDATE},
  {"using", USING},
  {"when", WHEN},
  {"while", WHILE},
  {"sql", SQL},
  {"reads", READS},
  {"modifies", MODIFIES},
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
  {"double", DOUBLE},
  {"precision", PRECISION},
  {"dec", DEC},
  {"decimal", DECIMAL},
  {"numeric", NUMERIC},
  {"real", DOUBLE},
  {"character", CHARACTER},
  {"char", CHARACTER},
  {"varchar", VARCHAR},
  {"varcharacter", VARCHAR},
  {"varying", VARYING},
  {"varbinary", VARBINARY},
  {"unsigned", UNSIGNED},
  {"zerofill", ZEROFILL},
  {"collate", COLLATE},
  {"charset", CHARSET},
  {"tinytext", TINYTEXT},
  {"mediumtext", MEDIUMTEXT},
  {"longtext", LONGTEXT},
  {"blob", BLOB},
  {"tinyblob", TINYBLOB},
  {"mediumblob", MEDIUMBLOB},
  {"longblob", LONGBLOB},
  {"or", OR},
  {"replace", REPLACE}
};

const ReservedKeyword *get_mysql_pl_reserved_keywords(int32_t *count)
{
  if (NULL != count) {
    *count = LENGTH_OF(MYSQL_PL_RESERVED_KEYWORDS);
  }
  return MYSQL_PL_RESERVED_KEYWORDS;
}

const NonReservedKeyword *get_mysql_pl_non_reserved_keywords(int32_t *count)
{
  if (NULL != count) {
    *count = LENGTH_OF(MYSQL_PL_NON_RESERVED_KEYWORDS);
  }
  return MYSQL_PL_NON_RESERVED_KEYWORDS;
}

const ReservedKeyword *mysql_pl_reserved_keyword_lookup(const char *word)
{
  return find_word(word, MYSQL_PL_RESERVED_KEYWORDS_ROOT, MYSQL_PL_RESERVED_KEYWORDS);
}

const NonReservedKeyword *mysql_pl_non_reserved_keyword_lookup(const char *word)
{
  return find_word(word, MYSQL_PL_NON_RESERVED_KEYWORDS_ROOT, MYSQL_PL_NON_RESERVED_KEYWORDS);
}

const NonReservedKeyword *mysql_pl_keyword_lookup(const char *word)
{
  const NonReservedKeyword *result = find_word(word, MYSQL_PL_RESERVED_KEYWORDS_ROOT, MYSQL_PL_RESERVED_KEYWORDS);
  if (NULL == result) {
    result = find_word(word, MYSQL_PL_NON_RESERVED_KEYWORDS_ROOT, MYSQL_PL_NON_RESERVED_KEYWORDS);
  }
  return result;
}

int create_mysql_pl_trie_tree()
{
  int ret = 0;
  if (0 != (ret = create_trie_tree(MYSQL_PL_RESERVED_KEYWORDS, LENGTH_OF(MYSQL_PL_RESERVED_KEYWORDS), &MYSQL_PL_RESERVED_KEYWORDS_ROOT))) {
    (void)printf("ERROR create mysql pl reserved trie tree failed!\n");
  } else if (0 != (ret = create_trie_tree(MYSQL_PL_NON_RESERVED_KEYWORDS, LENGTH_OF(MYSQL_PL_NON_RESERVED_KEYWORDS), &MYSQL_PL_NON_RESERVED_KEYWORDS_ROOT))) {
    (void)printf("ERROR create mysql pl non reserved trie tree failed!\n");
  }
  return ret;
}

void __attribute__((constructor)) init_mysql_pl_keywords_tree()
{
  int ret = 0;
  if (0 != (ret = create_mysql_pl_trie_tree())) {
    (void)printf("ERROR build mysql_pl_keywords tree failed=>%d", ret);
  }
}
