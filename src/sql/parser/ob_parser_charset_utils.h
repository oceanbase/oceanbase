/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_OB_PARSER_CHARSET_UTILS_H
#define OCEANBASE_COMMON_OB_PARSER_CHARSET_UTILS_H

#ifdef __cplusplus
extern "C"
{
#endif


typedef enum ObCharsetParserType_ {
  CHARSET_PARSER_TYPE_NONE = 0,
  CHARSET_PARSER_TYPE_GB,
  CHARSET_PARSER_TYPE_SINGLE_BYTE,
  CHARSET_PARSER_TYPE_UTF8MB4,
  CHARSET_PARSER_TYPE_HKSCS,
  CHARSET_PARSER_TYPE_MAX,
} ObCharsetParserType;

int obcharset_get_parser_type_by_coll(const int collation_type, ObCharsetParserType *parser_type);

#ifdef __cplusplus
}
#endif
#endif //OCEANBASE_COMMON_OB_PARSER_CHARSET_UTILS_H
