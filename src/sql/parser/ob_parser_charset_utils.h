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
