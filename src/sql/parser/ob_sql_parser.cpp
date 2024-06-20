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

#define USING_LOG_PREFIX SQL

#include "ob_sql_parser.h"
#include "sql/parser/parse_node.h"
#include "sql/parser/parse_malloc.h"
#include "share/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#ifndef SQL_PARSER_COMPILATION
#include "lib/ash/ob_active_session_guard.h"
#endif
#include "sql/parser/parser_utility.h"
#include <openssl/md5.h>


namespace oceanbase
{
using namespace common;
namespace sql
{
int ObSQLParser::parse(const char * str_ptr, const int64_t str_len, ParseResult &result)
{
  int ret = OB_SUCCESS;
#ifndef SQL_PARSER_COMPILATION
  // proxy don't need this, only for observer
  ObActiveSessionGuard::get_stat().in_parse_ = true;
#endif
  if (OB_FAIL(parse_init(&result))) {
    // do nothing
  } else if (OB_FAIL(parse_sql(&result, str_ptr, static_cast<size_t>(str_len)))) {
    // do nothing
  }
#ifndef SQL_PARSER_COMPILATION
  ObActiveSessionGuard::get_stat().in_parse_ = false;
#endif
  return ret;
}

int ObSQLParser::parse_and_gen_sqlid(void *malloc_pool,
                                     const char *str_ptr,
                                     const int64_t str_len,
                                     const int64_t len,
                                     char *sql_id)
{
  int ret = OB_SUCCESS;
  ParseResult *parse_result = (ParseResult *)parse_malloc(sizeof(ParseResult), malloc_pool);
  if (OB_ISNULL(parse_result)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    memset((void *)parse_result, 0, sizeof(ParseResult));
    parse_result->is_fp_ = true;
    parse_result->is_multi_query_ = false;
    parse_result->malloc_pool_ = malloc_pool;
    parse_result->is_ignore_hint_ = false;
    parse_result->need_parameterize_ = true;
    parse_result->pl_parse_info_.is_pl_parse_ = false;
    parse_result->minus_ctx_.has_minus_ = false;
    parse_result->minus_ctx_.pos_ = -1;
    parse_result->minus_ctx_.raw_sql_offset_ = -1;
    parse_result->is_for_trigger_ = false;
    parse_result->is_for_remap_ = false;
    parse_result->is_dynamic_sql_ = false;
    parse_result->is_batched_multi_enabled_split_ = false;
    parse_result->may_bool_value_ = false;
    parse_result->is_external_table_ = false;

    int64_t new_length = str_len + 1;
    char *buf = (char *)parse_malloc(new_length, parse_result->malloc_pool_);
    if (OB_UNLIKELY(NULL == buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      parse_result->param_nodes_ = NULL;
      parse_result->tail_param_node_ = NULL;
      parse_result->no_param_sql_ = buf;
      parse_result->no_param_sql_buf_len_ = new_length;
      ret = parse(str_ptr, new_length, *parse_result);
    }

    if (OB_SUCC(ret)) {
      ret = gen_sqlid(parse_result->no_param_sql_,
                      parse_result->no_param_sql_len_,
                      len,
                      sql_id);
    }
  }
  return ret;
}

int ObSQLParser::gen_sqlid(const char* paramed_sql, const int64_t sql_len,
                           const int64_t len,
                           char *sql_id)
{
  int ret = OB_SUCCESS;
  const int32_t MD5_LENGTH = 16;
  if (OB_ISNULL(sql_id) || len < 32) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    unsigned char md5_buf[MD5_LENGTH];
    unsigned char *res = MD5(reinterpret_cast<const unsigned char *>(paramed_sql),
                             sql_len, md5_buf);

    if (OB_ISNULL(res)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = parser_to_hex_cstr(md5_buf, MD5_LENGTH, sql_id, len);
    }
  }
  return ret;
}

}
}
