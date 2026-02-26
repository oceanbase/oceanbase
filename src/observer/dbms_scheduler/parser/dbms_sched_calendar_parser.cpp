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

#define USING_LOG_PREFIX PL
#include "dbms_sched_calendar_parser.h"
#include "pl/ob_pl_resolver.h"
#include "sql/parser/parse_malloc.h"

#ifdef __cplusplus
extern "C" {
#endif
extern int ob_dbms_sched_parser_calendar_init(ObParseCtx *parse_ctx);
extern int ob_dbms_sched_parser_calendar_parse(ObParseCtx *parse_ctx);
extern ParseNode *merge_tree(void *malloc_pool, int *fatal_error, ObItemType node_tag, ParseNode *source_tree);
#ifdef __cplusplus
}
#endif

namespace oceanbase
{
using namespace common;
namespace dbms_scheduler
{

int ObDBMSSchedCalendarParser::parse(const ObString &calendar_body, ObStmtNodeTree *& parser_tree)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_pl_parse);
  int ret = OB_SUCCESS;
  ObParseCtx parse_ctx;
  memset(&parse_ctx, 0, sizeof(ObParseCtx));
  parse_ctx.global_errno_ = OB_SUCCESS;
  parse_ctx.mem_pool_ = &allocator_;
  parse_ctx.stmt_str_ = calendar_body.ptr();
  parse_ctx.stmt_len_ = calendar_body.length();
  parse_ctx.orig_stmt_str_ = calendar_body.ptr();
  parse_ctx.orig_stmt_len_ = calendar_body.length();
  parse_ctx.comp_mode_ = lib::is_oracle_mode();
  parse_ctx.is_inner_parse_ = 1;
  parse_ctx.is_for_trigger_ = 0;
  parse_ctx.charset_info_ = ObCharset::get_charset(charsets4parser_.string_collation_);
  parse_ctx.charset_info_oracle_db_ = ObCharset::is_valid_collation(charsets4parser_.nls_collation_) ?
        ObCharset::get_charset(charsets4parser_.nls_collation_) : NULL;
  parse_ctx.is_not_utf8_connection_ = ObCharset::is_valid_collation(charsets4parser_.string_collation_) ?
        (ObCharset::charset_type_by_coll(charsets4parser_.string_collation_) != CHARSET_UTF8MB4) : false;
  parse_ctx.connection_collation_ = charsets4parser_.string_collation_;
  parse_ctx.scanner_ctx_.sql_mode_ = sql_mode_;

  if (0 != ob_dbms_sched_parser_calendar_init(&parse_ctx)) {
    ret = OB_ERR_PARSER_INIT;
    LOG_WARN("failed to initialized parser", K(ret));
  } else if (OB_FAIL(ob_dbms_sched_parser_calendar_parse(&parse_ctx))) {
    LOG_WARN("failed to parse the statement",
              "orig_stmt_str", ObString(parse_ctx.orig_stmt_len_, parse_ctx.orig_stmt_str_),
              "stmt_str", ObString(parse_ctx.stmt_len_, parse_ctx.stmt_str_),
              K_(parse_ctx.global_errmsg),
              K_(parse_ctx.global_errno),
              K_(parse_ctx.is_for_trigger),
              K_(parse_ctx.is_dynamic),
              K_(parse_ctx.is_for_preprocess),
              K(ret));
    if (OB_NOT_SUPPORTED == ret) {
      LOG_USER_ERROR(OB_NOT_SUPPORTED, parse_ctx.global_errmsg_);
    }
    parser_tree = parse_ctx.stmt_tree_;
  } else {
    parser_tree = parse_ctx.stmt_tree_;
  }
  return ret;
}

}  // namespace dbms_scheduler
}  // namespace oceanbase
