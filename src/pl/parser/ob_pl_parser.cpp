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
#include "pl/parser/ob_pl_parser.h"
#include "pl/parser/parse_stmt_node.h"
#include "pl/ob_pl_resolver.h"
#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/charset/ob_charset.h"
#include "lib/ash/ob_active_session_guard.h"
#include "sql/parser/parse_malloc.h"

#ifdef __cplusplus
extern "C" {
#endif
extern int obpl_parser_init(ObParseCtx *parse_ctx);
extern int obpl_parser_parse(ObParseCtx *parse_ctx);
extern ParseNode *merge_tree(void *malloc_pool, int *fatal_error, ObItemType node_tag, ParseNode *source_tree);
int obpl_parser_check_stack_overflow() {
  int ret = OB_SUCCESS;
  bool is_overflow = true;
  if (OB_FAIL(check_stack_overflow(is_overflow))) {
    LOG_WARN("failed to check stack overflow status", K(ret));
  }
  return is_overflow;
}
#ifdef __cplusplus
}
#endif

namespace oceanbase
{
using namespace common;
namespace pl
{

#define ISSPACE(c) ((c) == ' ' || (c) == '\n' || (c) == '\r' || (c) == '\t' || (c) == '\f' || (c) == '\v')
int ObPLParser::fast_parse(const ObString &query,
                      ParseResult &parse_result)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_pl_parse);
  int ret = OB_SUCCESS;
  // 删除SQL语句末尾的空格
  int64_t len = query.length();
  while (len > 0 && ISSPACE(query[len - 1])) {
    --len;
  }
  const ObString stmt_block(len, query.ptr());
  ObParseCtx parse_ctx;
  memset(&parse_ctx, 0, sizeof(ObParseCtx));
  parse_ctx.global_errno_ = OB_SUCCESS;
  parse_ctx.is_pl_fp_ = true;
  parse_ctx.mem_pool_ = &allocator_;
  parse_ctx.stmt_str_ = stmt_block.ptr();
  parse_ctx.stmt_len_ = stmt_block.length();
  parse_ctx.orig_stmt_str_ = query.ptr();
  parse_ctx.orig_stmt_len_ = query.length();
  parse_ctx.comp_mode_ = lib::is_oracle_mode();
  parse_ctx.is_for_trigger_ = 0;
  parse_ctx.is_dynamic_ = 0;
  parse_ctx.is_inner_parse_ = 1;
  parse_ctx.charset_info_ = ObCharset::get_charset(charsets4parser_.string_collation_);
  parse_result.charset_info_oracle_db_ = ObCharset::is_valid_collation(charsets4parser_.nls_collation_) ?
          ObCharset::get_charset(charsets4parser_.nls_collation_) : NULL;
  parse_ctx.is_not_utf8_connection_ = ObCharset::is_valid_collation(charsets4parser_.string_collation_) ?
        (ObCharset::charset_type_by_coll(charsets4parser_.string_collation_) != CHARSET_UTF8MB4) : false;
  parse_ctx.connection_collation_ = charsets4parser_.string_collation_;
  parse_ctx.mysql_compatible_comment_ = false;
  int64_t new_length = stmt_block.length() + 1;
  char *buf = (char *)parse_malloc(new_length, parse_ctx.mem_pool_);
  if (OB_UNLIKELY(NULL == buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("no memory for parser");
  } else {
    parse_ctx.no_param_sql_ = buf;
    parse_ctx.no_param_sql_buf_len_ = new_length;
  }
  if (OB_SUCC(ret)) {
    ret = parse_stmt_block(parse_ctx, parse_result.result_tree_);
    if (OB_ERR_PARSE_SQL == ret) {
      int err_len = 0;
      const char *err_str = "", *global_errmsg = "";
      int err_line = 0;
      if (parse_ctx.cur_error_info_ != NULL) {
        int first_column = parse_ctx.cur_error_info_->stmt_loc_.first_column_;
        int last_column = parse_ctx.cur_error_info_->stmt_loc_.last_column_;
        err_len = last_column - first_column + 1;
        err_str = parse_ctx.stmt_str_ + first_column;
        err_line = parse_ctx.cur_error_info_->stmt_loc_.last_line_ + 1;
        global_errmsg = parse_ctx.global_errmsg_;
      }
      ObString stmt(parse_ctx.stmt_len_, parse_ctx.stmt_str_);
      LOG_WARN("failed to parser pl stmt",
              K(ret), K(err_line), K(global_errmsg), K(stmt));
      LOG_USER_ERROR(OB_ERR_PARSE_SQL, ob_errpkt_strerror(OB_ERR_PARSER_SYNTAX, false),
                    err_len, err_str, err_line);
    } else {
      memmove(parse_ctx.no_param_sql_ + parse_ctx.no_param_sql_len_,
                      parse_ctx.stmt_str_ + parse_ctx.copied_pos_,
                      parse_ctx.stmt_len_ - parse_ctx.copied_pos_);
      parse_ctx.no_param_sql_len_ += parse_ctx.stmt_len_ - parse_ctx.copied_pos_;
      parse_result.no_param_sql_ = parse_ctx.no_param_sql_;
      parse_result.no_param_sql_len_ = parse_ctx.no_param_sql_len_;
      parse_result.no_param_sql_buf_len_ = parse_ctx.no_param_sql_buf_len_;
      parse_result.param_node_num_ = parse_ctx.param_node_num_;
      parse_result.param_nodes_ = parse_ctx.param_nodes_;
      parse_result.tail_param_node_ = parse_ctx.tail_param_node_;
    }
  }
  return ret;
}

int ObPLParser::parse(const ObString &stmt_block,
                      const ObString &orig_stmt_block, // for preprocess
                      ParseResult &parse_result,
                      bool is_inner_parse)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_pl_parse);
  int ret = OB_SUCCESS;
  bool is_include_old_new_in_trigger = false;
  ObQuestionMarkCtx question_mark_ctx;
  if (OB_FAIL(parse_procedure(stmt_block,
                              orig_stmt_block,
                              parse_result.result_tree_,
                              question_mark_ctx,
                              parse_result.is_for_trigger_,
                              parse_result.is_dynamic_sql_,
                              is_inner_parse,
                              is_include_old_new_in_trigger))) {
    LOG_WARN("parse stmt block failed", K(ret), K(stmt_block), K(orig_stmt_block));
  } else if (OB_ISNULL(parse_result.result_tree_)) {
    ret = OB_ERR_PARSE_SQL;
    LOG_WARN("result tree is NULL", K(stmt_block), K(ret));
  } else {
    parse_result.question_mark_ctx_ = question_mark_ctx;
    parse_result.input_sql_ = stmt_block.ptr();
    parse_result.input_sql_len_ = stmt_block.length();
    parse_result.no_param_sql_ = const_cast<char*>(stmt_block.ptr());
    parse_result.no_param_sql_len_ = stmt_block.length();
    parse_result.end_col_ = stmt_block.length();
    parse_result.is_include_old_new_in_trigger_ = is_include_old_new_in_trigger;
  }
  return ret;
}

int ObPLParser::parse_procedure(const ObString &stmt_block,
                                const ObString &orig_stmt_block,
                                ObStmtNodeTree *&multi_stmt,
                                ObQuestionMarkCtx &question_mark_ctx,
                                bool is_for_trigger,
                                bool is_dynamic,
                                bool is_inner_parse,
                                bool &is_include_old_new_in_trigger)
{
  int ret = OB_SUCCESS;
  ObParseCtx parse_ctx;
  memset(&parse_ctx, 0, sizeof(ObParseCtx));
  parse_ctx.global_errno_ = OB_SUCCESS;
  parse_ctx.mem_pool_ = &allocator_;
  parse_ctx.stmt_str_ = stmt_block.ptr();
  parse_ctx.stmt_len_ = stmt_block.length();
  parse_ctx.orig_stmt_str_ = orig_stmt_block.ptr();
  parse_ctx.orig_stmt_len_ = orig_stmt_block.length();
  parse_ctx.comp_mode_ = lib::is_oracle_mode();
  parse_ctx.is_for_trigger_ = is_for_trigger ? 1 : 0;
  parse_ctx.is_dynamic_ = is_dynamic ? 1 : 0;
  parse_ctx.is_inner_parse_ = is_inner_parse ? 1 : 0;
  parse_ctx.charset_info_ = ObCharset::get_charset(charsets4parser_.string_collation_);
  parse_ctx.is_not_utf8_connection_ = ObCharset::is_valid_collation(charsets4parser_.string_collation_) ?
        (ObCharset::charset_type_by_coll(charsets4parser_.string_collation_) != CHARSET_UTF8MB4) : false;
  parse_ctx.connection_collation_ = charsets4parser_.string_collation_;
  parse_ctx.scanner_ctx_.sql_mode_ = sql_mode_;

  ret = parse_stmt_block(parse_ctx, multi_stmt);
  if (OB_ERR_PARSE_SQL == ret) {
    int err_len = 0;
    const char *err_str = "", *global_errmsg = "";
    int err_line = 0;
    if (parse_ctx.cur_error_info_ != NULL) {
      int first_column = parse_ctx.cur_error_info_->stmt_loc_.first_column_;
      int last_column = parse_ctx.cur_error_info_->stmt_loc_.last_column_;
      err_len = last_column - first_column + 1;
      err_str = parse_ctx.stmt_str_ + first_column;
      err_line = parse_ctx.cur_error_info_->stmt_loc_.last_line_ + 1;
      global_errmsg = parse_ctx.global_errmsg_;
    }
    ObString stmt(parse_ctx.stmt_len_, parse_ctx.stmt_str_);
    LOG_WARN("failed to parser pl stmt",
             K(ret), K(err_line), K(global_errmsg), K(stmt));
    LOG_USER_ERROR(OB_ERR_PARSE_SQL, ob_errpkt_strerror(OB_ERR_PARSER_SYNTAX, false),
                   err_len, err_str, err_line);
  } else if (parse_ctx.mysql_compatible_comment_) {
    ret = OB_ERR_PARSE_SQL;
    LOG_WARN("the sql is invalid", K(ret), K(stmt_block));
  } else {
    question_mark_ctx = parse_ctx.question_mark_ctx_;
    is_include_old_new_in_trigger = parse_ctx.is_include_old_new_in_trigger_;
  }
  return ret;
}

int ObPLParser::parse_routine_body(const ObString &routine_body, ObStmtNodeTree *&routine_stmt, bool is_for_trigger)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_pl_parse);
  int ret = OB_SUCCESS;
  int32_t prefix_len = 0;
  char *buf = NULL;
  if (lib::is_oracle_mode()) {
    buf = const_cast<char*>(routine_body.ptr());
  } else {
    const char *prefix = "CALL\n";
    prefix_len = static_cast<int32_t>(strlen(prefix));
    buf = static_cast<char*>(allocator_.alloc(routine_body.length() + prefix_len));
    if (OB_UNLIKELY(NULL == buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate string buffer", "buf_len", routine_body.length() + prefix_len);
    } else {
      MEMCPY(buf, prefix, prefix_len);
      MEMCPY(buf + prefix_len, routine_body.ptr(), routine_body.length());
    }
  }

  if (OB_SUCC(ret)) {
    ObParseCtx parse_ctx;
    memset(&parse_ctx, 0, sizeof(ObParseCtx));
    parse_ctx.global_errno_ = OB_SUCCESS;
    parse_ctx.mem_pool_ = &allocator_;
    parse_ctx.stmt_str_ = buf;
    parse_ctx.stmt_len_ = routine_body.length() + prefix_len;
    parse_ctx.orig_stmt_str_ = buf;
    parse_ctx.orig_stmt_len_ = routine_body.length() + prefix_len;
    parse_ctx.is_inner_parse_ = 1;
    parse_ctx.is_for_trigger_ = is_for_trigger ? 1 : 0;
    parse_ctx.comp_mode_ = lib::is_oracle_mode();
    parse_ctx.charset_info_ = ObCharset::get_charset(charsets4parser_.string_collation_);
    parse_ctx.charset_info_oracle_db_ = ObCharset::is_valid_collation(charsets4parser_.nls_collation_) ?
          ObCharset::get_charset(charsets4parser_.nls_collation_) : NULL;
    parse_ctx.is_not_utf8_connection_ = ObCharset::is_valid_collation(charsets4parser_.string_collation_) ?
          (ObCharset::charset_type_by_coll(charsets4parser_.string_collation_) != CHARSET_UTF8MB4) : false;
    parse_ctx.connection_collation_ = charsets4parser_.string_collation_;
    parse_ctx.scanner_ctx_.sql_mode_ = sql_mode_;

    if (OB_FAIL(parse_stmt_block(parse_ctx, routine_stmt))) {
      LOG_WARN("failed to parse stmt block", K(ret));
    }
  }
  return ret;
}

int ObPLParser::parse_package(const ObString &package,
                              ObStmtNodeTree *&package_stmt,
                              const ObDataTypeCastParams &dtc_params,
                              share::schema::ObSchemaGetterGuard *schema_guard,
                              bool is_for_trigger,
                              const ObTriggerInfo *trg_info)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_pl_parse);
  int ret = OB_SUCCESS;
  ObParseCtx parse_ctx;
  memset(&parse_ctx, 0, sizeof(ObParseCtx));
  parse_ctx.global_errno_ = OB_SUCCESS;
  parse_ctx.mem_pool_ = &allocator_;
  parse_ctx.stmt_str_ = package.ptr();
  parse_ctx.stmt_len_ = package.length();
  parse_ctx.orig_stmt_str_ = package.ptr();
  parse_ctx.orig_stmt_len_ = package.length();
  parse_ctx.comp_mode_ = lib::is_oracle_mode();
  parse_ctx.is_inner_parse_ = 1;
  parse_ctx.is_for_trigger_ = is_for_trigger ? 1 : 0;
  parse_ctx.charset_info_ = ObCharset::get_charset(charsets4parser_.string_collation_);
  parse_ctx.charset_info_oracle_db_ = ObCharset::is_valid_collation(charsets4parser_.nls_collation_) ?
        ObCharset::get_charset(charsets4parser_.nls_collation_) : NULL;
  parse_ctx.is_not_utf8_connection_ = ObCharset::is_valid_collation(charsets4parser_.string_collation_) ?
        (ObCharset::charset_type_by_coll(charsets4parser_.string_collation_) != CHARSET_UTF8MB4) : false;
  parse_ctx.connection_collation_ = charsets4parser_.string_collation_;
  parse_ctx.scanner_ctx_.sql_mode_ = sql_mode_;

  if (OB_FAIL(parse_stmt_block(parse_ctx, package_stmt))) {
    LOG_WARN("failed to parse stmt block", K(ret));
  } else if (!is_for_trigger) {
    // do nothing
  } else if (OB_NOT_NULL(trg_info) && lib::is_oracle_mode()) {
    OZ (reconstruct_trigger_package(package_stmt, trg_info, dtc_params, schema_guard));
  }
  return ret;
}

int ObPLParser::parse_stmt_block(ObParseCtx &parse_ctx, ObStmtNodeTree *&multi_stmt)
{
  int ret = OB_SUCCESS;
  if (0 != obpl_parser_init(&parse_ctx)) {
    ret = OB_ERR_PARSER_INIT;
    LOG_WARN("failed to initialized parser", K(ret));
  } else if (OB_FAIL(obpl_parser_parse(&parse_ctx))) {
    if (OB_ERR_PARSE_SQL == ret && parse_ctx.is_for_preprocess_) {
      LOG_INFO("meet condition syntax, try preparse for condition compile",
               K(ret), K(parse_ctx.is_for_preprocess_));
      ObParseCtx pre_parse_ctx;
      memset(&pre_parse_ctx, 0, sizeof(ObParseCtx));
      pre_parse_ctx.global_errno_ = OB_SUCCESS;
      pre_parse_ctx.mem_pool_ = parse_ctx.mem_pool_;
      pre_parse_ctx.stmt_str_ = parse_ctx.stmt_str_;
      pre_parse_ctx.stmt_len_ = parse_ctx.stmt_len_;
      pre_parse_ctx.orig_stmt_str_ = parse_ctx.stmt_str_;
      pre_parse_ctx.orig_stmt_len_ = parse_ctx.stmt_len_;
      pre_parse_ctx.comp_mode_ = parse_ctx.comp_mode_;
      pre_parse_ctx.is_inner_parse_ = parse_ctx.is_inner_parse_;
      pre_parse_ctx.is_for_trigger_ = parse_ctx.is_for_trigger_;
      pre_parse_ctx.is_for_preprocess_ = true;
      pre_parse_ctx.connection_collation_ = parse_ctx.connection_collation_;
      pre_parse_ctx.scanner_ctx_.sql_mode_ = parse_ctx.scanner_ctx_.sql_mode_;
      if (0 != obpl_parser_init(&pre_parse_ctx)) {
        ret = OB_ERR_PARSER_INIT;
        LOG_WARN("failed to initialized parser", K(ret));
      } else if (OB_FAIL(obpl_parser_parse(&pre_parse_ctx))) {
        LOG_WARN("failed to preparse", K(ret));
      } else {
        OX (multi_stmt = pre_parse_ctx.stmt_tree_);
      }
    }
    if (OB_FAIL(ret)) {
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
      parse_ctx.stmt_tree_ = merge_tree(parse_ctx.mem_pool_, &(parse_ctx.global_errno_), T_STMT_LIST, parse_ctx.stmt_tree_);
      multi_stmt = parse_ctx.stmt_tree_;
    }
  } else {
    multi_stmt = parse_ctx.stmt_tree_;
  }
  return ret;
}

int ObPLParser::reconstruct_trigger_package(ObStmtNodeTree *&package_stmt,
                                            const ObTriggerInfo *trg_info,
                                            const ObDataTypeCastParams &dtc_params,
                                            share::schema::ObSchemaGetterGuard *schema_guard)
{
  int ret = OB_SUCCESS;
  ObString trg_define;
  int64_t buf_len = trg_info->get_trigger_body().length() + 1;
  char *buf = static_cast<char *>(allocator_.alloc(buf_len));
  int64_t pos = 0;
  OV (OB_NOT_NULL(package_stmt) && OB_NOT_NULL(trg_info) && OB_NOT_NULL(buf));
  OZ (databuff_printf(buf, buf_len, pos, "%.*s",
                      trg_info->get_trigger_body().length(),
                      trg_info->get_trigger_body().ptr()));
  OX (trg_define.assign_ptr(buf, static_cast<int32_t>(pos)));
  OZ (ObSQLUtils::convert_sql_text_from_schema_for_resolve(allocator_, dtc_params, trg_define));
  if (OB_SUCC(ret)) {
    ObStmtNodeTree *trg_tree = NULL;
    ObParseCtx trg_parse_ctx;
    memset(&trg_parse_ctx, 0, sizeof(ObParseCtx));
    trg_parse_ctx.global_errno_ = OB_SUCCESS;
    trg_parse_ctx.mem_pool_ = &allocator_;
    trg_parse_ctx.stmt_str_ = trg_define.ptr();
    trg_parse_ctx.stmt_len_ = trg_define.length();
    trg_parse_ctx.orig_stmt_str_ = trg_define.ptr();
    trg_parse_ctx.orig_stmt_len_ = trg_define.length();
    trg_parse_ctx.comp_mode_ = 1;
    trg_parse_ctx.is_inner_parse_ = 1;
    trg_parse_ctx.is_for_trigger_ = 1;
    trg_parse_ctx.charset_info_ = ObCharset::get_charset(charsets4parser_.string_collation_);
    trg_parse_ctx.charset_info_oracle_db_ = ObCharset::is_valid_collation(charsets4parser_.nls_collation_) ?
          ObCharset::get_charset(charsets4parser_.nls_collation_) : NULL;
    trg_parse_ctx.is_not_utf8_connection_ = ObCharset::is_valid_collation(charsets4parser_.string_collation_) ?
        (ObCharset::charset_type_by_coll(charsets4parser_.string_collation_) != CHARSET_UTF8MB4) : false;
    trg_parse_ctx.connection_collation_ = charsets4parser_.string_collation_;
    if (OB_FAIL(parse_stmt_block(trg_parse_ctx, trg_tree))) {
      LOG_WARN("failed to parse trigger", K(ret));
    } else if (OB_ISNULL(trg_tree)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("trg_tree or package_stmt is NULL", K(ret));
    } else {
      ObStmtNodeTree *pkg_body = NULL;
      const ObStmtNodeTree *trigger_source_node = NULL;
      CK (1 == trg_tree->num_child_);
      CK (OB_NOT_NULL(trigger_source_node = trg_tree->children_[0])); // T_SP_PRE_STMTS or T_TG_SOURCE
      if (OB_SUCC(ret) && T_SP_PRE_STMTS == trigger_source_node->type_) {
        OV (OB_NOT_NULL(schema_guard));
        OZ (pl::ObPLResolver::resolve_condition_compile(allocator_, 
                                                        NULL,
                                                        schema_guard,
                                                        NULL,
                                                        NULL,
                                                        &(trg_info->get_package_exec_env()),
                                                        trigger_source_node,
                                                        trigger_source_node,
                                                        true /*inner_parse*/,
                                                        true /*is_trigger*/));
        OV (OB_NOT_NULL(trigger_source_node)); // T_TG_SOURCE
      }
      CK (2 == trigger_source_node->num_child_);
      CK (OB_NOT_NULL(trigger_source_node->children_[1])); // trigger definition

      CK (T_STMT_LIST == package_stmt->type_ && 1 == package_stmt->num_child_);
      CK (OB_NOT_NULL(package_stmt->children_[0]));
      CK (4 == package_stmt->children_[0]->num_child_);
      CK (OB_NOT_NULL(pkg_body = package_stmt->children_[0]->children_[1]));
      CK (T_PACKAGE_BODY_STMTS == pkg_body->type_);

      if (trg_info->is_simple_dml_type() || trg_info->is_instead_dml_type()) {
        OV (T_TG_SIMPLE_DML == trigger_source_node->children_[1]->type_
            || T_TG_INSTEAD_DML == trigger_source_node->children_[1]->type_);
        CK (5 == trigger_source_node->children_[1]->num_child_);
        CK (OB_NOT_NULL(trigger_source_node->children_[1]->children_[4])); // simple/instead trigger body

        CK (5 == pkg_body->num_child_); // 5 routines predefined in trigger_package

        if (OB_SUCC(ret) && NULL != trigger_source_node->children_[1]->children_[3]) {
          // replace routine `calc_when` with trigger's `when_condition`
          // opt_when_condition
          OV (T_TG_WHEN_CONDITION == trigger_source_node->children_[1]->children_[3]->type_);
          OV (1 == trigger_source_node->children_[1]->children_[3]->num_child_);
          OV (OB_NOT_NULL(trigger_source_node->children_[1]->children_[3]->children_[0])); // bool expr

          OV (OB_NOT_NULL(pkg_body->children_[0]->children_[1])); // T_SP_BLOCK_CONTENT
          OV (1 == pkg_body->children_[0]->children_[1]->num_child_);

          OV (OB_NOT_NULL(pkg_body->children_[0]->children_[1]->children_[0])); // T_SP_PROC_STMT_LIST
          OV (1 == pkg_body->children_[0]->children_[1]->children_[0]->num_child_);

          OV (OB_NOT_NULL(pkg_body->children_[0]->children_[1]->children_[0]->children_[0])); // T_SP_RETURN
          OV (1 == pkg_body->children_[0]->children_[1]->children_[0]->children_[0]->num_child_);

          OV (OB_NOT_NULL(pkg_body->children_[0]->children_[1]->children_[0]->children_[0]->children_[0])); // bool expr

          OV (pkg_body->children_[0]->children_[1]->children_[0]->children_[0]->children_[0]->type_
              == trigger_source_node->children_[1]->children_[3]->children_[0]->type_);
          OX (pkg_body->children_[0]->children_[1]->children_[0]->children_[0]->children_[0]
              = trigger_source_node->children_[1]->children_[3]->children_[0]);
        }

        if (OB_SUCC(ret)) {
          int64_t proc_pos = -1;
          if (trg_info->has_before_stmt_point()) {
            proc_pos = 1;
          } else if (trg_info->has_before_row_point()) {
            proc_pos = 2;
          } else if (trg_info->has_after_row_point()) {
            proc_pos = 3;
          } else if (trg_info->has_after_stmt_point()) {
            proc_pos = 4;
          }
          CK (T_SUB_PROC_DEF == pkg_body->children_[proc_pos]->type_);
          CK (2 == pkg_body->children_[proc_pos]->num_child_); // pl_impl_body
          CK (OB_NOT_NULL(pkg_body->children_[proc_pos]->children_[1]));
          CK (T_SP_BLOCK_CONTENT == pkg_body->children_[proc_pos]->children_[1]->type_);

          if (OB_FAIL(ret)) {
          } else if (T_SP_BLOCK_CONTENT == trigger_source_node->children_[1]->children_[4]->type_) {
            pkg_body->children_[proc_pos]->children_[1] = trigger_source_node->children_[1]->children_[4];
          } else if (T_SP_LABELED_BLOCK == trigger_source_node->children_[1]->children_[4]->type_) {
            pkg_body->children_[proc_pos]->children_[1] =
              trigger_source_node->children_[1]->children_[4]->children_[1];
          }
        }
      } else if (trg_info->is_compound_dml_type()) {
        ObStmtNodeTree *trg_com_body = NULL;
        OV (T_TG_COMPOUND_DML == trigger_source_node->children_[1]->type_);
        OV (5 == trigger_source_node->children_[1]->num_child_);
        OV (OB_NOT_NULL(trg_com_body = trigger_source_node->children_[1]->children_[4])); // compoud trigger body
        OV (T_TG_COMPOUND_BODY == trg_com_body->type_ && 3 == trg_com_body->num_child_);
        OV (OB_NOT_NULL(trg_com_body->children_[1]));// timing_point_section_list
        OV (T_TG_TIMPING_POINT_SECTION_LIST == trg_com_body->children_[1]->type_);

        if (OB_SUCC(ret) && NULL != trigger_source_node->children_[1]->children_[3]) {
          // replace `calc_when` routine with trigger's `when_condition`
          // opt_when_condition
          OV (T_TG_WHEN_CONDITION == trigger_source_node->children_[1]->children_[3]->type_);
          OV (OB_NOT_NULL(trigger_source_node->children_[1]->children_[3]->children_[0])); // bool expr

          OV (OB_NOT_NULL(pkg_body->children_[0]) && 2 == pkg_body->children_[0]->num_child_); // T_SUB_FUNC_DEF
          
          OV (OB_NOT_NULL(pkg_body->children_[0]->children_[1])); // T_SP_BLOCK_CONTENT
          OV (1 == pkg_body->children_[0]->children_[1]->num_child_);

          OV (OB_NOT_NULL(pkg_body->children_[0]->children_[1]->children_[0])); // T_SP_PROC_STMT_LIST
          OV (1 == pkg_body->children_[0]->children_[1]->children_[0]->num_child_);

          OV (OB_NOT_NULL(pkg_body->children_[0]->children_[1]->children_[0]->children_[0])); // T_SP_RETURN
          OV (1 == pkg_body->children_[0]->children_[1]->children_[0]->children_[0]->num_child_);

          OV (OB_NOT_NULL(pkg_body->children_[0]->children_[1]->children_[0]->children_[0]->children_[0])); // bool expr

          OV (pkg_body->children_[0]->children_[1]->children_[0]->children_[0]->children_[0]->type_
              == trigger_source_node->children_[1]->children_[3]->children_[0]->type_);
          OX (pkg_body->children_[0]->children_[1]->children_[0]->children_[0]->children_[0]
              = trigger_source_node->children_[1]->children_[3]->children_[0]);
        }

        if (OB_SUCC(ret) && NULL != trg_com_body->children_[0]) {
          // replace trigger_package's vars by trigger's vars
          // opt_decl_stmt_ext_list
          OV (T_PACKAGE_BODY_STMTS == trg_com_body->children_[0]->type_);
          // 5 routines predefined in trigger_package
          OV (5 == (pkg_body->num_child_ - trg_com_body->children_[0]->num_child_));
          for (uint64_t i = 0; OB_SUCC(ret) && i < trg_com_body->children_[0]->num_child_; i++) {
            OV (OB_NOT_NULL(trg_com_body->children_[0]->children_[i]));
            OV (OB_NOT_NULL(pkg_body->children_[i + 1])); // need +1, because the first is `calc_when`
            OX (pkg_body->children_[i + 1] = trg_com_body->children_[0]->children_[i]);
          }
        }

        if (OB_SUCC(ret)) {
          // replace 4 routines in trigger_package with trigger's events
          ObStmtNodeTree *timing_list = trg_com_body->children_[1];
          int64_t decl_num = pkg_body->num_child_;
          int16_t timing = 0;
          int16_t level = 0;
          for (int64_t i = 0; OB_SUCC(ret) && i < timing_list->num_child_; i++) {
            OV (OB_NOT_NULL(timing_list->children_[i]));
            OV (OB_NOT_NULL(timing_list->children_[i]->children_[0]));
            OX (timing = timing_list->children_[i]->int16_values_[0]);
            OX (level = timing_list->children_[i]->int16_values_[1]);
            if (OB_FAIL(ret)) {
            } else if (T_BEFORE == timing && T_TP_STATEMENT == level) {
              OV (OB_NOT_NULL(pkg_body->children_[decl_num - 4]));
              OX (pkg_body->children_[decl_num - 4]->children_[1] = timing_list->children_[i]->children_[0]);
            } else if ((T_BEFORE == timing && T_TP_EACH_ROW == level) || T_INSTEAD == timing) {
              OV (OB_NOT_NULL(pkg_body->children_[decl_num - 3]));
              OX (pkg_body->children_[decl_num - 3]->children_[1] = timing_list->children_[i]->children_[0]);
            } else if (T_AFTER == timing && T_TP_EACH_ROW == level) {
              OV (OB_NOT_NULL(pkg_body->children_[decl_num - 2]));
              OX (pkg_body->children_[decl_num - 2]->children_[1] = timing_list->children_[i]->children_[0]);
            } else if (T_AFTER == timing && T_TP_STATEMENT == level) {
              OV (OB_NOT_NULL(pkg_body->children_[decl_num - 1]));
              OX (pkg_body->children_[decl_num - 1]->children_[1] = timing_list->children_[i]->children_[0]);
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("timing point is error", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

}  // namespace pl
}  // namespace oceanbase
