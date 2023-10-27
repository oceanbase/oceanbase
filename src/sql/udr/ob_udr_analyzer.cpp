/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


#define USING_LOG_PREFIX SQL_QRR
#include "lib/utility/ob_print_utils.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "common/ob_smart_call.h"
#include "sql/plan_cache/ob_sql_parameterization.h"
#include "sql/udr/ob_udr_analyzer.h"

namespace oceanbase
{
namespace sql
{

bool ObUDRAnalyzer::check_is_allow_stmt_type(stmt::StmtType stmt_type)
{
  return (stmt_type == stmt::T_SELECT
          || stmt_type == stmt::T_INSERT
          || stmt_type == stmt::T_REPLACE
          || stmt_type == stmt::T_MERGE
          || stmt_type == stmt::T_DELETE
          || stmt_type == stmt::T_UPDATE
          || stmt_type == stmt::T_VARIABLE_SET);
}

int ObUDRAnalyzer::parse_and_resolve_stmt_type(const common::ObString &sql,
                                               ParseResult &parse_result,
                                               stmt::StmtType &stmt_type)
{
  int ret = OB_SUCCESS;
  stmt_type = stmt::T_NONE;
  ObParser parser(allocator_, sql_mode_, charsets4parser_);
  if (OB_FAIL(multiple_query_check(sql))) {
    LOG_WARN("failed to check multiple query check", K(ret));
  } else if (OB_FAIL(parser.parse(sql, parse_result))) {
    LOG_WARN("generate syntax tree failed", K(sql), K(ret));
  } else if (OB_FAIL(ObResolverUtils::resolve_stmt_type(parse_result, stmt_type))) {
    LOG_WARN("failed to resolve stmt type", K(ret));
  }
  return ret;
}

int ObUDRAnalyzer::find_leftest_const_node(ParseNode &cur_node, ParseNode *&const_node)
{
  int ret = OB_SUCCESS;
  if (T_QUESTIONMARK == cur_node.type_) {
    const_node = &cur_node;
  } else if (T_OP_MUL == cur_node.type_ || T_OP_DIV == cur_node.type_ || T_OP_MOD == cur_node.type_) {
    // syntax tree of '1 - (?-3)/4' is
    //     -
    //    / \
    //   1  div
    //     /  \
    //    -    4
    //   / \
    //  2   3
    // at this time '-' cannot be combined with 2, that is, the syntax tree cannot be converted
    if (OB_ISNULL(cur_node.children_) || 2 != cur_node.num_child_
        || OB_ISNULL(cur_node.children_[0]) || OB_ISNULL(cur_node.children_[1])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument");
    } else if (OB_FAIL(SMART_CALL(find_leftest_const_node(*cur_node.children_[0], const_node)))) {
      LOG_WARN("failed to find leftest const node", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObUDRAnalyzer::check_transform_minus_op(ParseNode *tree)
{
  int ret = OB_SUCCESS;
  if (T_OP_MINUS != tree->type_) {
    // do nothing
  } else if (2 != tree->num_child_
             || OB_ISNULL(tree->children_)
             || OB_ISNULL(tree->children_[1])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid minus tree", K(ret));
  } else if (1 == tree->children_[1]->is_assigned_from_child_) {
    // select 1 - (2) from dual;
    // select 1 - (2/3/4) from dual;
    // do nothing
  } else if (T_QUESTIONMARK == tree->children_[1]->type_) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "template statement type");
    LOG_WARN("template statement type not supported", K(ret));
  } else if (T_OP_MUL == tree->children_[1]->type_
             || T_OP_DIV == tree->children_[1]->type_
             || (lib::is_mysql_mode() && T_OP_MOD == tree->children_[1]->type_)) {
    // '0 - 2 * 3' should be transformed to '0 + (-2) * 3'
    // '0 - 2 / 3' should be transformed to '0 + (-2) / 3'
    // '0 - 4 mod 3' should be transformed to '0 + (-4 mod 3)'
    // '0 - 2/3/4' => '0 + (-2/3/4)'
    // notify that, syntax tree of '0 - 2/3/4' is
    //           -
    //         /   \
    //        0    div
    //            /   \
    //          div    4
    //         /   \
    //        2     3
    // so, we need to find the leftest leave node and change its value and str
    // same for '%','*', mod
    ParseNode *const_node = NULL;
    ParseNode *op_node = tree->children_[1];
    if (OB_FAIL(find_leftest_const_node(*op_node, const_node))) {
      LOG_WARN("failed to find leftest const node", K(ret));
    } else if (OB_ISNULL(const_node)) {
      // do nothing
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "template statement type");
      LOG_WARN("template statement type not supported", K(ret));
    }
  }
  return ret;
}

int ObUDRAnalyzer::traverse_and_check(ParseNode *tree)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stack_overflow())) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (OB_ISNULL(tree)) {
    // do nothing
  } else if (OB_FAIL(check_transform_minus_op(tree))) {
    LOG_WARN("failed to check transform minus op", K(ret));
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < tree->num_child_; ++i) {
      if (OB_ISNULL(tree->children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid argument");
      } else {
        ret = SMART_CALL(traverse_and_check(tree->children_[i]));
      }
    }
  }
  return ret;
}

int ObUDRAnalyzer::multiple_query_check(const ObString &sql)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 1> queries;
  ObParser parser(allocator_, sql_mode_, charsets4parser_);
  ObMPParseStat parse_stat;
  if (OB_FAIL(parser.split_multiple_stmt(sql, queries, parse_stat))) {
    LOG_WARN("failed to split multiple stmt", K(ret), K(sql));
  } else if (OB_UNLIKELY(queries.count() <= 0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "empty sql");
    LOG_WARN("empty sql not supported", K(sql));
  } else if (OB_UNLIKELY(queries.count() > 1)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "multi-statement query");
    LOG_WARN("multi-statement query not supported", K(ret), K(queries.count()));
  }
  return ret;
}

int ObUDRAnalyzer::parse_and_check(const common::ObString &pattern,
                                   const common::ObString &replacement)
{
  int ret = OB_SUCCESS;
  int64_t l_param_cnt = 0;
  int64_t r_param_cnt = 0;
  ParseResult l_parse_result;
  ParseResult r_parse_result;
  stmt::StmtType l_stmt_type = stmt::T_NONE;
  stmt::StmtType r_stmt_type = stmt::T_NONE;
  if (pattern.empty() || replacement.empty()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "empty sql");
    LOG_WARN("empty sql not supported", K(pattern), K(replacement));
  } else if (OB_FAIL(parse_and_resolve_stmt_type(pattern, l_parse_result, l_stmt_type))) {
    LOG_WARN("failed to parser and resolve stmt type", K(ret));
  } else if (OB_FAIL(parse_and_resolve_stmt_type(replacement, r_parse_result, r_stmt_type))) {
    LOG_WARN("failed to parser and resolve stmt type", K(ret));
  } else if (!check_is_allow_stmt_type(l_stmt_type) || !check_is_allow_stmt_type(r_stmt_type)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "SQL types in user defined rule");
    LOG_WARN("SQL type in user defined rule not supported", K(l_stmt_type), K(r_stmt_type), K(ret));
  } else if (OB_FAIL(traverse_and_check(l_parse_result.result_tree_))) {
    LOG_WARN("failed to traverse and check", K(ret));
  } else if (FALSE_IT(l_param_cnt = l_parse_result.question_mark_ctx_.count_)) {
  } else if (FALSE_IT(r_param_cnt = r_parse_result.question_mark_ctx_.count_)) {
  } else if (l_param_cnt != r_param_cnt) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "the number of question marks is not equal");
    LOG_WARN("the number of question marks is not equal", K(ret), K(l_param_cnt), K(r_param_cnt));
  } else if (l_parse_result.question_mark_ctx_.count_ > 0
    || r_parse_result.question_mark_ctx_.count_ > 0) {
    const ObQuestionMarkCtx &l_question_mark_ctx = l_parse_result.question_mark_ctx_;
    const ObQuestionMarkCtx &r_question_mark_ctx = r_parse_result.question_mark_ctx_;
    if ((!l_question_mark_ctx.by_name_ && !l_question_mark_ctx.by_ordinal_)
      || (!r_question_mark_ctx.by_name_ && !r_question_mark_ctx.by_ordinal_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "binding methods");
      LOG_WARN("binding methods not supported");
    } else if (l_question_mark_ctx.by_ordinal_ != r_question_mark_ctx.by_ordinal_
      || l_question_mark_ctx.by_name_ != r_question_mark_ctx.by_name_) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "different binding methods");
      LOG_WARN("different binding methods not supported");
    } else if (l_question_mark_ctx.count_ != r_question_mark_ctx.count_) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "question marks are not equal");
      LOG_WARN("question marks are not equal not supported");
    } else if (l_question_mark_ctx.count_ > 0 && l_question_mark_ctx.by_name_) {
      std::sort(l_question_mark_ctx.name_, l_question_mark_ctx.name_ + l_question_mark_ctx.count_,
                [](char const *lhs, char const *rhs) {
                  return STRCASECMP(lhs, rhs) < 0;
                });
      std::sort(r_question_mark_ctx.name_, r_question_mark_ctx.name_ + r_question_mark_ctx.count_,
                [](char const *lhs, char const *rhs) {
                  return STRCASECMP(lhs, rhs) < 0;
                });
      for (int64_t i = 0; OB_SUCC(ret) && i < l_question_mark_ctx.count_; ++i) {
        if (0 != STRCASECMP(l_question_mark_ctx.name_[i], r_question_mark_ctx.name_[i])) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "question marks are defined with different names");
          LOG_WARN("question marks are defined with different names not supported");
        }
      }
    }
  }
  return ret;
}

int ObUDRAnalyzer::add_dynamic_param_info(
    const int64_t raw_param_idx,
    const int64_t question_mark_idx,
    DynamicParamInfoArray &dynamic_param_infos)
{
  int ret = OB_SUCCESS;
  DynamicParamInfo param_info;
  param_info.raw_param_idx_ = raw_param_idx;
  param_info.question_mark_idx_ = question_mark_idx;
  if (OB_FAIL(dynamic_param_infos.push_back(param_info))) {
    LOG_WARN("failed to add dynamic param info", K(ret), K(param_info));
  } else {
    LOG_DEBUG("succ to add dynamic param info", K(param_info));
  }
  return ret;
}

int ObUDRAnalyzer::add_fixed_param_value(
    const int64_t raw_param_idx,
    const ParseNode *raw_param,
    FixedParamValueArray &fixed_param_infos)
{
  int ret = OB_SUCCESS;
  FixedParamValue param_value;
  param_value.idx_ = raw_param_idx;
  param_value.raw_text_.assign(const_cast<char *>(raw_param->raw_text_), raw_param->text_len_);
  if (OB_FAIL(fixed_param_infos.push_back(param_value))) {
    LOG_WARN("failed to add fixed param info", K(ret), K(param_value));
  } else {
    LOG_DEBUG("succ to add fixed param value", K(param_value));
  }
  return ret;
}

int ObUDRAnalyzer::cons_raw_param_infos(
    const ObIArray<ObPCParam*> &raw_params,
    FixedParamValueArray &fixed_param_infos,
    DynamicParamInfoArray &dynamic_param_infos)
{
  int ret = OB_SUCCESS;
  ParseNode *raw_param = NULL;
  ObPCParam *pc_param = NULL;
  common::ObBitSet<> mark_pos;
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_params.count(); ++i) {
    if (OB_ISNULL(pc_param = raw_params.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("is null", K(pc_param));
    } else if (NULL == (raw_param = pc_param->node_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("is null", K(raw_param));
    } else if (T_QUESTIONMARK != raw_param->type_) {
      if (OB_FAIL(add_fixed_param_value(i, raw_param, fixed_param_infos))) {
        LOG_WARN("failed to add fixed param value", K(ret));
      }
    } else if (mark_pos.has_member(raw_param->value_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "pattern contains the same question mark");
      LOG_WARN("pattern contains the same question mark not supported", K(ret));
    } else if (OB_FAIL(add_dynamic_param_info(i, raw_param->value_, dynamic_param_infos))) {
      LOG_WARN("failed to add dynamic param info", K(ret));
    } else if (OB_FAIL(mark_pos.add_member(raw_param->value_))) {
      LOG_WARN("failed to add question_mark_idx", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    std::sort(dynamic_param_infos.begin(), dynamic_param_infos.end(),
              [](DynamicParamInfo &l, DynamicParamInfo &r)
              {
                return (l.question_mark_idx_  < r.question_mark_idx_);
              });
  }
  return ret;
}

int ObUDRAnalyzer::parse_sql_to_gen_match_param_infos(
    const common::ObString &pattern,
    common::ObString &normalized_pattern,
    ObIArray<ObPCParam*> &raw_params)
{
  int ret = OB_SUCCESS;
  ObFastParserResult fp_result;
  FPContext fp_ctx(charsets4parser_);
  fp_ctx.sql_mode_ = sql_mode_;
  fp_ctx.is_udr_mode_ = true;
  if (pattern.empty()) {
    LOG_WARN("empty sql", K(pattern));
  } else if (OB_FAIL(ObSqlParameterization::fast_parser(allocator_,
                                                        fp_ctx,
                                                        pattern,
                                                        fp_result))) {
    LOG_WARN("failed to fast parser", K(ret), K(sql_mode_), K(pattern));
  } else if (FALSE_IT(normalized_pattern = fp_result.pc_key_.name_)) {
  } else if (OB_FAIL(raw_params.assign(fp_result.raw_params_))) {
    LOG_WARN("failed to assign raw params", K(ret));
  }
  return ret;
}

int ObUDRAnalyzer::parse_pattern_to_gen_param_infos(
    const common::ObString &pattern,
    common::ObString &normalized_pattern,
    ObIArray<ObPCParam*> &raw_params,
    ObQuestionMarkCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObFastParserResult fp_result;
  FPContext fp_ctx(charsets4parser_);
  fp_ctx.sql_mode_ = sql_mode_;
  fp_ctx.is_udr_mode_ = true;
  if (pattern.empty()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "empty pattern");
    LOG_WARN("empty pattern not supported", K(ret));
  } else if (OB_FAIL(ObSqlParameterization::fast_parser(allocator_,
                                                        fp_ctx,
                                                        pattern,
                                                        fp_result))) {
    LOG_WARN("failed to fast parser", K(ret), K(sql_mode_), K(pattern));
  } else if (FALSE_IT(normalized_pattern = fp_result.pc_key_.name_)) {
  } else if (OB_FAIL(raw_params.assign(fp_result.raw_params_))) {
    LOG_WARN("failed to assign raw params", K(ret));
  } else {
    ctx = fp_result.question_mark_ctx_;
  }
  return ret;
}

template<typename T>
int ObUDRAnalyzer::serialize_to_hex(const T &infos, common::ObString &infos_str)
{
  int ret = OB_SUCCESS;
  char *serialize_buf = nullptr;
  char *hex_buf = nullptr;
  int64_t serialize_pos = 0;
  int64_t hex_pos = 0;
  const int64_t serialize_size = infos.get_serialize_size();
  const int64_t hex_size = 2 * serialize_size;
  if (OB_ISNULL(serialize_buf = static_cast<char *>(allocator_.alloc(serialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate", K(ret));
  } else if (OB_FAIL(infos.serialize(serialize_buf, serialize_size, serialize_pos))) {
    LOG_WARN("failed to serialize infos", K(ret), K(serialize_size), K(serialize_pos));
  } else if (OB_UNLIKELY(serialize_pos > serialize_size)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("serialize error", KR(ret), K(serialize_pos), K(serialize_size));
  } else if (OB_ISNULL(hex_buf = static_cast<char*>(allocator_.alloc(hex_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(hex_size));
  } else if (OB_FAIL(hex_print(serialize_buf, serialize_pos, hex_buf, hex_size, hex_pos))) {
    LOG_WARN("fail to print hex", KR(ret), K(serialize_pos), K(hex_size), K(serialize_buf));
  } else if (OB_UNLIKELY(hex_pos > hex_size)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("encode error", KR(ret), K(hex_pos), K(hex_size));
  } else {
    infos_str.assign(hex_buf, hex_pos);
    LOG_DEBUG("succ to serialize", K(infos_str));
  }
  return ret;
}

int ObUDRAnalyzer::parse_pattern_to_gen_param_infos_str(
    const common::ObString &pattern,
    common::ObString &normalized_pattern,
    common::ObString &fixed_param_infos_str,
    common::ObString &dynamic_param_infos_str,
    common::ObString &question_mark_def_name_ctx_str)
{
  int ret = OB_SUCCESS;
  FixedParamValueArray fixed_param_infos;
  DynamicParamInfoArray dynamic_param_infos;
  ObSEArray<ObPCParam*, 16> raw_param_list;
  ObQuestionMarkCtx question_mark_ctx;
  MEMSET(&question_mark_ctx, 0, sizeof(ObQuestionMarkCtx));
  if (OB_FAIL(parse_pattern_to_gen_param_infos(pattern, normalized_pattern, raw_param_list, question_mark_ctx))) {
    LOG_WARN("failed to parse_pattern_to_gen_param_infos", K(ret), K(pattern));
  } else if (OB_FAIL(cons_raw_param_infos(raw_param_list,
                                          fixed_param_infos,
                                          dynamic_param_infos))) {
    LOG_WARN("failed to parse and gen param infos", K(ret));
  } else if (!fixed_param_infos.empty()
    && OB_FAIL(serialize_to_hex<FixedParamValueArray>(fixed_param_infos,
                                                      fixed_param_infos_str))) {
    LOG_WARN("failed to serialize fixed param infos", K(ret));
  } else if (!dynamic_param_infos.empty()
    && OB_FAIL(serialize_to_hex<DynamicParamInfoArray>(dynamic_param_infos,
                                                       dynamic_param_infos_str))) {
    LOG_WARN("failed to serialize dynamic param infos", K(ret));
  } else {
    QuestionMarkDefNameCtx def_name_ctx(allocator_);
    def_name_ctx.name_ = question_mark_ctx.name_;
    def_name_ctx.count_ = question_mark_ctx.count_;

    if (question_mark_ctx.by_name_ && question_mark_ctx.name_ != nullptr
      && OB_FAIL(serialize_to_hex<QuestionMarkDefNameCtx>(def_name_ctx,
                                                          question_mark_def_name_ctx_str))) {
      LOG_WARN("failed to serialize defined name ctx", K(ret));
    }
  }
  return ret;
}

} // namespace sql end
} // namespace oceanbase end
