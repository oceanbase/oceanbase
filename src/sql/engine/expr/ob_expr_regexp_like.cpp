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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_regexp_like.h"
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_regexp_count.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprRegexpLike::ObExprRegexpLike(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_REGEXP_LIKE, N_REGEXP_LIKE, MORE_THAN_ONE, NOT_ROW_DIMENSION)
{
}

ObExprRegexpLike::~ObExprRegexpLike()
{
}

int ObExprRegexpLike::calc_result_typeN(ObExprResType &type,
                                        ObExprResType *types,
                                        int64_t param_num,
                                        common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  CK(NULL != type_ctx.get_raw_expr());
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(param_num < 2 || param_num > 3)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("param number of regexp_replace at least 2 and at most 3", K(ret), K(param_num));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < param_num; i++) {
      if (!types[i].is_null() && !is_type_valid(types[i].get_type())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("the parameter is not castable", K(ret), K(i));
      }
    }
    if (lib::is_oracle_mode()) {//set default value
      type.set_calc_type(ObVarcharType);
      type.set_calc_collation_type(ObCharset::get_default_collation_oracle(CHARSET_UTF8MB4));
      type.set_calc_collation_level(CS_LEVEL_IMPLICIT);
    } else {
      ObObjMeta type_metas[2] = {types[0], types[1]};
      if (OB_FAIL(aggregate_charsets_for_comparison(
                  type.get_calc_meta(), type_metas, 2, type_ctx.get_coll_type()))) {
        LOG_WARN("fail to aggregate charsets for comparison", K(ret), K(types[0]), K(types[1]));
      }
    }
    if (OB_SUCC(ret)) {//because of regexp engine need utf8 code, here need reset calc collation type and can implicit cast.
      switch (param_num) {
        case 3:
          types[2].set_calc_type(ObVarcharType);
        case 2:
          types[1].set_calc_type(ObVarcharType);
          if (ObCharset::is_bin_sort(types[1].get_calc_collation_type())) {
            types[1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
          } else {
            types[1].set_calc_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
          }
          if (!types[0].is_clob()) {
            types[0].set_calc_type(ObVarcharType);
            if (ObCharset::is_bin_sort(types[0].get_calc_collation_type())) {
              types[0].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
            } else {
              types[0].set_calc_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
            }
          }
        default:
          // already check before
          break;
      }
    }
    if (OB_SUCC(ret)) {
      type.set_tinyint();
      type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
      type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
      ObExprOperator::calc_result_flag2(type, types[0], types[1]);
    }
  }
  return ret;
}

int ObExprRegexpLike::regexp_like(bool &match,
                                  const ObString &text,
                                  const ObString &pattern,
                                  int64_t position,
                                  int64_t occurrence,
                                  const ObCollationType calc_cs_type,
                                  const ObString &match_param,
                                  bool has_null_argument,
                                  bool reusable,
                                  ObExprRegexContext* regexp_ptr,
                                  ObExprStringBuf &string_buf,
                                  ObIAllocator &exec_ctx_alloc)
{
  int ret = OB_SUCCESS;
  bool sub = false;
  //begin with '^'
  bool from_begin = false;
  //end with '$'
  bool from_end = false;
  ObExprRegexContext *regexp_like_ctx = regexp_ptr;
  ObSEArray<uint32_t, 4> begin_locations(common::ObModIds::OB_SQL_EXPR_REPLACE,
                                           common::OB_MALLOC_NORMAL_BLOCK_SIZE);
  int flags = 0, multi_flag = 0;
  if (OB_ISNULL(regexp_like_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("regexp ptr is null", K(ret));
  } else if (OB_FAIL(ObExprRegexpCount::get_regexp_flags(calc_cs_type, match_param,
                                                         flags, multi_flag))) {
    LOG_WARN("fail to get regexp flags", K(ret), K(match_param));
  } else if (OB_FAIL(regexp_like_ctx->init(pattern, flags,
                                           reusable ? exec_ctx_alloc : string_buf,
                                           reusable))) {
    LOG_WARN("fail to init regexp", K(pattern), K(flags));
  }
  if (OB_SUCC(ret)) {
    //为了更好的和oracle兼容
    if (has_null_argument) {
      match = false;
    } else {
      if (!pattern.empty()) {
        const char* pat = pattern.ptr();
        if ('^' == pat[0]) {
          from_begin = true;
        } else if ('$' == pat[pattern.length() - 1]) {
          from_end = true;
        }
      }
      if (OB_FAIL(begin_locations.push_back(position - 1))) {
        LOG_WARN("begin_locations push_back error", K(ret));
      } else if (from_begin && 1 != position && !multi_flag) {
        match = sub;
      } else {
        ObSEArray<size_t, 4> byte_num;
        ObSEArray<size_t, 4> byte_offsets;
        if (OB_FAIL(ObExprUtil::get_mb_str_info(text,
                                                ObCharset::get_default_collation_oracle(CHARSET_UTF8MB4),
                                                byte_num,
                                                byte_offsets))) {
          LOG_WARN("failed to get mb str info", K(ret));
        } else if (multi_flag) {
          if (from_begin && 1 != position) {
            begin_locations.pop_back();
          }
          const char *text_ptr = text.ptr();
          for (int64_t idx = position; OB_SUCC(ret) && idx < byte_offsets.count() - 1; ++idx) {
            if (OB_UNLIKELY(byte_offsets.at(idx) >= text.length())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected error", K(byte_offsets.at(idx)), K(text.length()), K(ret));
            } else if (text_ptr[byte_offsets.at(idx)] == '\n') {
              if (OB_FAIL(begin_locations.push_back(idx + 1))) {
                LOG_WARN("begin_locations push_back error", K(ret));
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(regexp_like_ctx->like(text, occurrence, sub, string_buf,
                                            from_begin, from_end, begin_locations))) {
            LOG_WARN("fail to replace substr", K(ret), K(text));
          } else {
            match = sub;
          }
        }
      }
    }
  }
  return ret;
}

int ObExprRegexpLike::calc(common::ObObj &result,
                           const ObString &text,
                           const ObString &pattern,
                           int64_t position,
                           int64_t occurrence,
                           const ObString &match_param,
                           bool has_null,
                           ObExprRegexContext* regexp_ptr,
                           ObExprStringBuf &string_buf,
                           ObIAllocator &exec_ctx_alloc) const
{
  int ret = OB_SUCCESS;
  bool match = false;
  if (OB_FAIL(regexp_like(match, text, pattern, position, occurrence,
                          result_type_.get_calc_collation_type(),
                          match_param, has_null, false, regexp_ptr, string_buf, exec_ctx_alloc))) {
    LOG_WARN("regexp like failed", K(ret));
  } else {
    result.set_int(result_type_.get_type(), static_cast<int64_t>(match));
  }
  return ret;
}

int ObExprRegexpLike::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  CK(2 == rt_expr.arg_cnt_ || 3 == rt_expr.arg_cnt_);
  CK(raw_expr.get_param_count() >= 2);
  if (OB_SUCC(ret)) {
    const ObRawExpr *text = raw_expr.get_param_expr(0);
    const ObRawExpr *pattern = raw_expr.get_param_expr(1);
    if (OB_ISNULL(text) || OB_ISNULL(pattern)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(text), K(pattern), K(ret));
    } else {
      const bool const_text = text->is_const_expr();
      const bool const_pattern = pattern->is_const_expr();
      rt_expr.extra_ = (!const_text && const_pattern) ? 1 : 0;
      rt_expr.eval_func_ = &eval_regexp_like;
      LOG_DEBUG("regexp like expr cg", K(const_text), K(const_pattern), K(rt_expr.extra_));
    }
  }
  return ret;
}

int ObExprRegexpLike::eval_regexp_like(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *text = NULL;
  ObDatum *pattern = NULL;
  ObDatum *flags = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, text, pattern, flags))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else {
    // use default flags if NULL == flags and flags->is_null()
    bool is_flag_null = (NULL != flags && flags->is_null());
    const bool null_result = (text->is_null() || pattern->is_null() ||
                              (lib::is_mysql_mode() && is_flag_null));
    ObString match_param = (NULL != flags && !flags->is_null())
        ? flags->get_string()
        : ObString();
    const int64_t pos = 1;
    const int64_t occurrence = 1;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &alloc = alloc_guard.get_allocator();
    bool match = false;
    ObExprRegexContext local_regex_ctx;
    ObExprRegexContext *regexp_ctx = &local_regex_ctx;
    const bool reusable = (0 != expr.extra_) && ObExpr::INVALID_EXP_CTX_ID != expr.expr_ctx_id_;
    if (OB_SUCC(ret) && reusable) {
      if (NULL == (regexp_ctx = static_cast<ObExprRegexContext *>(
                  ctx.exec_ctx_.get_expr_op_ctx(expr.expr_ctx_id_)))) {
        if (OB_FAIL(ctx.exec_ctx_.create_expr_op_ctx(expr.expr_ctx_id_, regexp_ctx))) {
          LOG_WARN("create expr regex context failed", K(ret), K(expr));
        } else if (OB_ISNULL(regexp_ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL context returned", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (lib::is_mysql_mode() && !pattern->is_null() &&
        pattern->get_string().empty() && !is_flag_null) {//compatible mysql
      ret = OB_ERR_REGEXP_ERROR;
      LOG_WARN("empty regex expression", K(ret));
    } else if (OB_FAIL(regexp_like(match, text->get_string(), pattern->get_string(), pos,
                                   occurrence, expr.args_[0]->datum_meta_.cs_type_, match_param,
                                   null_result, reusable, regexp_ctx, alloc,
                                   ctx.exec_ctx_.get_allocator()))) {
      LOG_WARN("do regexp like failed", K(ret));
    } else if (null_result) {
      expr_datum.set_null();
    } else {
      expr_datum.set_int32(match);
    }
  }
  return ret;
}

}
}
