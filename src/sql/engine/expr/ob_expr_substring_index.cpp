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

#include "sql/engine/expr/ob_expr_substring_index.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/string/ob_string.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{


ObExprSubstringIndex::ObExprSubstringIndex(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_SUBSTRING_INDEX, N_SUBSTRING_INDEX, 3, VALID_FOR_GENERATED_COL)
{
  need_charset_convert_ = false;
}

ObExprSubstringIndex::~ObExprSubstringIndex()
{
}

inline int ObExprSubstringIndex::calc_result_type3(ObExprResType &type,
                                                   ObExprResType &str,
                                                   ObExprResType &delim,
                                                   ObExprResType &count,
                                                   ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  str.set_calc_type(ObVarcharType);
  delim.set_calc_type(ObVarcharType);

  //there can't set int type for count, because it is dynamically
  //count.set_calc_type(ObIntType);

  // substring_index is mysql only expr.
  CK(true == lib::is_mysql_mode());
  CK(NULL != type_ctx.get_session());

  if (OB_SUCC(ret)) {
    // cast count to int64, see comment in eval_substring_index
    count.set_calc_type(ObIntType);
    // Set cast mode for %count:
    //   truncate string to integer.
    //   no range check for uint to int
    type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
    type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NO_RANGE_CHECK);
  }

  if (OB_SUCC(ret)) {
    type.set_varchar();
    type.set_length(str.get_length());
    common::ObArenaAllocator alloc;
    ObExprResType types[2] = {alloc, alloc};
    types[0] = str;
    types[1] = delim;
    if (OB_FAIL(aggregate_charsets_for_string_result_with_comparison(
                type, types, 2, type_ctx.get_coll_type()))) {
      LOG_WARN("aggregate_charsets_for_string_result_with_comparison failed", K(ret));

    } else {
      str.set_calc_collation_type(type.get_collation_type());
      str.set_calc_collation_level(type.get_collation_level());
      delim.set_calc_collation_type(type.get_collation_type());
      delim.set_calc_collation_level(type.get_collation_level());

    }
  }

  return ret;
}

int ObExprSubstringIndex::cg_expr(ObExprCGCtx &, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  const ObRawExpr *text_expr = NULL;
  const ObRawExpr *pattern_expr = NULL;
  const ObRawExpr *nth_appearance_expr = NULL;
  if (OB_UNLIKELY(3 != raw_expr.get_param_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("substring_index op should have 3 arguments", K(raw_expr.get_param_count()));
  } else if (OB_ISNULL(text_expr = raw_expr.get_param_expr(0)) ||
               OB_ISNULL(pattern_expr = raw_expr.get_param_expr(1)) ||
               OB_ISNULL(nth_appearance_expr = raw_expr.get_param_expr(2))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer", K(text_expr), K(pattern_expr), K(nth_appearance_expr));
  } else if (rt_expr.arg_cnt_ != 3 || OB_ISNULL(rt_expr.args_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("substring_index expr should have 3 arguments", K(ret),
                                                             K(rt_expr.arg_cnt_), K(rt_expr.args_));
  } else if (OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1]) ||
               OB_ISNULL(rt_expr.args_[2])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret), K(rt_expr.args_[0]), K(rt_expr.args_[1]),
                              K(rt_expr.args_[2]));
  } else {
    rt_expr.eval_func_ = eval_substring_index;
    rt_expr.eval_batch_func_ = ObExprSubstringIndex::eval_substring_index_batch;
  }

  return ret;
}

int ObExprSubstringIndex::eval_substring_index(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum &str = expr.locate_param_datum(ctx, 0);
  ObDatum &delim = expr.locate_param_datum(ctx, 1);
  ObDatum &count = expr.locate_param_datum(ctx, 2);
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (OB_UNLIKELY(str.is_null() || delim.is_null() || count.is_null())) {
    expr_datum.set_null();
  } else if (0 == str.len_ || 0 == delim.len_) {
    // return empty string if %str or %delim is empty.
    //重置null flag 防止丢失空串信息
    expr_datum.null_ = 0;
    expr_datum.len_ = 0;
  } else {
    const ObString m_delim = delim.get_string();
    // mysql 5.6 static cast count to int32,
    // actually this is a bug and fixed in mysql 8.0.
    int64_t count_val = count.get_int();
    ObString res_str;
    ObExprKMPSearchCtx *kmp_ctx = NULL;
    const uint64_t op_id = static_cast<uint64_t>(expr.expr_ctx_id_);
    if (OB_FAIL(ObExprKMPSearchCtx::get_kmp_ctx_from_exec_ctx(ctx.exec_ctx_, op_id, kmp_ctx))) {
      LOG_WARN("get kmp ctx failed", K(ret));
    } else if (OB_FAIL(kmp_ctx->init(m_delim, count_val < 0, ctx.exec_ctx_.get_allocator()))) {
      LOG_WARN("init kmp ctx failed", K(ret), K(m_delim));
    } else if (OB_FAIL(kmp_ctx->substring_index_search(str.get_string(), count_val, res_str))) {
      LOG_WARN("string search failed", K(ret));
    } else {
      expr_datum.set_string(res_str);
    }
  }

  return ret;
}

int ObExprSubstringIndex::eval_substring_index_batch(const ObExpr &expr,
                                                     ObEvalCtx &ctx,
                                                     const ObBitVector &skip,
                                                     const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObExprKMPSearchCtx *kmp_ctx = NULL;
  const uint64_t op_id = static_cast<uint64_t>(expr.expr_ctx_id_);
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval args_0 failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval args_1 failed", K(ret));
  } else if (OB_FAIL(expr.args_[2]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval args_2 failed", K(ret));
  } else if (OB_FAIL(ObExprKMPSearchCtx::get_kmp_ctx_from_exec_ctx(ctx.exec_ctx_,
                                                                   op_id, kmp_ctx))) {
    LOG_WARN("get kmp ctx failed", K(ret));
  } else {
    ObDatum *res = expr.locate_batch_datums(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;;
      }
      ObString res_str;
      int64_t count_val = 0;
      ObDatum &text = expr.args_[0]->locate_expr_datum(ctx, i);
      ObDatum &delim = expr.args_[1]->locate_expr_datum(ctx, i);
      ObDatum &count = expr.args_[2]->locate_expr_datum(ctx, i);
      if (OB_UNLIKELY(text.is_null() || delim.is_null() || count.is_null())) {
        res[i].set_null();
      } else if (0 == text.len_ || 0 == delim.len_ ||
                 0 == (count_val = count.get_int())) {
        // return empty string if %str or %delim is empty.
        //重置null flag 防止丢失空串信息
        res[i].null_ = 0;
        res[i].len_ = 0;
      } else if (OB_FAIL(kmp_ctx->init(delim.get_string(),
                                       count_val < 0, ctx.exec_ctx_.get_allocator()))) {
        LOG_WARN("init kmp ctx failed", K(ret), K(delim));
      } else if (OB_FAIL(kmp_ctx->substring_index_search(text.get_string(), count_val, res_str))) {
        LOG_WARN("string search failed", K(ret));
      } else {
        res[i].set_string(res_str);
      }
      eval_flags.set(i);
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprSubstringIndex, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

} /* sql */
} /* oceanbase */

