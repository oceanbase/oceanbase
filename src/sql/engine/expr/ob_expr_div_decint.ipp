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

#ifndef OCEANBASE_EXPR_DIV_DECINT_IPP_
#define OCEANBASE_EXPR_DIV_DECINT_IPP_
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_div.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_batch_eval_util.h"

namespace oceanbase
{
namespace sql
{
using namespace common;
using namespace share;

template<typename LVal, typename RVal>
struct ObDecintMySQLDivDatumFunc
{
  OB_INLINE int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r,
                           const bool is_err_div_by_zero, const ObScale round_up_scale,
                           const ObScale decint_res_scale) const
  {
    int ret = OB_SUCCESS;
    const LVal &numerator = *reinterpret_cast<const LVal *>(l.get_decimal_int());
    const RVal &denominator = *reinterpret_cast<const RVal *>(r.get_decimal_int());
    LVal quo;
    number::ObNumber res_nmb;
    ObNumStackOnceAlloc tmp_alloc;
    if (OB_UNLIKELY(denominator == 0)) {
      if (is_err_div_by_zero) {
        ret = OB_DIVISION_BY_ZERO;
      } else {
        res.set_null();
        LOG_DEBUG("divisor is equal to zero", K(l), K(r), K(ret));
      }
    } else {
      quo = numerator / denominator;
      ObScale round_scale = MIN(round_up_scale, decint_res_scale);
      if (OB_FAIL(wide::to_number(quo, decint_res_scale, tmp_alloc, res_nmb))) {
        LOG_WARN("wide::to_number failed", K(ret));
      } else if (round_scale < decint_res_scale && OB_FAIL(res_nmb.trunc(round_scale))) {
        LOG_WARN("truncate number failed", K(ret));
      } else {
        res.set_number(res_nmb);
      }
    }
    return ret;
  }
};


template <typename L, typename R>
struct ObDecintMySQLDivVecFunc
{
  template <typename ResVector, typename LeftVector, typename RightVector>
  int operator()(ResVector &res_vec, const LeftVector &l_vec, const RightVector &r_vec,
                 const int64_t idx, const int64_t round_up_scale, const bool is_err_div_by_zero,
                 const ObScale decint_res_scale) const
  {
    int ret = OB_SUCCESS;
    const L &lhs = *reinterpret_cast<const L *>(l_vec.get_payload(idx));
    const R &rhs = *reinterpret_cast<const R *>(r_vec.get_payload(idx));
    number::ObNumber res_nmb;
    ObNumStackOnceAlloc tmp_alloc;
    L quo;
    if (OB_UNLIKELY(rhs == 0)) {
      if (is_err_div_by_zero) {
        ret = OB_DIVISION_BY_ZERO;
      } else {
        res_vec.set_null(idx);
      }
    } else {
      quo = lhs / rhs;
      ObScale round_scale = MIN(round_up_scale, decint_res_scale);
      if (OB_FAIL(wide::to_number(quo, decint_res_scale, tmp_alloc, res_nmb))) {
        LOG_WARN("wide::to_number failed", K(ret));
      } else if (round_scale < decint_res_scale && OB_FAIL(res_nmb.trunc(round_scale))) {
        LOG_WARN("truncate number failed", K(ret));
      } else {
        res_vec.set_number(idx, res_nmb);
      }
    }
    return ret;
  }
};

template<typename ltype, typename rtype, typename LFmt, typename RFmt, typename ResFmt>
static int inner_decint_div_mysql_vec_fn(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  bool right_evaluated = true;
  EvalBound pvt_bound = bound;
  LFmt *l_vec = static_cast<LFmt *>(expr.args_[0]->get_vector(ctx));
  RFmt *r_vec = static_cast<RFmt *>(expr.args_[1]->get_vector(ctx));
  ResFmt *res_vec = static_cast<ResFmt *>(expr.get_vector(ctx));
  int64_t div_inc = 0;
  bool is_err_div_by_zero = expr.is_error_div_by_zero_;
  ObDecintMySQLDivVecFunc<ltype, rtype> div_fn;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  if (OB_FAIL(ctx.exec_ctx_.get_my_session()->get_div_precision_increment(div_inc))) {
    LOG_WARN("get_div_precision_increment failed", K(ret));
  } else {
    ObScale round_up_scale = ObExprDiv::decint_res_round_up_scale(expr, div_inc);
    if (OB_LIKELY(!l_vec->has_null() && !r_vec->has_null() && bound.get_all_rows_active())) {
      for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
        ret = div_fn(*res_vec, *l_vec, *r_vec, i, round_up_scale, is_err_div_by_zero,
                     expr.div_calc_scale_);
      }
      if (OB_SUCC(ret)) { eval_flags.set_all(bound.start(), bound.end()); }
    } else {
      for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
        if (skip.at(i) || eval_flags.at(i)) {
        } else if (l_vec->is_null(i) || r_vec->is_null(i)) {
          res_vec->set_null(i);
          eval_flags.set(i);
        } else {
          ret = div_fn(*res_vec, *l_vec, *r_vec, i, round_up_scale, is_err_div_by_zero,
                       expr.div_calc_scale_);
          if (OB_SUCC(ret)) { eval_flags.set(i); }
        }
      }
    }
  }
  return ret;
}

template<typename ltype, typename rtype>
int ObExprDiv::decint_div_mysql_fn(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  int64_t div_inc = 0;
  if (OB_FAIL(ctx.exec_ctx_.get_my_session()->get_div_precision_increment(div_inc))) {
    LOG_WARN("get_div_precision_increment failed", K(ret));
  } else {
    ObScale round_up_scale = decint_res_round_up_scale(expr, div_inc);
    ret = def_arith_eval_func<ObDecintMySQLDivDatumFunc<ltype, rtype>>(
      expr, ctx, expr_datum, expr.is_error_div_by_zero_, round_up_scale, expr.div_calc_scale_);
  }
  return ret;
}

template <typename ltype, typename rtype>
int ObExprDiv::decint_div_mysql_batch_fn(BATCH_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  int64_t div_inc = 0;
  if (OB_FAIL(binary_operand_batch_eval(expr, ctx, skip, size, false))) { // mysql mode
    LOG_WARN("eval operands failed", K(ret));
  } else if (OB_FAIL(ctx.exec_ctx_.get_my_session()->get_div_precision_increment(div_inc))) {
    LOG_WARN("get_div_precision_increment failed", K(ret));
  } else {
    ObDatumVector l_vec = expr.args_[0]->locate_expr_datumvector(ctx);
    ObDatumVector r_vec = expr.args_[1]->locate_expr_datumvector(ctx);
    ObDatum *res_datums = expr.locate_datums_for_update(ctx, size);
    ObDecintMySQLDivDatumFunc<ltype, rtype> div_fn;
    // ObScale res_scale = expr.datum_meta_.scale_;
    ObScale round_up_scale = decint_res_round_up_scale(expr, div_inc);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    for (int i = 0; OB_SUCC(ret) && i < size; i++) {
      if (skip.at(i) || eval_flags.at(i)) {
      } else if (l_vec.at(i)->is_null() || r_vec.at(i)->is_null()) {
        res_datums[i].set_null();
        eval_flags.set(i);
      } else {
        ret = div_fn(res_datums[i], *l_vec.at(i), *r_vec.at(i), expr.is_error_div_by_zero_,
                     round_up_scale, expr.div_calc_scale_);
        if (OB_SUCC(ret)) {
          eval_flags.set(i);
        }
      }
    }
  }
  return ret;
}

template<typename ltype, typename rtype>
int ObExprDiv::decint_div_mysql_vec_fn(VECTOR_EVAL_FUNC_ARG_DECL)
{
#define CALC_FMT_LIST(l_fmt, r_fmt) ((l_fmt << VEC_MAX_FORMAT) + (r_fmt))
  using LFixedFmt = ObFixedLengthFormat<ltype>;
  using UniFmt = ObUniformFormat<false>;
  using UniConstFmt =  ObUniformFormat<true>;
  using RFixedFmt = ObFixedLengthFormat<rtype>;

  int ret = OB_SUCCESS;
  // mysql mode, just eval operands is fine
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval left operands failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval right operands failed", K(ret));
  } else {
    VectorFormat l_fmt = expr.args_[0]->get_format(ctx);
    VectorFormat r_fmt = expr.args_[1]->get_format(ctx);
    VectorFormat res_fmt = expr.get_format(ctx);
    switch (res_fmt) {
    case VEC_DISCRETE: {
      switch (CALC_FMT_LIST(l_fmt, r_fmt)) {
      case CALC_FMT_LIST(VEC_FIXED, VEC_FIXED): {
        ret = inner_decint_div_mysql_vec_fn<ltype, rtype, LFixedFmt, RFixedFmt, ObDiscreteFormat>(
          VECTOR_EVAL_FUNC_ARG_LIST);
        break;
      }
      case CALC_FMT_LIST(VEC_FIXED, VEC_UNIFORM): {
        ret = inner_decint_div_mysql_vec_fn<ltype, rtype, LFixedFmt, UniFmt, ObDiscreteFormat>(
          VECTOR_EVAL_FUNC_ARG_LIST);
        break;
      }
      case CALC_FMT_LIST(VEC_FIXED, VEC_UNIFORM_CONST): {
        ret = inner_decint_div_mysql_vec_fn<ltype, rtype, LFixedFmt, UniConstFmt, ObDiscreteFormat>(
          VECTOR_EVAL_FUNC_ARG_LIST);
        break;
      }
      case CALC_FMT_LIST(VEC_UNIFORM, VEC_FIXED): {
        ret = inner_decint_div_mysql_vec_fn<ltype, rtype, UniFmt, RFixedFmt, ObDiscreteFormat>(
          VECTOR_EVAL_FUNC_ARG_LIST);
        break;
      }
      case CALC_FMT_LIST(VEC_UNIFORM, VEC_UNIFORM): {
        ret = inner_decint_div_mysql_vec_fn<ltype, rtype, UniFmt, UniFmt, ObDiscreteFormat>(
          VECTOR_EVAL_FUNC_ARG_LIST);
        break;
      }
      case CALC_FMT_LIST(VEC_UNIFORM, VEC_UNIFORM_CONST): {
        ret = inner_decint_div_mysql_vec_fn<ltype, rtype, UniFmt, UniConstFmt, ObDiscreteFormat>(
          VECTOR_EVAL_FUNC_ARG_LIST);
        break;
      }
      default: {
        ret = inner_decint_div_mysql_vec_fn<ltype, rtype, ObVectorBase, ObVectorBase, ObVectorBase>(
          VECTOR_EVAL_FUNC_ARG_LIST);
        break;
      }
      }
      break;
    }
    case VEC_UNIFORM: {
      switch (CALC_FMT_LIST(l_fmt, r_fmt)) {
      case CALC_FMT_LIST(VEC_FIXED, VEC_FIXED): {
        ret = inner_decint_div_mysql_vec_fn<ltype, rtype, LFixedFmt, RFixedFmt, UniFmt>(
          VECTOR_EVAL_FUNC_ARG_LIST);
        break;
      }
      case CALC_FMT_LIST(VEC_FIXED, VEC_UNIFORM): {
        ret = inner_decint_div_mysql_vec_fn<ltype, rtype, LFixedFmt, UniFmt, UniFmt>(
          VECTOR_EVAL_FUNC_ARG_LIST);
        break;
      }
      case CALC_FMT_LIST(VEC_FIXED, VEC_UNIFORM_CONST): {
        ret = inner_decint_div_mysql_vec_fn<ltype, rtype, LFixedFmt, UniConstFmt, UniFmt>(
          VECTOR_EVAL_FUNC_ARG_LIST);
        break;
      }
      case CALC_FMT_LIST(VEC_UNIFORM, VEC_FIXED): {
        ret = inner_decint_div_mysql_vec_fn<ltype, rtype, UniFmt, RFixedFmt, UniFmt>(
          VECTOR_EVAL_FUNC_ARG_LIST);
        break;
      }
      case CALC_FMT_LIST(VEC_UNIFORM, VEC_UNIFORM): {
        ret = inner_decint_div_mysql_vec_fn<ltype, rtype, UniFmt, UniFmt, UniFmt>(
          VECTOR_EVAL_FUNC_ARG_LIST);
        break;
      }
      case CALC_FMT_LIST(VEC_UNIFORM, VEC_UNIFORM_CONST): {
        ret = inner_decint_div_mysql_vec_fn<ltype, rtype, UniFmt, UniConstFmt, UniFmt>(
          VECTOR_EVAL_FUNC_ARG_LIST);
        break;
      }
      default: {
        ret = inner_decint_div_mysql_vec_fn<ltype, rtype, ObVectorBase, ObVectorBase, ObVectorBase>(
          VECTOR_EVAL_FUNC_ARG_LIST);
        break;
      }
      }
      break;
    }
    default: {
      ret = inner_decint_div_mysql_vec_fn<ltype, rtype, ObVectorBase, ObVectorBase, ObVectorBase>(
        VECTOR_EVAL_FUNC_ARG_LIST);
      break;
    }
    }
  }
  return ret;
}

ObScale ObExprDiv::decint_res_round_up_scale(const ObExpr &expr, int64_t div_inc)
{
  ObScale decint_res_scale = expr.div_calc_scale_;
  ObScale s2 = expr.args_[1]->datum_meta_.scale_;
  ObScale p2 = expr.args_[1]->datum_meta_.precision_;
  ObScale s1 = decint_res_scale - div_inc - extra_scale_for_decint_div - p2 + s2;
  ObScale res_scale = MAX(round_up_scale(s1) + round_up_scale(s2), round_up_scale(s1 + s2 + div_inc));
  return res_scale;
}

} // end sql
} // end oceanbase

#endif // OCEANBASE_EXPR_DIV_DECINT_IPP_



