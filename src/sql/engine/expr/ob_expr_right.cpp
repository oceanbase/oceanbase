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
#include "ob_expr_right.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/charset/ob_charset_string_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprRight::ObExprRight(ObIAllocator &alloc) :
    ObStringExprOperator(alloc, T_FUN_SYS_RIGHT, "right", 2, VALID_FOR_GENERATED_COL)
{
}

ObExprRight::~ObExprRight()
{
}

int ObExprRight::calc_result_type2(ObExprResType &type, ObExprResType &type1,
                                   ObExprResType &type2,ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  type2.set_calc_type(ObIntType);
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
  type.set_varchar();
  type.set_length(type1.get_length());
  OZ(aggregate_charsets_for_string_result(type, &type1, 1, type_ctx));
  OX(type1.set_calc_type(ObVarcharType));
  OX(type1.set_calc_collation_type(type.get_collation_type()));
  OX(type1.set_calc_collation_level(type.get_collation_level()));
  return ret;
}

int do_right(const ObString &input_value, const ObCollationType &cs_type,
             const int64_t length, ObString &res_str)
{
  int ret = OB_SUCCESS;
  int64_t char_len = (int64_t)(ObCharset::strlen_char(cs_type,
                                                      input_value.ptr(),
                                                      input_value.length()));
  int64_t pos = char_len - length;
  if (length < 0) {
    res_str.reset();
    //return empty string
  } else if (pos <= 0) {
    res_str = input_value;
  } else {
    int32_t offset = (int32_t)
        ObCharset::charpos(cs_type, input_value.ptr(), input_value.length(), pos);
    if (offset <= input_value.length()) {
      res_str.assign_ptr(input_value.ptr() + offset, input_value.length() - offset);
    }
  }
  return ret;
}

int calc_right_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  // right(s, n)
  ObDatum *s_datum = NULL;
  ObDatum *n_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, s_datum)) ||
      OB_FAIL(expr.args_[1]->eval(ctx, n_datum))) {
    LOG_WARN("eval arg failed", K(ret), KP(s_datum), KP(n_datum));
  } else if (s_datum->is_null() || n_datum->is_null()) {
    res_datum.set_null();
  } else {
    ObString res_str;
    const ObCollationType arg_cs_type = expr.args_[0]->datum_meta_.cs_type_;
		if(OB_FAIL(do_right(s_datum->get_string(), arg_cs_type, n_datum->get_int(), res_str))) {
			LOG_WARN("failed to calculate right expression", K(ret));
    } else {
      if (res_str.empty() && is_oracle_mode()) {
        res_datum.set_null();
      } else {
        res_datum.set_string(res_str);
      }
    }
  }
  return ret;
}

int ObExprRight::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_right_expr;
  rt_expr.eval_vector_func_ = calc_right_vector;
  return ret;
}

// scan from trailing to leading in order to locate n-th utf char
static inline const char* skip_trailing_utf8(const char* p, const char* begin, size_t n) {
    const int8_t threshold = static_cast<int8_t>(0xBF);
    for (size_t i = 0; i < n && p >= begin; ++i) {
        --p;
        while (p >= begin && static_cast<int8_t>(*p) <= threshold) --p;
    }
    return p;
}

template <typename ArgVec, typename ResVec, bool IsAscii, bool CanDoAsciiOptimize, bool HasNull>
int ObExprRight::calc_right_vector_const_inner(const ObExpr &expr,
                                               ObEvalCtx &ctx,
                                               const ObBitVector &skip,
                                               const EvalBound &bound,
                                               int64_t const_len)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = reinterpret_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = reinterpret_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObCollationType cs_type = expr.args_[0]->datum_meta_.cs_type_;
  bool is_oracle_mode = lib::is_oracle_mode();

  for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
    if (skip.at(idx) || eval_flags.at(idx)) {
      continue;
    } else if (HasNull && arg_vec->is_null(idx)) {
      res_vec->set_null(idx);
    } else {
      ObString text = arg_vec->get_string(idx);
      if (OB_UNLIKELY(const_len <= 0)) {
        // Return empty string for negative length
        if (is_oracle_mode) {
          res_vec->set_null(idx);
        } else {
          res_vec->set_string(idx, text.ptr(), 0);
        }
      } else if (IsAscii || (CanDoAsciiOptimize && ObCharsetStringHelper::is_ascii_str(text.ptr(), text.length()))) {
        // ASCII optimization: directly use byte position
        int64_t length = std::min<int64_t>(text.length(), const_len);
        res_vec->set_string(idx, text.ptr() + text.length() - length, length);
      } else if (cs_type == CS_TYPE_UTF8MB4_GENERAL_CI || cs_type == CS_TYPE_UTF8MB4_BIN || cs_type == CS_TYPE_UTF8MB4_UNICODE_CI) {
        // UTF8MB4 optimization
        const char* start = skip_trailing_utf8(text.ptr() + text.length(), text.ptr(), const_len);
        if (start <= text.ptr()) {
          res_vec->set_string(idx, text);
        } else {
          res_vec->set_string(idx, start, text.ptr() + text.length() - start);
        }
      } else {
        // other: need to calculate character position
        int64_t text_len = ObCharsetStringHelper::strlen_char_general(cs_type, text.ptr(), text.length());
        int64_t pos = text_len - const_len;
        if (pos <= 0) {
          res_vec->set_string(idx, text);
        } else {
          int64_t offset = ObCharsetStringHelper::charpos_general(cs_type, text.ptr(), text.length(), pos);
          res_vec->set_string(idx, text.ptr() + offset, text.length() - offset);
        }
      }
    }
  } // for end
  return ret;
}

template <typename ArgVec, typename ResVec>
int ObExprRight::calc_right_vector_const(const ObExpr &expr,
                                         ObEvalCtx &ctx,
                                         const ObBitVector &skip,
                                         const EvalBound &bound,
                                         int64_t const_len)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = reinterpret_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = reinterpret_cast<ResVec *>(expr.get_vector(ctx));
  bool is_ascii = arg_vec->is_batch_ascii();
  bool can_do_ascii_optimize = ObCharsetStringHelper::can_do_ascii_optimize(expr.args_[0]->datum_meta_.cs_type_);
  bool has_null = arg_vec->has_null();

  if (is_ascii) {
    if (can_do_ascii_optimize) {
      if (has_null) {
        ret = calc_right_vector_const_inner<ArgVec, ResVec, true, true, true>(expr, ctx, skip, bound, const_len);
      } else {
        ret = calc_right_vector_const_inner<ArgVec, ResVec, true, true, false>(expr, ctx, skip, bound, const_len);
      }
    } else {
      if (has_null) {
        ret = calc_right_vector_const_inner<ArgVec, ResVec, true, false, true>(expr, ctx, skip, bound, const_len);
      } else {
        ret = calc_right_vector_const_inner<ArgVec, ResVec, true, false, false>(expr, ctx, skip, bound, const_len);
      }
    }
  } else {
    if (can_do_ascii_optimize) {
      if (has_null) {
        ret = calc_right_vector_const_inner<ArgVec, ResVec, false, true, true>(expr, ctx, skip, bound, const_len);
      } else {
        ret = calc_right_vector_const_inner<ArgVec, ResVec, false, true, false>(expr, ctx, skip, bound, const_len);
      }
    } else {
      if (has_null) {
        ret = calc_right_vector_const_inner<ArgVec, ResVec, false, false, true>(expr, ctx, skip, bound, const_len);
      } else {
        ret = calc_right_vector_const_inner<ArgVec, ResVec, false, false, false>(expr, ctx, skip, bound, const_len);
      }
    }
  }

  return ret;
}

int ObExprRight::calc_right_vector_non_const(const ObExpr &expr,
                                             ObEvalCtx &ctx,
                                             const ObBitVector &skip,
                                             const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObIVector *arg_vec = reinterpret_cast<ObIVector *>(expr.args_[0]->get_vector(ctx));
  ObIVector *num_vec = reinterpret_cast<ObIVector *>(expr.args_[1]->get_vector(ctx));
  ObIVector *res_vec = reinterpret_cast<ObIVector *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObCollationType cs_type = expr.args_[0]->datum_meta_.cs_type_;
  bool is_oracle_mode = lib::is_oracle_mode();

  for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
    if (skip.at(idx) || eval_flags.at(idx)) {
      continue;
    } else if (arg_vec->is_null(idx) || num_vec->is_null(idx)) {
      res_vec->set_null(idx);
    } else {
      ObString text = arg_vec->get_string(idx);
      int64_t required_num_char = num_vec->get_int(idx);
      ObString res_str;
      if (OB_FAIL(do_right(text, cs_type, required_num_char, res_str))) {
        LOG_WARN("failed to calculate right expression", K(ret));
      } else {
        if (res_str.empty() && is_oracle_mode) {
          res_vec->set_null(idx);
        } else {
          res_vec->set_string(idx, res_str);
        }
      }
    }
  } // for end
  return ret;
}

int ObExprRight::calc_right_vector(const ObExpr &expr,
                                    ObEvalCtx &ctx,
                                    const ObBitVector &skip,
                                    const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(expr.arg_cnt_ == 2);
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval string param", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval number param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);

    // Check if the length parameter is constant
    if (expr.args_[1]->is_static_const_) {
      // Constant path: extract constant value and use optimized path
      ObIVector *num_vec = static_cast<ObIVector *>(expr.args_[1]->get_vector(ctx));
      int64_t const_len = 0;
      if (num_vec->is_null(0)) {
        // If constant is NULL, set all results to NULL
        ObIVector *res_vec = expr.get_vector(ctx);
        ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
        for (int64_t idx = bound.start(); idx < bound.end(); ++idx) {
          if (!skip.at(idx) && !eval_flags.at(idx)) {
            res_vec->set_null(idx);
          }
        }
      } else {
        // Extract constant value from datum
        const_len = num_vec->get_int(0);
        if (OB_SUCC(ret)) {
          // Dispatch based on arg and res formats for constant path
          if (VEC_DISCRETE == arg_format && VEC_DISCRETE == res_format) {
            ret = calc_right_vector_const<ObDiscreteFormat, ObDiscreteFormat>(
              expr, ctx, skip, bound, const_len);
          } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {
            ret = calc_right_vector_const<UniformFormat, ObDiscreteFormat>(
              expr, ctx, skip, bound, const_len);
          } else if (VEC_CONTINUOUS == arg_format && VEC_DISCRETE == res_format) {
            ret = calc_right_vector_const<ObContinuousFormat, ObDiscreteFormat>(
              expr, ctx, skip, bound, const_len);
          } else if (VEC_DISCRETE == arg_format && VEC_CONTINUOUS == res_format) {
            ret = calc_right_vector_const<ObDiscreteFormat, ObContinuousFormat>(
              expr, ctx, skip, bound, const_len);
          } else if (VEC_UNIFORM == arg_format && VEC_CONTINUOUS == res_format) {
            ret = calc_right_vector_const<UniformFormat, ObContinuousFormat>(
              expr, ctx, skip, bound, const_len);
          } else if (VEC_CONTINUOUS == arg_format && VEC_CONTINUOUS == res_format) {
            ret = calc_right_vector_const<ObContinuousFormat, ObContinuousFormat>(
              expr, ctx, skip, bound, const_len);
          } else {
            ret = calc_right_vector_const<ObVectorBase, ObVectorBase>(
              expr, ctx, skip, bound, const_len);
          }
        }
      }
    } else {
      // Non-constant path: use generic dispatch without template specialization
      ret = calc_right_vector_non_const(expr, ctx, skip, bound);
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprRight, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

}// namespace sql
}// namespace oceanbase
