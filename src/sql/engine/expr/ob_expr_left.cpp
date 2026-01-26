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
#include "sql/engine/expr/ob_expr_left.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/charset/ob_charset_string_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprLeft::ObExprLeft(ObIAllocator &alloc)
: ObStringExprOperator(alloc, T_FUN_SYS_LEFT, "left", 2, VALID_FOR_GENERATED_COL)
{
}

ObExprLeft::~ObExprLeft()
{
}

// to make "select left('abcd', '1.9')" compatible with mysql
int ObExprLeft::cast_param_type(const ObObj& in,
                                ObExprCtx& expr_ctx,
                                ObObj& out) const
{
  int ret = OB_SUCCESS;
  ObCastMode cast_mode = CM_NONE;
  EXPR_DEFINE_CAST_CTX(expr_ctx, cast_mode);
  // select left('abcd', '1.9'); -- MySQL will trunc '1.9' to 2
  // select left('abcd', 1.9); -- MySQL will round 1.9 to 1
  if (ObVarcharType == in.get_type()) {
    int64_t tmp = 0;
    if (OB_FAIL(ObExprUtil::get_trunc_int64(in, expr_ctx, tmp))) {
      LOG_WARN("ObExprLeft get_trunc_int64 failed", K(in.get_type()));
    } else if (INT_MAX < tmp) {
      out.set_int(INT_MAX);
    } else if (INT_MIN > tmp) {
      out.set_int(INT_MIN);
    } else {
      out.set_int(static_cast<int>(tmp));
    }
  } else if (OB_FAIL(ObObjCaster::to_type(ObIntType, cast_ctx, in, out))) {
    LOG_WARN("ObExprLeft to_type failed", K(in.get_type()));
  }
  return ret;
}

int ObExprLeft::calc_result_type2(ObExprResType &type,
																	ObExprResType &type1,
																	ObExprResType &type2,
																	ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (session->is_varparams_sql_prepare()) {
    // the ps prepare stage does not do type deduction, and directly gives a default type.
    type.set_char();
    type.set_default_collation_type();
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_length(0);
  } else {
    type2.set_calc_type(ObIntType);
    type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
    type.set_varchar();
    int64_t result_len = type1.get_length();
    const ObObj &result_len_obj = type2.get_param();
    ObObj int_obj;
    ObExprCtx tmp_expr_ctx;
    ObArenaAllocator allocator(common::ObModIds::OB_SQL_EXPR_CALC);
    tmp_expr_ctx.calc_buf_ = &allocator;
    tmp_expr_ctx.cast_mode_ = type_ctx.get_cast_mode();
    if (OB_FAIL(cast_param_type(result_len_obj, tmp_expr_ctx, int_obj))) {
      LOG_WARN("cast_param_type failed", K(result_len_obj.get_type()));
    } else if (int_obj.get_int() <= 0) {
      type.set_char();
      type.set_length(0);
    } else {
      type.set_length(int_obj.get_int());
    }

    OZ(aggregate_charsets_for_string_result(type, &type1, 1, type_ctx));
    OX(type1.set_calc_type(ObVarcharType));
    OX(type1.set_calc_collation_type(type.get_collation_type()));
    OX(type1.set_calc_collation_level(type.get_collation_level()));
  }
  return ret;
}

int calc_left(ObString &res_str, const ObString &text, const ObCollationType type,
							const int64_t required_num_char)
{
	int ret = OB_SUCCESS;
	const char* str_ptr = text.ptr();
	int64_t str_length = text.length();
	if(OB_ISNULL(str_ptr) && 0 != str_length) {
		ret = OB_INVALID_ARGUMENT;
		LOG_WARN("invalid argument", K(ret));
	} else {
		int64_t input_num_char = ObCharset::strlen_char(type, text.ptr(), text.length());
		int64_t expected_num_char = min(required_num_char, input_num_char);
		int64_t start_pos = 0;
		int64_t char_length = 0;
		while((start_pos < str_length) && OB_SUCC(ret) && (0 < expected_num_char)) {
			if(OB_FAIL(ObCharset::first_valid_char(type, str_ptr + start_pos,
                                             str_length - start_pos, char_length))) {
				LOG_WARN("get char failed", K(ret));
			} else {
				start_pos += char_length;
				--expected_num_char;
			}
		}
		if(OB_SUCC(ret)) {
      res_str.assign_ptr(str_ptr, static_cast<int32_t> (start_pos));
		}
	}
	return ret;
}

int calc_left_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  // left(s, n)
  ObDatum *s_datum = NULL;
  ObDatum *n_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, s_datum)) ||
      OB_FAIL(expr.args_[1]->eval(ctx, n_datum))) {
    LOG_WARN("eval arg failed", K(ret), KP(s_datum), KP(n_datum));
  } else if (s_datum->is_null() || n_datum->is_null()) {
    res_datum.set_null();
  } else {
    // res_str会指向s_datum的内存空间，所以下面不能改变res_str指向的字符串
    ObString res_str;
    const ObCollationType arg_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    if (OB_FAIL(calc_left(res_str, s_datum->get_string(), arg_cs_type, n_datum->get_int()))) {
      LOG_WARN("failed to calculate left expression", K(ret));
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


int ObExprLeft::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_left_expr;
  rt_expr.eval_vector_func_ = calc_left_vector;
  return ret;
}

template <typename ArgVec, typename ResVec, bool IsAscii, bool CanDoAsciiOptimize, bool HasNull>
int ObExprLeft::calc_left_vector_const_inner(const ObExpr &expr,
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
        res_vec->set_string(idx, text.ptr(), min(text.length(), const_len));
      } else {
        int64_t char_pos = ObCharsetStringHelper::charpos_optimized(cs_type, text.ptr(), text.length(), const_len);
        if (char_pos < text.length()) {
          res_vec->set_string(idx, text.ptr(), char_pos);
        } else {
          res_vec->set_string(idx, text);
        }
      }
    }
  } // for end
  return ret;
}

template <typename ArgVec, typename ResVec>
int ObExprLeft::calc_left_vector_const(const ObExpr &expr,
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
        ret = calc_left_vector_const_inner<ArgVec, ResVec, true, true, true>(expr, ctx, skip, bound, const_len);
      } else {
        ret = calc_left_vector_const_inner<ArgVec, ResVec, true, true, false>(expr, ctx, skip, bound, const_len);
      }
    } else {
      if (has_null) {
        ret = calc_left_vector_const_inner<ArgVec, ResVec, true, false, true>(expr, ctx, skip, bound, const_len);
      } else {
        ret = calc_left_vector_const_inner<ArgVec, ResVec, true, false, false>(expr, ctx, skip, bound, const_len);
      }
    }
  } else {
    if (can_do_ascii_optimize) {
      if (has_null) {
        ret = calc_left_vector_const_inner<ArgVec, ResVec, false, true, true>(expr, ctx, skip, bound, const_len);
      } else {
        ret = calc_left_vector_const_inner<ArgVec, ResVec, false, true, false>(expr, ctx, skip, bound, const_len);
      }
    } else {
      if (has_null) {
        ret = calc_left_vector_const_inner<ArgVec, ResVec, false, false, true>(expr, ctx, skip, bound, const_len);
      } else {
        ret = calc_left_vector_const_inner<ArgVec, ResVec, false, false, false>(expr, ctx, skip, bound, const_len);
      }
    }
  }

  return ret;
}

int ObExprLeft::calc_left_vector_non_const(const ObExpr &expr,
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
      int64_t required_num_char = 0;
      ObString text = arg_vec->get_string(idx);
      required_num_char = num_vec->get_int(idx);
      ObString res_str;
      if (OB_FAIL(calc_left(res_str, text, cs_type, required_num_char))) {
        LOG_WARN("failed to calculate left expression", K(ret));
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

int ObExprLeft::calc_left_vector(const ObExpr &expr,
                                 ObEvalCtx &ctx,
                                 const ObBitVector &skip,
                                 const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_vector_param_value(ctx, skip, bound))) {
    LOG_WARN("fail to eval vector param value", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);

    // Check if the length parameter is constant
    if (expr.args_[1]->is_static_const_) {
      // Constant path: extract constant value and use optimized path
      ObIVector *num_vec = expr.args_[1]->get_vector(ctx);
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
            ret = calc_left_vector_const<ObDiscreteFormat, ObDiscreteFormat>(
              expr, ctx, skip, bound, const_len);
          } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {
            ret = calc_left_vector_const<UniformFormat, ObDiscreteFormat>(
              expr, ctx, skip, bound, const_len);
          } else if (VEC_CONTINUOUS == arg_format && VEC_DISCRETE == res_format) {
            ret = calc_left_vector_const<ObContinuousFormat, ObDiscreteFormat>(
              expr, ctx, skip, bound, const_len);
          } else if (VEC_DISCRETE == arg_format && VEC_CONTINUOUS == res_format) {
            ret = calc_left_vector_const<ObDiscreteFormat, ObContinuousFormat>(
              expr, ctx, skip, bound, const_len);
          } else if (VEC_UNIFORM == arg_format && VEC_CONTINUOUS == res_format) {
            ret = calc_left_vector_const<UniformFormat, ObContinuousFormat>(
              expr, ctx, skip, bound, const_len);
          } else if (VEC_CONTINUOUS == arg_format && VEC_CONTINUOUS == res_format) {
            ret = calc_left_vector_const<ObContinuousFormat, ObContinuousFormat>(
              expr, ctx, skip, bound, const_len);
          } else {
            ret = calc_left_vector_const<ObVectorBase, ObVectorBase>(
              expr, ctx, skip, bound, const_len);
          }
        }
      }
    } else {
      // Non-constant path: use generic dispatch without template specialization
      ret = calc_left_vector_non_const(expr, ctx, skip, bound);
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprLeft, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
