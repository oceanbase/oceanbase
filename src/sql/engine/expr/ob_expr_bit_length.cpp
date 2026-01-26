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

#include "sql/engine/expr/ob_expr_bit_length.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprBitLength::ObExprBitLength(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_BIT_LENGTH, N_BIT_LENGTH, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprBitLength::~ObExprBitLength()
{
}

int ObExprBitLength::calc_result_type1(ObExprResType &type, ObExprResType &text,
                                    ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    type.set_int();
    type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
    type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
    text.set_calc_type(common::ObVarcharType);
    OX(ObExprOperator::calc_result_flag1(type, text));
  }
  return ret;
}


int ObExprBitLength::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("bit length expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of bit length expr is null", K(ret), K(rt_expr.args_));
  } else {
    ObObjType text_type = rt_expr.args_[0]->datum_meta_.type_;
    ObObjTypeClass type_class = ob_obj_type_class(text_type);

    if (!ob_is_castable_type_class(type_class)) {
      rt_expr.eval_func_ = ObExprBitLength::calc_null;
      rt_expr.eval_vector_func_ = ObExprBitLength::calc_null_vector;
    } else {
      CK(ObVarcharType == text_type);
      rt_expr.eval_func_ = ObExprBitLength::calc_bit_length;
      rt_expr.eval_vector_func_ = ObExprBitLength::calc_bit_length_vector;
    }
  }
  return ret;
}

int ObExprBitLength::calc_null(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  UNUSED(expr);
  UNUSED(ctx);
  expr_datum.set_null();
  return OB_SUCCESS;
}

int ObExprBitLength::calc_bit_length(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *text_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, text_datum))) {
    LOG_WARN("eval param value failed", K(ret));
  } else if (text_datum->is_null()) {
    expr_datum.set_null();
  } else {
    expr_datum.set_int(static_cast<int64_t>(text_datum->len_ * 8));
  }
  return ret;
}

int ObExprBitLength::calc_null_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  UNUSED(ctx);
  ret = vector_bit_length<ObVectorBase, ObVectorBase, true>(VECTOR_EVAL_FUNC_ARG_LIST);
  return ret;
}

int ObExprBitLength::calc_bit_length_vector(VECTOR_EVAL_FUNC_ARG_DECL) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval bitlength param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_DISCRETE == arg_format && VEC_FIXED == res_format) {
      ret = vector_bit_length<StrDiscVec, IntegerFixedVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_DISCRETE == arg_format && VEC_UNIFORM == res_format) {
      ret = vector_bit_length<StrDiscVec, IntegerUniVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_CONTINUOUS == arg_format && VEC_FIXED == res_format) {
      ret = vector_bit_length<StrContVec, IntegerFixedVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_CONTINUOUS == arg_format && VEC_UNIFORM == res_format) {
      ret = vector_bit_length<StrContVec, IntegerUniVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_CONTINUOUS == arg_format && VEC_UNIFORM == res_format) {
      ret = vector_bit_length<StrUniVec, IntegerUniVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_CONTINUOUS == arg_format && VEC_UNIFORM == res_format) {
      ret = vector_bit_length<StrUniVec, IntegerFixedVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else {
      ret = vector_bit_length<ObVectorBase, ObVectorBase, false>(VECTOR_EVAL_FUNC_ARG_LIST);
    }
  }
  return ret;
}

template<typename ArgVec, typename ResVec, bool isNull>
int ObExprBitLength::vector_bit_length(VECTOR_EVAL_FUNC_ARG_DECL) {
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  size_t SIMD_BLOCKSIZE = 1;
  uint64_t RANGE_SIZE = bound.range_size();
  uint64_t group_cnt = 0;
  uint64_t remaining = 0;
  uint64_t remaining_offset = 0;
  if constexpr (isNull) {
      for (int i = bound.start(); i < bound.end() && OB_SUCC(ret); i++) {
        if (OB_LIKELY(!(skip.at(i) || eval_flags.at(i)))) {
            res_vec->set_null(i);
        }
      }
  } else {
    bool no_check_path = (!arg_vec->has_null() && bound.get_all_rows_active() && eval_flags.accumulate_bit_cnt(bound) == 0);
    if (no_check_path) {
      for (int i = bound.start(); i < bound.end() && OB_SUCC(ret); i++) {
        res_vec->set_int(i, static_cast<int64_t>(arg_vec->get_length(i) * 8));
      }
    } else {
      for (int i = bound.start(); i < bound.end() && OB_SUCC(ret); i++) {
        if (OB_LIKELY(!(skip.at(i) || eval_flags.at(i)))) {
          if (OB_UNLIKELY(arg_vec->is_null(i))) {
            res_vec->set_null(i);
          } else {
            res_vec->set_int(i, static_cast<int64_t>(arg_vec->get_length(i) * 8));
          }
        }
      }
    }
  }
  return ret;
}


}
}
