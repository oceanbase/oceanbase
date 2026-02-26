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

#include "vector_cast.h"

#define USING_LOG_PREFIX SQL

namespace oceanbase
{
using namespace common;
namespace sql
{

int _eval_arg_vec_cast(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                   const EvalBound &bound)
{
#define SET_NULLS(out_vec_type)                                                                    \
  for (int i = bound.start(); i < bound.end(); i++) {                                              \
    if (eval_flags.at(i) || skip.at(i)) {                                                          \
      continue;                                                                                    \
    } else {                                                                                       \
      static_cast<out_vec_type *>(output_vector)->set_null(i);                                     \
      eval_flags.set(i);                                                                           \
    }                                                                                              \
  }
  int ret = OB_SUCCESS;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  VectorFormat out_fmt = expr.get_format(ctx);
  ObIVector *output_vector = expr.get_vector(ctx);
  if (out_fmt == VEC_UNIFORM) {
    SET_NULLS(ObUniformFormat<false>);
  } else if (out_fmt == VEC_UNIFORM_CONST) {
    SET_NULLS(ObUniformFormat<true>);
  } else {
    SET_NULLS(ObBitmapNullVectorBase);
  }
  return ret;
#undef SET_NULLS
}

extern int __init_all_vec_cast_funcs();

static int init_vector_cast_ret = __init_all_vec_cast_funcs();

// 0 for explicit cast, 1 for implicit cast
ObExpr::EvalVectorFunc VECTOR_CAST_FUNCS[MAX_VEC_TC][MAX_VEC_TC][2] = {};
ObExpr::EvalVectorFunc VECTOR_EVAL_ARG_CAST_FUNCS[MAX_VEC_TC][MAX_VEC_TC][2] = {};

extern int cast_not_expected(const sql::ObExpr &, sql::ObEvalCtx &, sql::ObDatum &);
extern int cast_not_support(const sql::ObExpr &, sql::ObEvalCtx &, sql::ObDatum &);
extern int cast_inconsistent_types(const sql::ObExpr &, sql::ObEvalCtx &, sql::ObDatum &);
extern int cast_inconsistent_types_json(const sql::ObExpr &, sql::ObEvalCtx &, sql::ObDatum &);
extern int cast_to_udt_not_support(const sql::ObExpr &, sql::ObEvalCtx &, sql::ObDatum &);
extern int unknown_other(const sql::ObExpr &, sql::ObEvalCtx &, sql::ObDatum &);

ObExpr::EvalVectorFunc VectorCasterUtil::get_vector_cast(const VecValueTypeClass in_tc,
                                                         const VecValueTypeClass out_tc,
                                                         const bool is_eval_arg_cast,
                                                         ObExpr::EvalFunc row_cast_fn,
                                                         const ObCastMode cast_mode)
{
  ObExpr::EvalVectorFunc ret_func = nullptr;
  ObExpr::EvalFunc temp_func = nullptr;
  if (is_eval_arg_cast) {
    ret_func = CM_IS_EXPLICIT_CAST(cast_mode) ? VECTOR_EVAL_ARG_CAST_FUNCS[in_tc][out_tc][EXPLICIT_CAST_FLAG] :
                                                VECTOR_EVAL_ARG_CAST_FUNCS[in_tc][out_tc][IMPLICIT_CAST_FLAG];
  } else if (row_cast_fn == (temp_func = cast_not_expected)
             || row_cast_fn == cast_not_support
             || row_cast_fn == cast_inconsistent_types
             || row_cast_fn == cast_inconsistent_types_json
             || row_cast_fn == cast_to_udt_not_support
             || row_cast_fn == unknown_other) {
    // if rt_expr.eval_func_ is set to any of functions listing above,
    // casting routine must fail, we use row casting function to report err_code by set eval_vector_func_ to be nullptr
    // do nothing
  } else if (lib::is_oracle_mode()) {
    ret_func = VECTOR_CAST_FUNCS[in_tc][out_tc][CM_IS_IMPLICIT_CAST(cast_mode)];
  } else {
    ret_func = VECTOR_CAST_FUNCS[in_tc][out_tc][CM_IS_IMPLICIT_CAST(cast_mode)];
  }
  LOG_DEBUG("choose vector casting funcs", K(in_tc), K(out_tc), K(is_eval_arg_cast), K(cast_mode));
  return ret_func;
}

// static int init_vector_cast_ret =
//   Ob2DArrayConstIniter<MAX_VEC_TC, MAX_VEC_TC, VectorCastFuncInit, VEC_TC_NULL, VEC_TC_INTEGER>::init();

static void *g_ser_vector_cast_funcs[MAX_VEC_TC][MAX_VEC_TC][2];
static bool g_ser_vector_cast_funcs_init = ObFuncSerialization::convert_NxN_array(
  reinterpret_cast<void **>(g_ser_vector_cast_funcs),
  reinterpret_cast<void **>(VECTOR_CAST_FUNCS),
  MAX_VEC_TC, 2, 0, 2);

static void *g_ser_vector_eval_arg_cast_funcs[MAX_VEC_TC][MAX_VEC_TC][2];
static bool g_ser_vector_eval_arg_cast_funcs_init = ObFuncSerialization::convert_NxN_array(
  reinterpret_cast<void **>(g_ser_vector_eval_arg_cast_funcs),
  reinterpret_cast<void **>(VECTOR_EVAL_ARG_CAST_FUNCS),
  MAX_VEC_TC, 2, 0, 2);

REG_SER_FUNC_ARRAY(OB_SFA_VECTOR_CAST, g_ser_vector_cast_funcs,
                   sizeof(g_ser_vector_cast_funcs) / sizeof(void *));
REG_SER_FUNC_ARRAY(OB_SFA_VECTOR_EVAL_ARG_CAST, g_ser_vector_eval_arg_cast_funcs,
                   sizeof(g_ser_vector_eval_arg_cast_funcs) / sizeof(void *));

} // end sql
} // end oceanbase