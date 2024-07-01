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

#include "sql/engine/expr/ob_expr.h"
#include "vector_cast.h"
#include "share/ob_errno.h"
#include "share/datum/ob_datum_util.h"

#define USING_LOG_PREFIX SQL

namespace oceanbase
{
using namespace common;
namespace sql
{
template <typename IN_VECTOR, typename OUT_VECTOR>
struct _eval_arg_impl
{
  static int eval_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                         const EvalBound &bound)
  {
    int ret = OB_SUCCESS;
    IN_VECTOR *input_vector = static_cast<IN_VECTOR *>(expr.args_[0]->get_vector(ctx));
    OUT_VECTOR *output_vector = static_cast<OUT_VECTOR *>(expr.get_vector(ctx));
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    const char *src = nullptr;
    int32_t src_len = 0;
    if (OB_LIKELY(bound.get_all_rows_active() && eval_flags.accumulate_bit_cnt(bound) == 0)) {
      if (!input_vector->has_null()) {
        for (int i = bound.start(); i < bound.end(); i++) {
          input_vector->get_payload(i, src, src_len);
          output_vector->set_payload_shallow(i, src, src_len);
        }
      } else {
        for (int i = bound.start(); i < bound.end(); i++) {
          if (input_vector->is_null(i)) {
            output_vector->set_null(i);
          } else {
            input_vector->get_payload(i, src, src_len);
            output_vector->set_payload_shallow(i, src, src_len);
          }
        }
      }
      eval_flags.set_all(bound.start(), bound.end());
    } else {
      for (int i = bound.start(); i < bound.end(); i++) {
        if(eval_flags.at(i) || skip.at(i)) {
          continue;
        } else if (input_vector->is_null(i)) {
          output_vector->set_null(i);
        } else {
          input_vector->get_payload(i, src, src_len);
          output_vector->set_payload_shallow(i, src, src_len);
        }
        eval_flags.set(i);
      }
    }
    return ret;
  }
};

template <VecValueTypeClass in_tc, VecValueTypeClass out_tc, bool implicit>
struct EvalArgCasterImpl
{};

template <VecValueTypeClass in_tc, VecValueTypeClass out_tc>
struct EvalArgCasterImpl<in_tc, out_tc, IMPLICIT_CAST_FLAG>
  : public VecCastFormatWrapper<_eval_arg_impl, in_tc, out_tc>
{};

template<VecValueTypeClass out_tc>
struct EvalArgCasterImpl<VEC_TC_NULL, out_tc, IMPLICIT_CAST_FLAG>
{
  static int eval_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
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
};

template<VecValueTypeClass in_tc, VecValueTypeClass out_tc>
struct EvalArgCasterImpl<in_tc, out_tc, EXPLICIT_CAST_FLAG>
{
  static int eval_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                         const EvalBound &bound)
  {
    int ret =
      EvalArgCasterImpl<in_tc, out_tc, IMPLICIT_CAST_FLAG>::eval_vector(expr, ctx, skip, bound);
    if (OB_FAIL(ret)) {
      LOG_WARN("implicit eval arg for casting failed", K(ret));
    } else {
      VectorFormat out_fmt = expr.get_format(ctx);
      int warning = OB_SUCCESS;
      switch (out_fmt) {
      case common::VEC_UNIFORM: {
        ret = std::conditional<is_uniform_vec(out_tc),
                               BatchValueRangeChecker<out_tc, ObUniformFormat<false>>,
                               DummyChecker>::type::check(expr, ctx, bound, skip, warning);
        break;
      }
      case common::VEC_UNIFORM_CONST: {
        ret = std::conditional<is_uniform_vec(out_tc),
                               BatchValueRangeChecker<out_tc, ObUniformFormat<true>>,
                               DummyChecker>::type::check(expr, ctx, bound, skip, warning);
        break;
      }
      case common::VEC_FIXED: {
        ret = std::conditional<is_fixed_length_vec(out_tc),
                               BatchValueRangeChecker<out_tc, ObFixedLengthFormat<RTCType<out_tc>>>,
                               DummyChecker>::type::check(expr, ctx, bound, skip, warning);
        break;
      }
      case common::VEC_DISCRETE: {
        ret = std::conditional<is_discrete_vec(out_tc),
                               BatchValueRangeChecker<out_tc, ObDiscreteFormat>,
                               DummyChecker>::type::check(expr, ctx, bound, skip, warning);
        break;
      }
      case common::VEC_CONTINUOUS: {
        ret = std::conditional<is_continuous_vec(out_tc),
                               BatchValueRangeChecker<out_tc, ObContinuousFormat>,
                               DummyChecker>::type::check(expr, ctx, bound, skip, warning);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid format", K(out_fmt));
        break;
      }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("accuracy check failed", K(in_tc), K(out_tc), K(out_fmt), K(warning), K(ret));
      }
    }
    return ret;
  }
private:
  struct DummyChecker
  {
    inline static int check(const ObExpr &expr, ObEvalCtx &ctx, const EvalBound &bound,
                            const ObBitVector &skip, int &warning)
    {
      return OB_SUCCESS;
    }
  };
};

template<VecValueTypeClass out_tc>
struct EvalArgCasterImpl<VEC_TC_NULL, out_tc, EXPLICIT_CAST_FLAG>
{
  inline static int eval_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                         const EvalBound &bound)
  {
    return EvalArgCasterImpl<VEC_TC_NULL, out_tc, IMPLICIT_CAST_FLAG>::eval_vector(expr, ctx, skip,
                                                                                   bound);
  }
};

extern int cast_not_expected(const sql::ObExpr &, sql::ObEvalCtx &, sql::ObDatum &);
extern int cast_not_support(const sql::ObExpr &, sql::ObEvalCtx &, sql::ObDatum &);
extern int cast_inconsistent_types(const sql::ObExpr &, sql::ObEvalCtx &, sql::ObDatum &);
extern int cast_inconsistent_types_json(const sql::ObExpr &, sql::ObEvalCtx &, sql::ObDatum &);
extern int cast_to_udt_not_support(const sql::ObExpr &, sql::ObEvalCtx &, sql::ObDatum &);
extern int unknown_other(const sql::ObExpr &, sql::ObEvalCtx &, sql::ObDatum &);

extern int expr_default_eval_vector_func(const ObExpr &, ObEvalCtx &, const ObBitVector &,
                                         const EvalBound &);

// 0 for explicit cast, 1 for implicit cast
ObExpr::EvalVectorFunc VECTOR_CAST_FUNCS[MAX_VEC_TC][MAX_VEC_TC][2] = {};
ObExpr::EvalVectorFunc VECTOR_EVAL_ARG_CAST_FUNCS[MAX_VEC_TC][MAX_VEC_TC][2] = {};
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

template<int N, int M>
struct VectorCastFuncInit
{
  static void init_array()
  {
    constexpr VecValueTypeClass in_tc = static_cast<VecValueTypeClass>(N);
    constexpr VecValueTypeClass out_tc = static_cast<VecValueTypeClass>(M);
    // if eval_vector is not defined, func ptr must set to `expr_default_eval_vector_func` for upgrading compatiblity.
    VECTOR_CAST_FUNCS[N][M][IMPLICIT_CAST_FLAG] =
      VectorCaster<in_tc, out_tc, IMPLICIT_CAST_FLAG>::defined_ ?
        VectorCaster<in_tc, out_tc, IMPLICIT_CAST_FLAG>::eval_vector :
        expr_default_eval_vector_func;
    VECTOR_CAST_FUNCS[N][M][EXPLICIT_CAST_FLAG] =
      VectorCaster<in_tc, out_tc, IMPLICIT_CAST_FLAG>::defined_ ?
        VectorCaster<in_tc, out_tc, EXPLICIT_CAST_FLAG>::eval_vector :
        expr_default_eval_vector_func;
    // eval arg funcs
    VECTOR_EVAL_ARG_CAST_FUNCS[N][M][IMPLICIT_CAST_FLAG] =
      EvalArgCasterImpl<in_tc, out_tc, IMPLICIT_CAST_FLAG>::eval_vector;
    // accuracy checking will happend after explicit eval arg funcs,
    // if accuracy checker not defined, use `eval_default_eval_vector_func` otherwise
    VECTOR_EVAL_ARG_CAST_FUNCS[N][M][EXPLICIT_CAST_FLAG] =
      ValueRangeChecker<out_tc, ObVectorBase>::defined_ ?
        EvalArgCasterImpl<in_tc, out_tc, EXPLICIT_CAST_FLAG>::eval_vector :
        expr_default_eval_vector_func;
  }
};

static int init_vector_cast_ret =
  Ob2DArrayConstIniter<MAX_VEC_TC, MAX_VEC_TC, VectorCastFuncInit, VEC_TC_NULL, VEC_TC_INTEGER>::init();

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