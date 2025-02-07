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

OB_NOINLINE int _eval_arg_vec_cast(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                   const EvalBound &bound);

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
    return _eval_arg_vec_cast(expr, ctx, skip, bound);
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

extern int expr_default_eval_vector_func(const ObExpr &, ObEvalCtx &, const ObBitVector &,
                                         const EvalBound &);

extern ObExpr::EvalVectorFunc VECTOR_CAST_FUNCS[MAX_VEC_TC][MAX_VEC_TC][2];
extern ObExpr::EvalVectorFunc VECTOR_EVAL_ARG_CAST_FUNCS[MAX_VEC_TC][MAX_VEC_TC][2];

template<int N, int M, bool defined = true>
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
    // VECTOR_EVAL_ARG_CAST_FUNCS[N][M][IMPLICIT_CAST_FLAG] =
    //   EvalArgCasterImpl<in_tc, out_tc, IMPLICIT_CAST_FLAG>::eval_vector;
    // accuracy checking will happend after explicit eval arg funcs,
    // if accuracy checker not defined, use `eval_default_eval_vector_func` otherwise
    // VECTOR_EVAL_ARG_CAST_FUNCS[N][M][EXPLICIT_CAST_FLAG] =
    //   ValueRangeChecker<out_tc, ObVectorBase>::defined_ ?
    //     EvalArgCasterImpl<in_tc, out_tc, EXPLICIT_CAST_FLAG>::eval_vector :
    //     expr_default_eval_vector_func;
  }
};

template<int N, int M>
struct VectorCastFuncInit<N, M, false>
{
  static void init_array()
  {
    constexpr VecValueTypeClass in_tc = static_cast<VecValueTypeClass>(N);
    constexpr VecValueTypeClass out_tc = static_cast<VecValueTypeClass>(M);
    VECTOR_CAST_FUNCS[N][M][IMPLICIT_CAST_FLAG] = expr_default_eval_vector_func;
    VECTOR_CAST_FUNCS[N][M][EXPLICIT_CAST_FLAG] = expr_default_eval_vector_func;
    // VECTOR_EVAL_ARG_CAST_FUNCS[N][M][IMPLICIT_CAST_FLAG] =
    //   EvalArgCasterImpl<in_tc, out_tc, IMPLICIT_CAST_FLAG>::eval_vector;
    // VECTOR_EVAL_ARG_CAST_FUNCS[N][M][EXPLICIT_CAST_FLAG] =
    //   ValueRangeChecker<out_tc, ObVectorBase>::defined_ ?
    //     EvalArgCasterImpl<in_tc, out_tc, EXPLICIT_CAST_FLAG>::eval_vector :
    //     expr_default_eval_vector_func;
  }
};

template<int N, int M, bool defined = true>
struct EvalArgVecCasterFuncInit
{
  static void init_array()
  {
    constexpr VecValueTypeClass in_tc = static_cast<VecValueTypeClass>(N);
    constexpr VecValueTypeClass out_tc = static_cast<VecValueTypeClass>(M);
    VECTOR_EVAL_ARG_CAST_FUNCS[N][M][IMPLICIT_CAST_FLAG] =
      EvalArgCasterImpl<in_tc, out_tc, IMPLICIT_CAST_FLAG>::eval_vector;
    VECTOR_EVAL_ARG_CAST_FUNCS[N][M][EXPLICIT_CAST_FLAG] =
      EvalArgCasterImpl<in_tc, out_tc, EXPLICIT_CAST_FLAG>::eval_vector;
  }
};

template<int N, int M>
struct EvalArgVecCasterFuncInit<N, M, false>
{
  static void init_array()
  {
    constexpr VecValueTypeClass in_tc = static_cast<VecValueTypeClass>(N);
    constexpr VecValueTypeClass out_tc = static_cast<VecValueTypeClass>(M);
    VECTOR_EVAL_ARG_CAST_FUNCS[N][M][IMPLICIT_CAST_FLAG] =
      EvalArgCasterImpl<in_tc, out_tc, IMPLICIT_CAST_FLAG>::eval_vector;
    VECTOR_EVAL_ARG_CAST_FUNCS[N][M][EXPLICIT_CAST_FLAG] = expr_default_eval_vector_func;
  }
};

template<int N, int M>
using VectorCastIniter = VectorCastFuncInit<N, M,
                                            VectorCaster<static_cast<VecValueTypeClass>(N),
                                            static_cast<VecValueTypeClass>(M), IMPLICIT_CAST_FLAG>::defined_>;

template<int N, int M>
using EvalArgVecCasterIniter = EvalArgVecCasterFuncInit<N, M,
                                                        BatchValueRangeChecker<static_cast<VecValueTypeClass>(M),
                                                                          ObVectorBase>::defined_>;

} // end sql
} // end oceanbase