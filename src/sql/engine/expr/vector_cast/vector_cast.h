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

#ifndef OCEANBASE_SQL_ENG_EXPR_VECTOR_CAST_H_
#define OCEANBASE_SQL_ENG_EXPR_VECTOR_CAST_H_

#include "sql/engine/expr/vector_cast/util.h"

#define IMPLICIT_CAST_FLAG true
#define EXPLICIT_CAST_FLAG false

// used to define type casting funcs
#define DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, out_tc)                                               \
  template <typename IN_VECTOR, typename OUT_VECTOR>                                               \
  struct _vector_caster_impl<in_tc, out_tc, IMPLICIT_CAST_FLAG, IN_VECTOR, OUT_VECTOR>             \
  {                                                                                                \
    inline static int eval_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,     \
                                  const EvalBound &bound);                                         \
  };                                                                                               \
  template <typename IN_VECTOR, typename OUT_VECTOR>                                               \
  struct __implicit_cast_impl_##in_tc##_##out_tc                                                   \
    : public _vector_caster_impl<in_tc, out_tc, IMPLICIT_CAST_FLAG, IN_VECTOR, OUT_VECTOR>         \
  {};                                                                                              \
  template <typename IN_VECTOR, typename OUT_VECTOR>                                               \
  struct __explicit_cast_impl__##in_tc##_##out_tc                                                  \
    : public _vector_caster_impl<in_tc, out_tc, EXPLICIT_CAST_FLAG, IN_VECTOR, OUT_VECTOR>         \
  {};                                                                                              \
  template <>                                                                                      \
  struct VectorCaster<in_tc, out_tc, true>                                                         \
    : public VecCastFormatWrapper<__implicit_cast_impl_##in_tc##_##out_tc, in_tc, out_tc>          \
  {                                                                                                \
    static const constexpr bool defined_ = true;                                                   \
  };                                                                                               \
  template <>                                                                                      \
  struct VectorCaster<in_tc, out_tc, false>                                                        \
    : public VecCastFormatWrapper<__explicit_cast_impl__##in_tc##_##out_tc, in_tc, out_tc>         \
  {};                                                                                              \
  template <typename IN_VECTOR, typename OUT_VECTOR>                                               \
  inline int                                                                                       \
  _vector_caster_impl<in_tc, out_tc, IMPLICIT_CAST_FLAG, IN_VECTOR, OUT_VECTOR>::eval_vector(      \
    const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)

// copy from `ob_datum_cast.cpp`
#define CAST_FAIL(stmt)                                                                            \
  (OB_UNLIKELY(                                                                                    \
    (OB_SUCCESS != (ret = VectorCasterHelper::get_cast_ret((expr.extra_), (stmt), warning)))))

namespace oceanbase
{
namespace sql
{

template<VecValueTypeClass in_tc, VecValueTypeClass out_tc, bool implicit>
struct VectorCaster
{
  static const constexpr bool defined_ = false;
  inline static int eval_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                         const EvalBound &bound)
  {
    int ret = OB_NOT_IMPLEMENT;
    SQL_LOG(WARN, "not implemented", K(ret));
    return ret;
  }
};

struct VectorCasterHelper
{
  // copy from `ob_datum_cast.cpp`
  static int get_cast_ret(const ObCastMode &cast_mode, int ret, int &warning)
  {
    // compatibility for old ob
    if (OB_UNLIKELY(OB_ERR_UNEXPECTED_TZ_TRANSITION == ret)
        || OB_UNLIKELY(OB_ERR_UNKNOWN_TIME_ZONE == ret)) {
      ret = OB_INVALID_DATE_VALUE;
    } else if (OB_SUCCESS != ret && CM_IS_WARN_ON_FAIL(cast_mode)) {
      warning = ret;
      ret = OB_SUCCESS;
    }
    return ret;
  }
};

template <template <typename IN_VECTOR, typename OUT_VECTOR> class CasterImpl,
          VecValueTypeClass in_tc, VecValueTypeClass out_tc>

struct VecCastFormatWrapper
{
  inline static int eval_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                const EvalBound &bound)
  {
#define CHOOSE_OUT_FMT(in_vector, out_fmt)                                                         \
  switch (out_fmt) {                                                                               \
  case VEC_UNIFORM: {                                                                              \
    if (is_uniform_vec(out_tc)) {                                                                  \
      ret = CasterImpl<in_vector, ObUniformFormat<false>>::eval_vector(expr, ctx, skip, bound);    \
    } else {                                                                                       \
      ret = OB_ERR_UNEXPECTED;                                                                     \
    }                                                                                              \
    break;                                                                                         \
  }                                                                                                \
  case VEC_UNIFORM_CONST: {                                                                        \
    if (is_uniform_vec(out_tc)) {                                                                  \
      ret = CasterImpl<in_vector, ObUniformFormat<true>>::eval_vector(expr, ctx, skip, bound);     \
    } else {                                                                                       \
      ret = OB_ERR_UNEXPECTED;                                                                     \
    }                                                                                              \
    break;                                                                                         \
  }                                                                                                \
  case VEC_FIXED: {                                                                                \
    if (is_fixed_length_vec(out_tc)) {                                                             \
      ret = CasterImpl<in_vector, ObFixedLengthFormat<RTCType<out_tc>>>::eval_vector(expr, ctx,    \
                                                                                     skip, bound); \
    } else {                                                                                       \
      ret = OB_ERR_UNEXPECTED;                                                                     \
    }                                                                                              \
    break;                                                                                         \
  }                                                                                                \
  case VEC_DISCRETE: {                                                                             \
    if (is_discrete_vec(out_tc)) {                                                                 \
      ret = CasterImpl<in_vector, ObDiscreteFormat>::eval_vector(expr, ctx, skip, bound);          \
    } else {                                                                                       \
      ret = OB_ERR_UNEXPECTED;                                                                     \
    }                                                                                              \
    break;                                                                                         \
  }                                                                                                \
  case VEC_CONTINUOUS: {                                                                           \
    if (is_continuous_vec(out_tc)) {                                                               \
      ret = CasterImpl<in_vector, ObContinuousFormat>::eval_vector(expr, ctx, skip, bound);        \
    } else {                                                                                       \
      ret = OB_ERR_UNEXPECTED;                                                                     \
    }                                                                                              \
    break;                                                                                         \
  }                                                                                                \
  default: {                                                                                       \
    ret = OB_ERR_UNEXPECTED;                                                                       \
  }                                                                                                \
  }
    int ret = OB_SUCCESS;
    if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
      SQL_LOG(WARN, "eval vector failed", K(ret));
    } else {
      VectorFormat in_fmt = expr.args_[0]->get_format(ctx);
      VectorFormat out_fmt = expr.get_format(ctx);
      switch (in_fmt) {
      case common::VEC_UNIFORM: {
        if (is_uniform_vec(in_tc)) {
          CHOOSE_OUT_FMT(ObUniformFormat<false>, out_fmt);
        } else {
          ret = OB_ERR_UNEXPECTED;
        }
        break;
      }
      case common::VEC_UNIFORM_CONST: {
        if (is_uniform_vec(in_tc)) {
          CHOOSE_OUT_FMT(ObUniformFormat<true>, out_fmt);
        } else {
          ret = OB_ERR_UNEXPECTED;
        }
        break;
      }
      case common::VEC_FIXED: {
        if (is_fixed_length_vec(in_tc)) {
          CHOOSE_OUT_FMT(ObFixedLengthFormat<RTCType<in_tc>>, out_fmt);
        } else {
          ret = OB_ERR_UNEXPECTED;
        }
        break;
      }
      case common::VEC_DISCRETE: {
        if (is_discrete_vec(in_tc)) {
          CHOOSE_OUT_FMT(ObDiscreteFormat, out_fmt);
        } else {
          ret = OB_ERR_UNEXPECTED;
        }
        break;
      }
      case common::VEC_CONTINUOUS: {
        if (is_continuous_vec(in_tc)) {
          CHOOSE_OUT_FMT(ObContinuousFormat, out_fmt);
        } else {
          ret = OB_ERR_UNEXPECTED;
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      }
      if (OB_FAIL(ret)) {
        SQL_LOG(WARN, "choose format failed", K(in_fmt), K(out_fmt), K(in_tc), K(out_tc));
      }
    }
    return ret;
  }
#undef CHOOSE_OUT_FMT
};

template <VecValueTypeClass in_tc, VecValueTypeClass out_tc, bool is_implicit,
          typename IN_VECTOR, typename OUT_VECTOR>
struct _vector_caster_impl {};

// explicit casting functions
template<VecValueTypeClass in_tc, VecValueTypeClass out_tc, typename IN_VECTOR, typename OUT_VECTOR>
struct _vector_caster_impl<in_tc, out_tc, EXPLICIT_CAST_FLAG, IN_VECTOR, OUT_VECTOR>
{
  inline static int eval_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                const EvalBound &bound)
  {
    int ret = _vector_caster_impl<in_tc, out_tc, true, IN_VECTOR, OUT_VECTOR>::eval_vector(
      expr, ctx, skip, bound);
    int warning = ret;
    if (OB_FAIL(ret)) {
      SQL_LOG(WARN, "implicit cast failed", K(in_tc), K(out_tc), K(bound));
    } else {
      ret = BatchValueRangeChecker<out_tc, OUT_VECTOR>::check(expr, ctx, bound, skip, warning);
      if (OB_FAIL(ret)) {
        SQL_LOG(WARN, "accuracy check failed", K(ret), K(out_tc));
      }
    }
    return ret;
  }
};

struct VectorCasterUtil
{
  static ObExpr::EvalVectorFunc get_vector_cast(const VecValueTypeClass in_tc,
                                                const VecValueTypeClass out_tc,
                                                const bool is_eval_arg_cast,
                                                ObExpr::EvalFunc row_cast_fn,
                                                const ObCastMode cast_mode);
};
} // end sql
} // end oceanbase

// 出现错误时,处理如下：
// 如果只设置了WARN_ON_FAIL，会覆盖错误码
// 如果设置了WARN_ON_FAIL和ZERO_ON_WARN,会覆盖错误码，且结果被置为0
// 如果设置了WARN_ON_FAIL和NULL_ON_WARN,会覆盖错误码，且结果被置为null
#define SET_RES_OBJ(cast_mode, func_val, zero_value, value, idx)       \
  do {                                                            \
    if (OB_SUCC(ret)) {                                           \
      if (OB_SUCCESS == warning                                   \
          || OB_ERR_TRUNCATED_WRONG_VALUE == warning              \
          || OB_DATA_OUT_OF_RANGE == warning                      \
          || OB_ERR_DATA_TRUNCATED == warning                     \
          || OB_ERR_DOUBLE_TRUNCATED == warning                   \
          || OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD == warning) { \
        res_vec_->set_##func_val(idx, value);                      \
      } else if (CM_IS_ZERO_ON_WARN(cast_mode)) {                 \
        res_vec_->set_##func_val(idx, zero_value);                 \
      } else {                                                    \
        res_vec_->set_null(idx);                                   \
      }                                                           \
    } else {                                                      \
      res_vec_->set_##func_val(idx, value);                        \
    }                                                             \
  } while (0)
#define SET_RES_INT(idx, value)         \
  SET_RES_OBJ(expr.extra_, int, 0, value, idx)
#define SET_RES_UINT(idx, value)        \
  SET_RES_OBJ(expr.extra_, uint,0, value, idx)
#define SET_RES_DATE(idx, value)        \
  SET_RES_OBJ(expr.extra_, date, ObTimeConverter::ZERO_DATE, value, idx)
#define SET_RES_DATETIME(idx, value)    \
  SET_RES_OBJ(expr.extra_, datetime,ObTimeConverter::ZERO_DATETIME, value, idx)

#define EVAL_COMMON_ARG()                                                                   \
  int ret = OB_SUCCESS;                                                                     \
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);                                  \
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));                  \
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));                            \
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;                                     \
  ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;                         \
  ObScale in_scale = expr.args_[0]->datum_meta_.scale_;                                     \
  ObPrecision in_prec = expr.args_[0]->datum_meta_.precision_;                              \
  ObObjType out_type = expr.datum_meta_.type_;                                              \
  ObCollationType out_cs_type = expr.datum_meta_.cs_type_;                                  \
  ObScale out_scale = expr.datum_meta_.scale_;                                              \
  ObPrecision out_prec = expr.datum_meta_.precision_;                                       \
  if (eval_flags.accumulate_bit_cnt(bound) == bound.range_size()) {                         \
  } else

#include "sql/engine/expr/vector_cast/cast_impl_helper.ipp"
#include "sql/engine/expr/vector_cast/decimal_int.ipp"
#include "sql/engine/expr/vector_cast/string_float.ipp"
#include "sql/engine/expr/vector_cast/cast_to_decimalint.ipp"
#include "sql/engine/expr/vector_cast/cast_to_int.ipp"
#include "sql/engine/expr/vector_cast/cast_to_float.ipp"
#include "sql/engine/expr/vector_cast/cast_to_date.ipp"
#include "sql/engine/expr/vector_cast/cast_to_number.ipp"
#include "sql/engine/expr/vector_cast/cast_to_datetime.ipp"
#include "sql/engine/expr/vector_cast/cast_to_string.ipp"

#undef DEF_VECTOR_IMPLICIT_CAST_FUNC
#undef CAST_FAIL
#undef EVAL_COMMON_ARG
#undef SET_RES_OBJ
#undef SET_RES_INT
#undef SET_RES_UINT
#endif // OCEANBASE_SQL_ENG_EXPR_VECTOR_CAST_H_
