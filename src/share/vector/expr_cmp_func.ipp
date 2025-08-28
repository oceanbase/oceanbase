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

#ifndef _OCEANBASE_SHARE_VECTOR_EXPR_CMP_FUNC_IPP_
#define _OCEANBASE_SHARE_VECTOR_EXPR_CMP_FUNC_IPP_

#define USING_LOG_PREFIX SHARE

#include "expr_cmp_func.h"
#include "share/vector/vector_basic_op.h"
#include "share/datum/ob_datum_util.h"
#include "share/vector/ob_discrete_format.h"
#include "share/vector/ob_continuous_format.h"
#include "share/vector/ob_uniform_format.h"
#include "share/vector/ob_fixed_length_format.h"
#include "sql/engine/ob_serializable_function.h"
#include "sql/engine/expr/ob_array_expr_utils.h"

#define NULL_FIRST_IDX 0
#define NULL_LAST_IDX  1

namespace oceanbase
{
namespace common
{
using namespace sql;

extern NullSafeRowCmpFunc NULLSAFE_ROW_CMP_FUNCS[MAX_VEC_TC][MAX_VEC_TC][2];
extern RowCmpFunc ROW_CMP_FUNCS[MAX_VEC_TC][MAX_VEC_TC];
extern sql::ObExpr::EvalVectorFunc EVAL_VECTOR_EXPR_CMP_FUNCS[MAX_VEC_TC][MAX_VEC_TC][CO_MAX];

template<VecValueTypeClass l_tc, VecValueTypeClass r_tc, bool null_first>
struct NullSafeRowCmpImpl
{
  inline static int cmp(const ObObjMeta &l_meta, const ObObjMeta &r_meta,
                        const void *l_data, const ObLength l_len, const bool l_null,
                        const void *r_data, const ObLength r_len, const bool r_null,
                        int &cmp_ret)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(l_null) && OB_UNLIKELY(r_null)) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l_null)) {
      cmp_ret = null_first ? -1 : 1;
    } else if (OB_UNLIKELY(r_null)) {
      cmp_ret = null_first ? 1 : -1;
    } else {
      ret = VecTCCmpCalc<l_tc, r_tc>::cmp(l_meta, r_meta, l_data, l_len, r_data, r_len, cmp_ret);
    }
    return ret;
  }
};

template<int l_tc, int r_tc, bool defined>
struct InitCmpSet
{
  static void init_array()
  {
    return;
  }
};

template<int l_tc, int r_tc>
struct InitCmpSet<l_tc, r_tc, true>
{
  template <bool null_first>
  using NullSafeCmpFunc = NullSafeRowCmpImpl<static_cast<VecValueTypeClass>(l_tc),
                                     static_cast<VecValueTypeClass>(r_tc), null_first>;
  using CmpFunc = VecTCCmpCalc<static_cast<VecValueTypeClass>(l_tc),
                                     static_cast<VecValueTypeClass>(r_tc)>;

  static void init_array()
  {
    auto &nullsafe_cmp_funcs = NULLSAFE_ROW_CMP_FUNCS;
    auto &cmp_funcs = ROW_CMP_FUNCS;
    nullsafe_cmp_funcs[l_tc][r_tc][NULL_FIRST_IDX] =  NullSafeCmpFunc<true>::cmp;
    nullsafe_cmp_funcs[l_tc][r_tc][NULL_LAST_IDX] = NullSafeCmpFunc<false>::cmp;

    cmp_funcs[l_tc][r_tc] = CmpFunc::cmp;
  }
};

template <int X, int Y>
using nullsafe_cmp_initer = InitCmpSet<
  X, Y,
  VecTCCmpCalc<static_cast<VecValueTypeClass>(X), static_cast<VecValueTypeClass>(Y)>::defined_>;
// static bool g_init_cmp_set =
//   Ob2DArrayConstIniter<MAX_VEC_TC, MAX_VEC_TC, nullsafe_cmp_initer>::init();

// ===================== expr cmp functions =====================

struct ObNestedVectorCmpFunc
{
  template <typename LeftVector, typename RightVector>
  static int cmp(const LeftVector *l_vec, const RightVector *r_vec,
                 const int64_t idx, const ObExpr &expr, ObEvalCtx &ctx, int &cmp_ret)
  {
    int ret = OB_SUCCESS;
    cmp_ret = 0;
    ObString left = l_vec->get_string(idx);
    ObString right = r_vec->get_string(idx);
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
    const uint16_t left_meta_id = expr.args_[0]->obj_meta_.get_subschema_id();
    const uint16_t right_meta_id = expr.args_[1]->obj_meta_.get_subschema_id();
    const uint16_t res_meta_id = expr.obj_meta_.get_subschema_id();
    ObIArrayType *left_obj = NULL;
    ObIArrayType *right_obj = NULL;
    ObIArrayType *res_obj = NULL;
    ObString res_str;
    if (OB_FAIL(construct_param(tmp_allocator, ctx, left_meta_id, left, left_obj))) {
      SQL_ENG_LOG(WARN, "init nested obj failed", K(ret)); 
    } else if (OB_FAIL(construct_param(tmp_allocator, ctx, right_meta_id, right, right_obj))) {
      SQL_ENG_LOG(WARN, "init nested obj failed", K(ret));
    } else if (OB_FAIL(left_obj->compare(*right_obj, cmp_ret))) {
      SQL_ENG_LOG(WARN, "init nested obj failed", K(ret)); 
    } 
    return ret;
  }

  static int construct_param(ObIAllocator &alloc, ObEvalCtx &ctx, const uint16_t meta_id,
                              ObString &str_data, ObIArrayType *&param_obj)
  {
    return ObNestedVectorFunc::construct_param(alloc, ctx, meta_id, str_data, param_obj);
  }

  static int construct_res_obj(ObIAllocator &alloc, ObEvalCtx &ctx, const uint16_t meta_id, ObIArrayType *&res_obj)
  {
    return ObArrayExprUtils::construct_array_obj(alloc, ctx, meta_id, res_obj, false);
  }

  static int construct_params(ObIAllocator &alloc, ObEvalCtx &ctx, const uint16_t left_meta_id,
                                const uint16_t right_meta_id, const uint16_t res_meta_id, ObString &left, ObString right,
                                ObIArrayType *&left_obj, ObIArrayType *&right_obj, ObIArrayType *&res_obj)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ObArrayExprUtils::get_array_obj(alloc, ctx, left_meta_id, left, left_obj))) {
      SQL_ENG_LOG(WARN, "get array failed", K(ret));
    } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(alloc, ctx, right_meta_id, right, right_obj))) {
      SQL_ENG_LOG(WARN, "get array failed", K(ret));
    } else if (OB_FAIL(ObArrayExprUtils::construct_array_obj(alloc, ctx, res_meta_id, res_obj, false))) {
      SQL_ENG_LOG(WARN, "construct res array failed", K(ret));
    }
    return ret;
  }
};

template <typename LVec>
static int eval_right_operand(const ObExpr &cmp_expr, const ObExpr &left, const ObExpr &right,
                              ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  LVec *left_vec = static_cast<LVec *>(left.get_vector(ctx));
  ObBitVector &lskip = cmp_expr.get_pvt_skip(ctx);
  const ObBitVector *rskip = &skip;
  EvalBound pvt_bound = bound;
  if (left_vec->has_null()) {
    lskip.deep_copy(skip, bound.batch_size());
    for (int i = bound.start(); i < bound.end(); i++) {
      if (!lskip.at(i) && left_vec->is_null(i)) {
        lskip.set(i);
      }
    } // end for
    rskip = &lskip;
    pvt_bound.set_all_row_active(false);
  }
  // if rskip is all true, `eval_vector` still needs to be called, for that expr format may not be inited.
  if (OB_FAIL(right.eval_vector(ctx, *rskip, pvt_bound))) {
    LOG_WARN("eval right operand failed", K(ret));
  }
  return ret;
}

static int eval_cmp_operands(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                             const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  const ObExpr &left = *expr.args_[0];
  const ObExpr &right = *expr.args_[1];
  if (OB_FAIL(left.eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval left operand failed", K(ret));
  } else {
    VectorFormat left_format = left.get_format(ctx);
    switch (left_format) {
    case VEC_DISCRETE:
    case VEC_CONTINUOUS:
    case VEC_FIXED: {
      ret = eval_right_operand<ObBitmapNullVectorBase>(expr, left, right, ctx, skip, bound);
      break;
    }
    case VEC_UNIFORM: {
      ret = eval_right_operand<ObUniformFormat<false>>(expr, left, right, ctx, skip, bound);
      break;
    }
    case VEC_UNIFORM_CONST: {
      ret = eval_right_operand<ObUniformFormat<true>>(expr, left, right, ctx, skip, bound);
      break;
    }
    default: {
      ret = eval_right_operand<ObVectorBase>(expr, left, right, ctx, skip, bound);
    }
    }
  }
  return ret;
}

template<ObCmpOp cmp_op>
int get_cmp_ret(const int ret)
{
  if (cmp_op == CO_EQ) {
    return ret == 0;
  } else if (cmp_op == CO_LE) {
    return ret <= 0;
  } else if (cmp_op == CO_LT) {
    return ret < 0;
  } else if (cmp_op == CO_GE) {
    return ret >= 0;
  } else if (cmp_op == CO_GT) {
    return ret > 0;
  } else if (cmp_op == CO_NE) {
    return ret != 0;
  } else if (cmp_op == CO_CMP) {
    return ret;
  } else {
    return 0;
  }
}

// template <> int get_cmp_ret<CO_EQ> (const int ret) { return ret == 0; }
// template <> int get_cmp_ret<CO_LE> (const int ret) { return ret <= 0; }
// template <> int get_cmp_ret<CO_LT> (const int ret) { return ret < 0; }
// template <> int get_cmp_ret<CO_GE> (const int ret) { return ret >= 0; }
// template <> int get_cmp_ret<CO_GT> (const int ret) { return ret > 0; }
// template <> int get_cmp_ret<CO_NE> (const int ret) { return ret != 0; }
// template <> int get_cmp_ret<CO_CMP> (const int ret) { return ret; }

#define DO_VECTOR_CMP(LVec, RVec, ResVec)                                                          \
  do {                                                                                             \
    LVec *l_vector = static_cast<LVec *>(left.get_vector(ctx));                                    \
    RVec *r_vector = static_cast<RVec *>(right.get_vector(ctx));                                   \
    constexpr bool is_nested = (l_tc == VEC_TC_COLLECTION || r_tc == VEC_TC_COLLECTION);           \
    ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));                                 \
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);                                       \
    const char *l_payload = nullptr, *r_payload = nullptr;                                         \
    ObLength l_len = 0, r_len = 0;                                                                 \
    int cmp_ret = 0;                                                                               \
    if (!l_vector->has_null() && !r_vector->has_null()) {                                          \
      if (OB_LIKELY(bound.get_all_rows_active() && bound.is_full_size()                            \
                    && eval_flags.accumulate_bit_cnt(bound.batch_size()) == 0)) {                  \
        for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {                        \
          if (is_nested) {                                                                         \
            ret = ObNestedVectorCmpFunc::cmp(l_vector, r_vector, i, expr, ctx, cmp_ret);           \
          } else {                                                                                 \
            l_vector->get_payload(i, l_payload, l_len);                                            \
            r_vector->get_payload(i, r_payload, r_len);                                            \
            ret = VecTCCmpCalc<l_tc, r_tc>::cmp(left.obj_meta_, right.obj_meta_,                   \
                                                (const void *)l_payload, l_len,                    \
                                                (const void *)r_payload, r_len, cmp_ret);          \
          }                                                                                        \
          if (OB_FAIL(ret)) {                                                                      \
          } else {                                                                                 \
            res_vec->set_int(i, get_cmp_ret<cmp_op>(cmp_ret));                                     \
          }                                                                                        \
        }                                                                                          \
        if (OB_SUCC(ret)) { eval_flags.set_all(bound.batch_size()); }                              \
      } else {                                                                                     \
        for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {                        \
          if (skip.at(i) || eval_flags.at(i)) { continue; }                                        \
          if (is_nested) {                                                                         \
            ret = ObNestedVectorCmpFunc::cmp(l_vector, r_vector, i, expr, ctx, cmp_ret);           \
          } else {                                                                                 \
            l_vector->get_payload(i, l_payload, l_len);                                              \
            r_vector->get_payload(i, r_payload, r_len);                                              \
            ret = VecTCCmpCalc<l_tc, r_tc>::cmp(left.obj_meta_, right.obj_meta_,                     \
                                                (const void *)l_payload, l_len,                      \
                                                (const void *)r_payload, r_len, cmp_ret);            \
          }                                                                                        \
          if (OB_FAIL(ret)) {                                                                      \
          } else {                                                                                 \
            res_vec->set_int(i, get_cmp_ret<cmp_op>(cmp_ret));                                     \
            eval_flags.set(i);                                                                     \
          }                                                                                        \
        }                                                                                          \
      }                                                                                            \
    } else {                                                                                       \
      for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {                          \
        if (skip.at(i) || eval_flags.at(i)) { continue; }                                          \
        if (l_vector->is_null(i) || r_vector->is_null(i)) {                                        \
          res_vec->set_null(i);                                                                    \
          eval_flags.set(i);                                                                       \
        } else {                                                                                   \
          if (is_nested) {                                                                         \
            ret = ObNestedVectorCmpFunc::cmp(l_vector, r_vector, i, expr, ctx, cmp_ret);           \
          } else {                                                                                 \
            l_vector->get_payload(i, l_payload, l_len);                                            \
            r_vector->get_payload(i, r_payload, r_len);                                            \
            ret = VecTCCmpCalc<l_tc, r_tc>::cmp(left.obj_meta_, right.obj_meta_,                   \
                                                (const void *)l_payload, l_len,                    \
                                                (const void *)r_payload, r_len, cmp_ret);          \
          }                                                                                        \
          if (OB_FAIL(ret)) {                                                                      \
          } else {                                                                                 \
            res_vec->set_int(i, get_cmp_ret<cmp_op>(cmp_ret));                                     \
            eval_flags.set(i);                                                                     \
          }                                                                                        \
        }                                                                                          \
      }                                                                                            \
    }                                                                                              \
  } while (false)

#define CALC_FORMAT(l, r, res)                                                                     \
  ((int32_t)l + (((int32_t)r) << VEC_MAX_FORMAT) + (((int32_t)res) << (VEC_MAX_FORMAT * 2)))
template <VecValueTypeClass l_tc, VecValueTypeClass r_tc, ObCmpOp cmp_op>
struct EvalVectorCmp
{
#define VECTOR_CMP_CASE(l_fmt, r_fmt, res_fmt)                                                     \
  case CALC_FORMAT(l_fmt, r_fmt, res_fmt): {                                                       \
    DO_VECTOR_CMP(L_##l_fmt##_FMT, R_##r_fmt##_FMT, RES_##res_fmt##_FMT);                          \
  } break

  using L_VEC_FIXED_FMT =
    typename std::conditional<is_fixed_length_vec(l_tc), ObFixedLengthFormat<RTCType<l_tc>>,
                              ObVectorBase>::type;
  using R_VEC_FIXED_FMT =
    typename std::conditional<is_fixed_length_vec(r_tc), ObFixedLengthFormat<RTCType<r_tc>>,
                              ObVectorBase>::type;
  using RES_VEC_FIXED_FMT = ObFixedLengthFormat<int64_t>;
  // using RES_VEC_UNIFORM_FMT = ObUniformFormat<false>;
  using L_VEC_DISCRETE_FMT = ObDiscreteFormat;
  using R_VEC_DISCRETE_FMT = ObDiscreteFormat;
  using L_VEC_UNIFORM_FMT = ObUniformFormat<false>;
  using R_VEC_UNIFORM_FMT = ObUniformFormat<false>;
  using L_VEC_UNIFORM_CONST_FMT = ObUniformFormat<true>;
  using R_VEC_UNIFORM_CONST_FMT = ObUniformFormat<true>;
  static int eval_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                         const EvalBound &bound)
  {
    int ret = eval_cmp_operands(expr, ctx, skip, bound);

    if (OB_FAIL(ret)) {
      LOG_WARN("eval cmp operands failed", K(ret));
    } else {
      const ObExpr &left = *expr.args_[0];
      const ObExpr &right = *expr.args_[1];
      VectorFormat left_format = left.get_format(ctx);
      VectorFormat right_format = right.get_format(ctx);
      VectorFormat res_format = expr.get_format(ctx);
      LOG_DEBUG("eval vector cmp", K(expr), K(l_tc), K(r_tc), K(cmp_op), K(bound), K(left_format),
                K(right_format), K(res_format));
      if (is_valid_format(left_format) && is_valid_format(right_format) && is_valid_format(res_format)) {
        switch (CALC_FORMAT(left_format, right_format, res_format)) {
          VECTOR_CMP_CASE(VEC_DISCRETE, VEC_DISCRETE, VEC_FIXED);
          VECTOR_CMP_CASE(VEC_DISCRETE, VEC_UNIFORM, VEC_FIXED);
          VECTOR_CMP_CASE(VEC_DISCRETE, VEC_UNIFORM_CONST, VEC_FIXED);

          VECTOR_CMP_CASE(VEC_UNIFORM, VEC_DISCRETE, VEC_FIXED);
          VECTOR_CMP_CASE(VEC_UNIFORM, VEC_UNIFORM, VEC_FIXED);
          VECTOR_CMP_CASE(VEC_UNIFORM, VEC_UNIFORM_CONST, VEC_FIXED);

          VECTOR_CMP_CASE(VEC_UNIFORM_CONST, VEC_DISCRETE, VEC_FIXED);
          VECTOR_CMP_CASE(VEC_UNIFORM_CONST, VEC_UNIFORM, VEC_FIXED);
          default: {
            DO_VECTOR_CMP(ObVectorBase, ObVectorBase, ObVectorBase);
            break;
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid format", K(left_format), K(right_format), K(res_format));
      }
    }
    return ret;
  }

};

struct EvalVectorCmpWithNull
{
  static int eval_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                         const EvalBound &bound)
  {
    int ret = eval_cmp_operands(expr, ctx, skip, bound);
    if (OB_FAIL(ret)) {
      LOG_WARN("eval cmp operands failed", K(ret));
    } else {
      VectorFormat res_format = expr.get_format(ctx);
      ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
      if (OB_LIKELY(res_format == VEC_FIXED)) {
        ObBitmapNullVectorBase &res_nulls = *static_cast<ObBitmapNullVectorBase *>(expr.get_vector(ctx));
        if (OB_LIKELY(bound.get_all_rows_active() && eval_flags.accumulate_bit_cnt(bound) == 0)) {
          res_nulls.get_nulls()->set_all(bound.start(), bound.end());
          res_nulls.set_has_null();
          eval_flags.set_all(bound.start(), bound.end());
        } else {
          for (int i = bound.start(); i < bound.end(); i++) {
            if (skip.at(i) || eval_flags.at(i)) {
              continue;
            } else {
              res_nulls.set_null(i);
              eval_flags.set(i);
            }
          }
        }
      } else {
        ObIVector *res_vec = expr.get_vector(ctx);
        for (int i = bound.start(); i < bound.end(); i++) {
          if (skip.at(i) || eval_flags.at(i)) {
            continue;
          } else {
            res_vec->set_null(i);
            eval_flags.set(i);
          }
        }
      }
    }
    return ret;
  }
};

template<VecValueTypeClass l_tc, ObCmpOp cmp_op>
struct EvalVectorCmp<l_tc, VEC_TC_NULL, cmp_op>: public EvalVectorCmpWithNull {};

template<VecValueTypeClass r_tc, ObCmpOp cmp_op>
struct EvalVectorCmp<VEC_TC_NULL, r_tc, cmp_op>: public EvalVectorCmpWithNull {};

template<ObCmpOp cmp_op>
struct EvalVectorCmp<VEC_TC_NULL, VEC_TC_NULL, cmp_op>: public EvalVectorCmpWithNull {};
template<int X, int Y, bool defined>
struct VectorExprCmpFuncIniter
{
  static void init_array()
  {
    return;
  }
};

template<int X, int Y>
struct VectorExprCmpFuncIniter<X, Y, true>
{
  template <ObCmpOp cmp_op>
  using EvalFunc =
    EvalVectorCmp<static_cast<VecValueTypeClass>(X), static_cast<VecValueTypeClass>(Y), cmp_op>;
  static void init_array()
  {
    auto &funcs = EVAL_VECTOR_EXPR_CMP_FUNCS;
    funcs[X][Y][CO_LE] = &EvalFunc<CO_LE>::eval_vector;
    funcs[X][Y][CO_LT] = &EvalFunc<CO_LT>::eval_vector;
    funcs[X][Y][CO_GE] = &EvalFunc<CO_GE>::eval_vector;
    funcs[X][Y][CO_GT] = &EvalFunc<CO_GT>::eval_vector;
    funcs[X][Y][CO_NE] = &EvalFunc<CO_NE>::eval_vector;
    funcs[X][Y][CO_EQ] = &EvalFunc<CO_EQ>::eval_vector;
    funcs[X][Y][CO_CMP] = &EvalFunc<CO_CMP>::eval_vector;
  }
};

template <int X, int Y>
using cmp_initer = VectorExprCmpFuncIniter<
  X, Y,
  VecTCCmpCalc<static_cast<VecValueTypeClass>(X), static_cast<VecValueTypeClass>(Y)>::defined_
    && (!is_fixed_length_vec(static_cast<VecValueTypeClass>(X))
        || !is_fixed_length_vec(static_cast<VecValueTypeClass>(Y)))>;

} // end common
} // end oceanbase

#include "expr_cmp_func_simd.ipp"

namespace oceanbase
{
namespace common
{

static const int COMPILATION_UNIT = 8;

#define DEF_EXPR_CMP_FUNC_PART(unit_idx)                                                           \
  void __expr_cmp_func_compilation##unit_idx()                                                     \
  {                                                                                                \
    constexpr int unit_size =                                                                      \
      (MAX_VEC_TC / COMPILATION_UNIT + (MAX_VEC_TC % COMPILATION_UNIT == 0 ? 0 : 1));              \
    constexpr int start = unit_size * unit_idx < MAX_VEC_TC ? unit_size * unit_idx : MAX_VEC_TC;   \
    constexpr int end = start + unit_size >= MAX_VEC_TC ? MAX_VEC_TC : start + unit_size;          \
    Ob2DArrayConstIniter<end, MAX_VEC_TC, nullsafe_cmp_initer, start, 0>::init();                  \
    Ob2DArrayConstIniter<end, MAX_VEC_TC, cmp_initer, start, 0>::init();                           \
    Ob2DArrayConstIniter<end, MAX_VEC_TC, fixed_cmp_initer, start, 0>::init();                     \
  }
} // common
} // oceanbase

#endif // _OCEANBASE_SHARE_VECTOR_EXPR_CMP_FUNC_IPP_