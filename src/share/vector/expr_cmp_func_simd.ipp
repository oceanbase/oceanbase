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

#include "expr_cmp_func.h"
#include "share/vector/ob_fixed_length_format.h"
#include "share/datum/ob_datum_util.h"
#include "share/vector/ob_uniform_format.h"
#include "share/vector/vector_basic_op.h"
#include "common/ob_target_specific.h"
#if OB_USE_MULTITARGET_CODE
#include <emmintrin.h>
#include <immintrin.h>
#endif

namespace oceanbase
{
namespace common
{
using namespace sql;

OB_DECLARE_AVX512_SPECIFIC_CODE(
template <VecValueTypeClass vec_tc, int val_size, ObCmpOp cmp_op>
static int simd_eval_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                            const EvalBound &bound);
)

template<VecValueTypeClass l_tc, VecValueTypeClass r_tc, ObCmpOp cmp_op>
struct FixedVectorCmp
{
  using L_VEC_FIXED_FMT =
    typename std::conditional<is_fixed_length_vec(l_tc), ObFixedLengthFormat<RTCType<l_tc>>,
                              ObVectorBase>::type;
  using R_VEC_FIXED_FMT =
    typename std::conditional<is_fixed_length_vec(r_tc), ObFixedLengthFormat<RTCType<r_tc>>,
                              ObVectorBase>::type;
  using RES_VEC_FIXED_FMT = ObFixedLengthFormat<int64_t>;

  using L_VEC_UNIFORM_FMT = ObUniformFormat<false>;
  using R_VEC_UNIFORM_FMT = ObUniformFormat<false>;
  using L_VEC_UNIFORM_CONST_FMT = ObUniformFormat<true>;
  using R_VEC_UNIFORM_CONST_FMT = ObUniformFormat<true>;

  static int eval_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                         const EvalBound &bound)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(eval_cmp_operands(expr, ctx, skip, bound))) {
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
        switch(CALC_FORMAT(left_format, right_format, res_format)) {
          case CALC_FORMAT(VEC_FIXED, VEC_FIXED, VEC_FIXED): {
            bool use_simd = (l_tc == r_tc
                             && static_cast<ObFixedLengthBase *>(expr.get_vector(ctx))->get_length() == sizeof(int64_t)
                             && sizeof(RTCType<l_tc>) <= sizeof(int64_t)
                             && cmp_op != CO_CMP && !left.get_vector(ctx)->has_null()
                             && !right.get_vector(ctx)->has_null() & bound.get_all_rows_active())
                             && simd_supported(l_tc);
            LOG_DEBUG("simd used", K(l_tc), K(r_tc), K(cmp_op), K(left.get_vector(ctx)->has_null()),
                      K(right.get_vector(ctx)->has_null()), K(bound.get_all_rows_active()), K(use_simd));
#if OB_USE_MULTITARGET_CODE
            if (use_simd && common::is_arch_supported(ObTargetArch::AVX512)) {
              ret = common::specific::avx512::simd_eval_vector<l_tc, sizeof(RTCType<l_tc>), cmp_op>(
                      expr, ctx, skip, bound);
            } else {
              DO_VECTOR_CMP(L_VEC_FIXED_FMT, R_VEC_FIXED_FMT, RES_VEC_FIXED_FMT);
            }
#else
              DO_VECTOR_CMP(L_VEC_FIXED_FMT, R_VEC_FIXED_FMT, RES_VEC_FIXED_FMT);
#endif
            break;
          }
          VECTOR_CMP_CASE(VEC_UNIFORM, VEC_FIXED, VEC_FIXED);
          VECTOR_CMP_CASE(VEC_UNIFORM, VEC_UNIFORM, VEC_FIXED);
          VECTOR_CMP_CASE(VEC_UNIFORM, VEC_UNIFORM_CONST, VEC_FIXED);
          VECTOR_CMP_CASE(VEC_UNIFORM_CONST, VEC_FIXED, VEC_FIXED);
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
private:
  static constexpr bool simd_supported(VecValueTypeClass vec_tc)
  {
    return vec_tc == VEC_TC_INTEGER
           || vec_tc == VEC_TC_UINTEGER
           || vec_tc == VEC_TC_FLOAT
           || vec_tc == VEC_TC_DOUBLE
           || vec_tc == VEC_TC_DATE
           || vec_tc == VEC_TC_DATETIME
           || vec_tc == VEC_TC_TIME
           || vec_tc == VEC_TC_BIT
           || vec_tc == VEC_TC_ENUM_SET
           || vec_tc == VEC_TC_DEC_INT32
           || vec_tc == VEC_TC_YEAR
           || vec_tc == VEC_TC_INTERVAL_YM
           || vec_tc == VEC_TC_DEC_INT64;
  }
};


OB_DECLARE_AVX512_SPECIFIC_CODE(
template<VecValueTypeClass vec_tc, int val_size, ObCmpOp cmp_op>
struct __simd_cmp
{
  using ret_type = char;
  OB_INLINE char operator()(const char *left, const char *right)
  {
    return char();
  }
};

template<typename T>
OB_INLINE void __store_cmp_results(char *dst, const T & res_mask)
{
  static const uint64_t MASK = 0xFF;
  static const __m512i TRUE_VALUES = _mm512_set1_epi64(1);
  uint64_t res_bits = res_mask;
  for (int i = 0; i < sizeof(T); i++) {
    uint8_t store_bits = static_cast<uint8_t>(MASK & res_bits);
    res_bits >>= 8;
    __m512i store_v = _mm512_maskz_mov_epi64(store_bits, TRUE_VALUES);
    _mm512_storeu_epi64(dst, store_v);
    dst += 64;
  }
}
#define DEF_SIMD_INTEGER_OP(ret_size, val_size, bits, cmp_op, cmp_name)                            \
  template <>                                                                                      \
  struct __simd_cmp<VEC_TC_INTEGER, val_size, cmp_op>                                              \
  {                                                                                                \
    using ret_type = __mmask##ret_size;                                                            \
    OB_INLINE ret_type operator()(const char *left, const char *right)                             \
    {                                                                                              \
      __m512i left_v = _mm512_loadu_epi64(left);                                                   \
      __m512i right_v = _mm512_loadu_epi64(right);                                                 \
      __mmask##ret_size res_mask = _mm512_cmp##cmp_name##_epi##bits##_mask(left_v, right_v);       \
      return res_mask;                                                                             \
    }                                                                                              \
  };                                                                                               \
  template <>                                                                                      \
  struct __simd_cmp<VEC_TC_UINTEGER, val_size, cmp_op>                                             \
  {                                                                                                \
    using ret_type = __mmask##ret_size;                                                            \
    OB_INLINE ret_type operator()(const char *left, const char *right)                             \
    {                                                                                              \
      __m512i left_v = _mm512_loadu_epi64(left);                                                   \
      __m512i right_v = _mm512_loadu_epi64(right);                                                 \
      __mmask##ret_size res_mask = _mm512_cmp##cmp_name##_epu##bits##_mask(left_v, right_v);       \
      return res_mask;                                                                             \
    }                                                                                              \
  }

DEF_SIMD_INTEGER_OP(64, 1, 8, CO_LE, le);
DEF_SIMD_INTEGER_OP(32, 2, 16, CO_LE, le);
DEF_SIMD_INTEGER_OP(16, 4, 32, CO_LE, le);
DEF_SIMD_INTEGER_OP(8, 8, 64, CO_LE, le);

DEF_SIMD_INTEGER_OP(64, 1, 8, CO_LT, lt);
DEF_SIMD_INTEGER_OP(32, 2, 16, CO_LT, lt);
DEF_SIMD_INTEGER_OP(16, 4, 32, CO_LT, lt);
DEF_SIMD_INTEGER_OP(8, 8, 64, CO_LT, lt);

DEF_SIMD_INTEGER_OP(64, 1, 8, CO_EQ, eq);
DEF_SIMD_INTEGER_OP(32, 2, 16, CO_EQ, eq);
DEF_SIMD_INTEGER_OP(16, 4, 32, CO_EQ, eq);
DEF_SIMD_INTEGER_OP(8, 8, 64, CO_EQ, eq);

DEF_SIMD_INTEGER_OP(64, 1, 8, CO_NE, neq);
DEF_SIMD_INTEGER_OP(32, 2, 16, CO_NE, neq);
DEF_SIMD_INTEGER_OP(16, 4, 32, CO_NE, neq);
DEF_SIMD_INTEGER_OP(8, 8, 64, CO_NE, neq);

DEF_SIMD_INTEGER_OP(64, 1, 8, CO_GT, gt);
DEF_SIMD_INTEGER_OP(32, 2, 16, CO_GT, gt);
DEF_SIMD_INTEGER_OP(16, 4, 32, CO_GT, gt);
DEF_SIMD_INTEGER_OP(8, 8, 64, CO_GT, gt);

DEF_SIMD_INTEGER_OP(64, 1, 8, CO_GE, ge);
DEF_SIMD_INTEGER_OP(32, 2, 16, CO_GE, ge);
DEF_SIMD_INTEGER_OP(16, 4, 32, CO_GE, ge);
DEF_SIMD_INTEGER_OP(8, 8, 64, CO_GE, ge);

#define DEF_SIMD_FLOATING_OP(cmp_name, cmp_op)                                                     \
  template <>                                                                                      \
  struct __simd_cmp<VEC_TC_FLOAT, sizeof(float), cmp_op>                                           \
  {                                                                                                \
    using ret_type = __mmask16;                                                                    \
    OB_INLINE ret_type operator()(const char *left, const char *right)                             \
    {                                                                                              \
      __m512 left_v = _mm512_loadu_ps(left);                                                       \
      __m512 right_v = _mm512_loadu_ps(right);                                                     \
      __mmask16 res_mask = _mm512_cmp##cmp_name##_ps_mask(left_v, right_v);                        \
      if (cmp_op == CO_GE || cmp_op == CO_GT) { res_mask = ~res_mask; }                            \
      return res_mask;                                                                             \
    }                                                                                              \
  };                                                                                               \
  template <>                                                                                      \
  struct __simd_cmp<VEC_TC_DOUBLE, sizeof(double), cmp_op>                                         \
  {                                                                                                \
    using ret_type = __mmask8;                                                                     \
    OB_INLINE ret_type operator()(const char *left, const char *right)                             \
    {                                                                                              \
      __m512d left_v = _mm512_loadu_pd(left);                                                      \
      __m512d righ_v = _mm512_loadu_pd(right);                                                     \
      __mmask8 res_mask = _mm512_cmp##cmp_name##_pd_mask(left_v, righ_v);                          \
      if (cmp_op == CO_GE || cmp_op == CO_GT) { res_mask = ~res_mask; }                            \
      return res_mask;                                                                             \
    }                                                                                              \
  }

DEF_SIMD_FLOATING_OP(eq, CO_EQ);
DEF_SIMD_FLOATING_OP(le, CO_LE);
DEF_SIMD_FLOATING_OP(lt, CO_LT);
DEF_SIMD_FLOATING_OP(neq , CO_NE);
DEF_SIMD_FLOATING_OP(lt, CO_GE);
DEF_SIMD_FLOATING_OP(le, CO_GT);

template <VecValueTypeClass vec_tc, int val_size, ObCmpOp cmp_op>
static int simd_eval_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                            const EvalBound &bound)
{
  constexpr const VecValueTypeClass calc_tc =
    (vec_tc == VEC_TC_FLOAT || vec_tc == VEC_TC_DOUBLE) ?
      vec_tc :
      (std::is_signed<RTCType<vec_tc>>::value ? VEC_TC_INTEGER : VEC_TC_UINTEGER);
  using mask_type = typename __simd_cmp<calc_tc, val_size, cmp_op>::ret_type;
#define DO_SIMD_CMP(off)                                                                           \
  do {                                                                                             \
    mask_type res_mask = __simd_cmp<calc_tc, val_size, cmp_op>()(left_data + offset + off * 64,    \
                                                                 right_data + offset + off * 64);  \
    __store_cmp_results(res_data + off * res_off_perf_unit, res_mask);                             \
  } while (false)

  using ResVec = ObFixedLengthFormat<int64_t>;
  int ret = OB_SUCCESS;
  ObFixedLengthBase *left_vec = static_cast<ObFixedLengthBase *>(expr.args_[0]->get_vector(ctx));
  ObFixedLengthBase *right_vec = static_cast<ObFixedLengthBase *>(expr.args_[1]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  int64_t size = bound.range_size(), unit = 512 / CHAR_BIT;
  int64_t chunk = size * val_size;
  int64_t unit_cnt = chunk / unit, remain  = chunk % unit;
  const char *left_data = left_vec->get_data() + bound.start() * val_size;
  const char *right_data = right_vec->get_data() + bound.start() * val_size;
  char *res_data = res_vec->get_data() + bound.start() * sizeof(int64_t);
  int32_t res_off_perf_unit = unit / val_size * sizeof(int64_t) , batch_cnt = (unit / val_size) * 8;
  int64_t output_idx = bound.start();
  int32_t offset = 0;
  LOG_DEBUG("simd cmp", K(vec_tc), K(val_size), K(cmp_op), K(bound), K(unit_cnt));
  if (remain > 0) {
    int cmp_ret = 0;
    ObObjMeta obj_meta = expr.args_[0]->obj_meta_;
    for (int i = 0; i < remain / val_size; i++) {
      VecTCCmpCalc<vec_tc, vec_tc>::cmp(obj_meta, obj_meta, left_data + offset, val_size,
                                          right_data + offset, val_size,
                                          cmp_ret); // ignore ret code
      res_vec->set_int(output_idx, get_cmp_ret<cmp_op>(cmp_ret));
      output_idx += 1;
      offset += val_size;
      res_data = res_data + sizeof(int64_t);
    }
  }
  for (int i = 0; i < unit_cnt / 8; i++) {
    LST_DO_CODE(DO_SIMD_CMP, 0, 1, 2, 3, 4, 5, 6, 7);
    output_idx += batch_cnt;
    offset += unit * 8;
    res_data = res_data + res_off_perf_unit * 8;
  }
  switch (unit_cnt % 8) {
  case 7: {
    LST_DO_CODE(DO_SIMD_CMP, 0, 1, 2, 3, 4, 5, 6);
    break;
  }
  case 6: {
    LST_DO_CODE(DO_SIMD_CMP, 0, 1, 2, 3, 4, 5);
    break;
  }
  case 5: {
    LST_DO_CODE(DO_SIMD_CMP, 0, 1, 2, 3, 4);
    break;
  }
  case 4: {
    LST_DO_CODE(DO_SIMD_CMP, 0, 1, 2, 3);
    break;
  }
  case 3: {
    LST_DO_CODE(DO_SIMD_CMP, 0, 1, 2);
    break;
  }
  case 2: {
    LST_DO_CODE(DO_SIMD_CMP, 0, 1);
    break;
  }
  case 1: {
    LST_DO_CODE(DO_SIMD_CMP, 0);
    break;
  }
  default: {
    break;
  }
  }
  batch_cnt = (unit_cnt % 8) * (unit / val_size);
  output_idx += batch_cnt;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  eval_flags.set_all(bound.start(), bound.end());
  OB_ASSERT(output_idx == bound.end());
  return ret;
};
)



template<int X, int Y, bool defined>
struct FixedExprCmpFuncIniter
{
  static void init_array()
  {
    return;
  }
};

template<int X, int Y>
struct FixedExprCmpFuncIniter<X, Y, true>
{
  template <ObCmpOp cmp_op>
  using EvalFunc =
    FixedVectorCmp<static_cast<VecValueTypeClass>(X), static_cast<VecValueTypeClass>(Y), cmp_op>;
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
using fixed_cmp_initer = FixedExprCmpFuncIniter<
  X, Y,
  VecTCCmpCalc<static_cast<VecValueTypeClass>(X), static_cast<VecValueTypeClass>(Y)>::defined_
  && is_fixed_length_vec(static_cast<VecValueTypeClass>(X))
  && is_fixed_length_vec(static_cast<VecValueTypeClass>(Y))>;

static int g_init_fixed_eval_vector_cmp_funcs = Ob2DArrayConstIniter<MAX_VEC_TC, MAX_VEC_TC, fixed_cmp_initer>::init();

} // end common
} // end oceanbase