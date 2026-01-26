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
#include "sql/engine/expr/ob_expr_bit_count.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

const uint8_t ObExprBitCount::char_to_num_bits[256] =
{
	0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
};

ObExprBitCount::ObExprBitCount(ObIAllocator &alloc)
: ObBitwiseExprOperator(alloc, T_FUN_SYS_BIT_COUNT, "bit_count", 1, NOT_ROW_DIMENSION)
{
}

ObExprBitCount::~ObExprBitCount()
{
}

int ObExprBitCount::calc_bitcount_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                       ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  uint64_t uint_val = 0;
  ObDatum *child_res = NULL;
  ObCastMode cast_mode = CM_NONE;
  void *get_uint_func = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObSQLMode sql_mode = 0;
  if (OB_UNLIKELY(1 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, child_res))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (child_res->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(choose_get_int_func(expr.args_[0]->datum_meta_, get_uint_func))) {
    LOG_WARN("choose_get_int_func failed", K(ret), K(expr.args_[0]->datum_meta_));
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (FALSE_IT(ObSQLUtils::get_default_cast_mode(false, 0,
                                      session->get_stmt_type(),
                                      session->is_ignore_stmt(),
                                      sql_mode, cast_mode))) {
  } else if (OB_FAIL((reinterpret_cast<GetUIntFunc>(get_uint_func)(expr.args_[0]->datum_meta_,
                                                                   *child_res, true,
                                                                   uint_val, cast_mode)))) {
    LOG_WARN("get uint64 failed", K(ret), K(*child_res));
  } else {
    uint64_t temp_result = 0;
    temp_result = calc_table_look_up(uint_val);
    res_datum.set_uint(temp_result);
  }
  return ret;
}



OB_DECLARE_AVX512_SPECIFIC_CODE(
void calc_bitcount_with_avx512_and_harley_seal(const char *group_inputs, char *group_results) {
    __m512i v = _mm512_loadu_si512((const __m512i*)group_inputs);

    const __m512i mask1 = _mm512_set1_epi64(0x5555555555555555);
    const __m512i mask2 = _mm512_set1_epi64(0x3333333333333333);
    const __m512i mask3 = _mm512_set1_epi64(0x0F0F0F0F0F0F0F0F);
    const __m512i mask4 = _mm512_set1_epi64(0x00FF00FF00FF00FF);
    const __m512i mask5 = _mm512_set1_epi64(0x0000FFFF0000FFFF);
    const __m512i mask6 = _mm512_set1_epi64(0x00000000FFFFFFFF);

    v = _mm512_sub_epi64(v, _mm512_and_epi64(_mm512_srli_epi64(v, 1), mask1));
    v = _mm512_add_epi64(_mm512_and_epi64(v, mask2),
                         _mm512_and_epi64(_mm512_srli_epi64(v, 2), mask2));
    v = _mm512_add_epi64(_mm512_and_epi64(v, mask3),
                         _mm512_and_epi64(_mm512_srli_epi64(v, 4), mask3));
    v = _mm512_add_epi64(_mm512_and_epi64(v, mask4),
                         _mm512_and_epi64(_mm512_srli_epi64(v, 8), mask4));
    v = _mm512_add_epi64(_mm512_and_epi64(v, mask5),
                         _mm512_and_epi64(_mm512_srli_epi64(v, 16), mask5));
    v = _mm512_add_epi64(_mm512_and_epi64(v, mask6),
                         _mm512_and_epi64(_mm512_srli_epi64(v, 32), mask6));

    _mm512_storeu_si512((__m512i*)group_results, v);
}

)

OB_DECLARE_AVX2_SPECIFIC_CODE(
void calc_bitcount_with_avx2_and_harley_seal(const char *group_inputs, char *group_results) {
    __m256i v = _mm256_loadu_si256((const __m256i*)group_inputs);

    const __m256i mask1 = _mm256_set1_epi64x(0x5555555555555555);
    const __m256i mask2 = _mm256_set1_epi64x(0x3333333333333333);
    const __m256i mask3 = _mm256_set1_epi64x(0x0F0F0F0F0F0F0F0F);
    const __m256i mask4 = _mm256_set1_epi64x(0x00FF00FF00FF00FF);
    const __m256i mask5 = _mm256_set1_epi64x(0x0000FFFF0000FFFF);
    const __m256i mask6 = _mm256_set1_epi64x(0x00000000FFFFFFFF);

    v = _mm256_sub_epi64(v, _mm256_and_si256(_mm256_srli_epi64(v, 1), mask1));
    v = _mm256_add_epi64(_mm256_and_si256(v, mask2),
                         _mm256_and_si256(_mm256_srli_epi64(v, 2), mask2));
    v = _mm256_add_epi64(_mm256_and_si256(v, mask3),
                         _mm256_and_si256(_mm256_srli_epi64(v, 4), mask3));
    v = _mm256_add_epi64(_mm256_and_si256(v, mask4),
                         _mm256_and_si256(_mm256_srli_epi64(v, 8), mask4));
    v = _mm256_add_epi64(_mm256_and_si256(v, mask5),
                         _mm256_and_si256(_mm256_srli_epi64(v, 16), mask5));
    v = _mm256_add_epi64(_mm256_and_si256(v, mask6),
                         _mm256_and_si256(_mm256_srli_epi64(v, 32), mask6));
    _mm256_storeu_si256((__m256i *)group_results, v);
}
)


template <typename ArgVec, typename ResVec>
int ObExprBitCount::vector_bitcount(VECTOR_EVAL_FUNC_ARG_DECL) {
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObCastMode cast_mode = CM_NONE;
  void *get_uint_func = nullptr;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObSQLMode sql_mode = 0;
  uint64_t uint_val;
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  ObDatum temp_datum;
  // Init some required things.
  if (OB_FAIL(choose_get_int_func(expr.args_[0]->datum_meta_, get_uint_func))) {
    LOG_WARN("choose_get_int_func failed", K(ret), K(expr.args_[0]->datum_meta_));
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (FALSE_IT(ObSQLUtils::get_default_cast_mode(false, 0,
                                      session->get_stmt_type(),
                                      session->is_ignore_stmt(),
                                      sql_mode, cast_mode))) {
    // do nothing
  }

  for (int i = bound.start(); i < bound.end() && OB_SUCC(ret); i++) {
    if (!(skip.at(i) || eval_flags.at(i))) {
      if (OB_UNLIKELY(arg_vec->is_null(i))) {
        res_vec->set_null(i);
      } else if (OB_FAIL((reinterpret_cast<GetUIntFunc>(get_uint_func)(expr.args_[0]->datum_meta_,
                                                                      ObDatum(arg_vec->get_payload(i), arg_vec->get_length(i), false), true,
                                                                      uint_val, cast_mode)))) {
        LOG_WARN("get uint64 failed", K(ret));
      } else {
        uint64_t temp_result;
        // After an easy test, __builtin_popcountll slightly faster than lookup table by 1.43x
        temp_result = __builtin_popcountll(uint_val);
        res_vec->set_uint(i, temp_result);
      }
    }
  }

  return ret;
}

template <typename ArgVec, typename ResVec, bool isFixed>
int ObExprBitCount::vector_bitcount_int_specific(VECTOR_EVAL_FUNC_ARG_DECL) {
  int ret = OB_SUCCESS;

  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  uint64_t uint_val = 0;
  size_t SIMD_BLOCKSIZE = 1;
  uint64_t RANGE_SIZE = bound.range_size();
  uint64_t group_cnt = 0;
  uint64_t remaining = 0;
  uint64_t remaining_offset = 0;
  if constexpr (isFixed) { // Only fixed vector can use SIMD
    #if OB_USE_MULTITARGET_CODE
    void (*bit_count_simd_func)(const char *, char *) = nullptr;
    bool use_simd = (!arg_vec->has_null() && bound.get_all_rows_active() && eval_flags.accumulate_bit_cnt(bound) == 0);
    if (common::is_arch_supported(ObTargetArch::AVX512)) {
      SIMD_BLOCKSIZE = sizeof(__m512i) / sizeof(uint64_t);
      bit_count_simd_func = specific::avx512::calc_bitcount_with_avx512_and_harley_seal;
    } else if (common::is_arch_supported(ObTargetArch::AVX2)) {
      SIMD_BLOCKSIZE = sizeof(__m256i) / sizeof(uint64_t);
      bit_count_simd_func = specific::avx2::calc_bitcount_with_avx2_and_harley_seal;
    }
    if (use_simd && OB_NOT_NULL(bit_count_simd_func)) {
      group_cnt = RANGE_SIZE / SIMD_BLOCKSIZE;
      remaining = RANGE_SIZE % SIMD_BLOCKSIZE;
      for (uint64_t g = 0; g < group_cnt && OB_SUCC(ret); ++g) {
        uint64_t group_offset = bound.start() + g * SIMD_BLOCKSIZE;
        (*bit_count_simd_func)(arg_vec->get_payload(group_offset), const_cast<char *>(res_vec->get_payload(group_offset)));
      }
      remaining_offset = group_cnt * SIMD_BLOCKSIZE;
    }
    #endif
  }
  if (OB_SUCC(ret)) {
    for (int i = bound.start() + remaining_offset; i < bound.end() && OB_SUCC(ret); i++) {
      if (OB_LIKELY(!(skip.at(i) || eval_flags.at(i)))) {
        if (OB_UNLIKELY(arg_vec->is_null(i))) {
          res_vec->set_null(i);
        } else {
          uint_val = arg_vec->get_uint(i);
          res_vec->set_uint(i, __builtin_popcountll(uint_val));
        }
      }
    }
  }
  return ret;
}



int ObExprBitCount::calc_bitcount_expr_vector(VECTOR_EVAL_FUNC_ARG_DECL) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval bitcount param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    bool can_use_optimization = is_int_specific(expr.args_[0]->datum_meta_);

    if (VEC_FIXED == arg_format && VEC_FIXED == res_format && can_use_optimization) { // for Fixed IntTC and decimal(p, 0), run no-cast+avx512 path
      ret = vector_bitcount_int_specific<UIntegerFixedVec, UIntegerFixedVec, true>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format && can_use_optimization) { // for non fixed IntTC, just run no-cast path
      ret = vector_bitcount_int_specific<UIntegerUniVec, UIntegerUniVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format && can_use_optimization) { // for non fixed IntTC, just run no-cast path
      ret = vector_bitcount_int_specific<UIntegerUniVec, UIntegerFixedVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_FIXED == arg_format && VEC_UNIFORM == res_format && can_use_optimization) { // for non fixed IntTC, just run no-cast path
      ret = vector_bitcount_int_specific<UIntegerFixedVec, UIntegerUniVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_DISCRETE == arg_format && VEC_DISCRETE == res_format) {
      ret = vector_bitcount<NumberDiscVec, NumberDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {
      ret = vector_bitcount<NumberUniVec, NumberDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_CONTINUOUS == arg_format && VEC_DISCRETE == res_format) {
      ret = vector_bitcount<NumberContVec, NumberDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    //...
    } else {
      ret = vector_bitcount<ObVectorBase, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
    }
  }
  return ret;
}

int ObExprBitCount::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  const BitOperator op = BIT_COUNT;
  if (OB_FAIL(cg_bitwise_expr(expr_cg_ctx, raw_expr, rt_expr, op))) {
    LOG_WARN("cg_bitwise_expr for bit count failed", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = ObExprBitCount::calc_bitcount_expr;
    rt_expr.eval_vector_func_ = ObExprBitCount::calc_bitcount_expr_vector;
  }

  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprBitCount, raw_expr) {
  int ret = OB_SUCCESS;
  if (is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_SQL_MODE);
  }
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
