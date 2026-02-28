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
#include "ob_expr_bit_neg.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

ObExprBitNeg::ObExprBitNeg(ObIAllocator &alloc)
    : ObBitwiseExprOperator(alloc, T_OP_BIT_NEG, N_BIT_NEG, 1, NOT_ROW_DIMENSION) {}

int ObExprBitNeg::calc_bitneg_expr(const ObExpr &expr, ObEvalCtx &ctx,
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
  } else if (OB_FAIL((reinterpret_cast<GetUIntFunc>(get_uint_func)(
                     expr.args_[0]->datum_meta_, *child_res, true, uint_val, cast_mode)))) {
    LOG_WARN("get uint from datum failed", K(ret), K(*child_res), K(cast_mode));
  } else {
    res_datum.set_uint(~uint_val);
  }
  return ret;
}


OB_DECLARE_AVX512_SPECIFIC_CODE(
int calc_bitneg_with_avx512(const char *group_inputs, char *group_results) {
    int ret = OB_SUCCESS;
    __m512i v = _mm512_loadu_si512((const __m512i*)group_inputs);
    _mm512_storeu_si512((__m512i*)group_results, ~v);
    return ret;
}
)

OB_DECLARE_AVX2_SPECIFIC_CODE(
int calc_bitneg_with_avx2(const char *group_inputs, char *group_results) {
    int ret = OB_SUCCESS;
    __m256i v = _mm256_loadu_si256((const __m256i*)group_inputs);
    _mm256_storeu_si256((__m256i*)group_results, ~v);
    return ret;
}
)

OB_DECLARE_SSE42_SPECIFIC_CODE(
int calc_bitneg_with_sse42(const char *group_inputs, char *group_results) {
    int ret = OB_SUCCESS;
    __m128i v = _mm_loadu_si128((const __m128i*)group_inputs);
    _mm_storeu_si128((__m128i*)group_results, ~v);
    return ret;
}
)


template<typename ArgVec, typename ResVec>
int ObExprBitNeg::vector_bitneg(VECTOR_EVAL_FUNC_ARG_DECL) {
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
  ObDatum temp_datum;
  // Init some required things
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
        res_vec->set_uint(i, ~uint_val);
      }
    }
  }

  return ret;
}

template<typename ArgVec, typename ResVec, bool Fixed>
int ObExprBitNeg::vector_bitneg_int_specific(VECTOR_EVAL_FUNC_ARG_DECL) {
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
  if constexpr (Fixed) {
    #if OB_USE_MULTITARGET_CODE
    int (*bit_neg_simd_func)(const char *, char *) = nullptr;
    bool use_simd = (!arg_vec->has_null() && bound.get_all_rows_active() && eval_flags.accumulate_bit_cnt(bound) == 0);
    if (common::is_arch_supported(ObTargetArch::AVX512)) {
      SIMD_BLOCKSIZE = sizeof(__m512i) / sizeof(uint64_t);
      bit_neg_simd_func = specific::avx512::calc_bitneg_with_avx512;
    } else if (common::is_arch_supported(ObTargetArch::AVX2)) {
      SIMD_BLOCKSIZE = sizeof(__m256i) / sizeof(uint64_t);
      bit_neg_simd_func = specific::avx2::calc_bitneg_with_avx2;
    } else if (common::is_arch_supported(ObTargetArch::SSE42)) {
      SIMD_BLOCKSIZE = sizeof(__m128i) / sizeof(uint64_t);
      bit_neg_simd_func = specific::sse42::calc_bitneg_with_sse42;
    }
    if (use_simd && OB_NOT_NULL(bit_neg_simd_func)) {
      group_cnt = RANGE_SIZE / SIMD_BLOCKSIZE;
      remaining = RANGE_SIZE % SIMD_BLOCKSIZE;
      for (uint64_t g = 0; g < group_cnt && OB_SUCC(ret); ++g) {
        uint64_t group_offset = bound.start() + g * SIMD_BLOCKSIZE;
        if (OB_FAIL((*bit_neg_simd_func)(arg_vec->get_payload(group_offset), const_cast<char *>(res_vec->get_payload(group_offset))))) {
          LOG_WARN("fail to calc bitneg with simd", K(ret), K(SIMD_BLOCKSIZE));
        }
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
          res_vec->set_uint(i, ~uint_val);
        }
      }
    }
  }
  return ret;
}

int ObExprBitNeg::calc_bitneg_expr_vector(VECTOR_EVAL_FUNC_ARG_DECL) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval bitneg param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    ObObjType calc_type = expr.args_[0]->datum_meta_.get_type();
    const int16_t calc_scale = expr.args_[0]->datum_meta_.scale_;
    bool can_use_optimization = is_int_specific(expr.args_[0]->datum_meta_);

    if (VEC_FIXED == arg_format && VEC_FIXED == res_format && can_use_optimization) { // for Fixed IntTC and decimal(p, 0), run no-cast+avx512 path
      ret = vector_bitneg_int_specific<UIntegerFixedVec, UIntegerFixedVec, true>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format && can_use_optimization) { // for non fixed IntTC, just run no-cast path
      ret = vector_bitneg_int_specific<UIntegerUniVec, UIntegerUniVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format && can_use_optimization) { // for non fixed IntTC, just run no-cast path
      ret = vector_bitneg_int_specific<UIntegerUniVec, UIntegerFixedVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_FIXED == arg_format && VEC_UNIFORM == res_format && can_use_optimization) { // for non fixed IntTC, just run no-cast path
      ret = vector_bitneg_int_specific<UIntegerFixedVec, UIntegerUniVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_DISCRETE == arg_format && VEC_DISCRETE == res_format) {
      ret = vector_bitneg<NumberDiscVec, NumberDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {
      ret = vector_bitneg<NumberUniVec, NumberDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_CONTINUOUS == arg_format && VEC_DISCRETE == res_format) {
      ret = vector_bitneg<NumberContVec, NumberDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    //...
    } else {
      ret = vector_bitneg<ObVectorBase, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
    }
  }
  return ret;
}

int ObExprBitNeg::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  const BitOperator op = BIT_NEG;
  if (OB_FAIL(cg_bitwise_expr(expr_cg_ctx, raw_expr, rt_expr, op))) {
    LOG_WARN("cg_bitwise_expr for bitneg failed", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = calc_bitneg_expr;
    rt_expr.eval_vector_func_ = calc_bitneg_expr_vector;
  }

  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprBitNeg, raw_expr) {
  int ret = OB_SUCCESS;
  if (is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_SQL_MODE);
  }
  return ret;
}
}//end of ns sql
}//end of ns oceanbase
