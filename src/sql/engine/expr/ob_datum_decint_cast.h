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

#ifndef _OB_DATUM_FAST_DECINT_CAST_H_
#define _OB_DATUM_FAST_DECINT_CAST_H_

#define USING_LOG_PREFIX SQL

namespace oceanbase
{
namespace sql
{

// ==================================================================================
// accuracy checker
template <ObObjTypeClass out_tc>
struct BatchAccuracyChecker
{
  static int check(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                   const int64_t batch_size)
  {
    return OB_SUCCESS;
  }
};

template <>
struct BatchAccuracyChecker<ObDecimalIntTC>
{
  static int check(const sql::ObExpr &expr, sql::ObEvalCtx &ctx, const ObBitVector &skip,
                   const int64_t batch_size)
  {
    int ret = OB_SUCCESS;
    int warn = ret;
    ObAccuracy out_acc;
    ObObjType out_type;
    ObDecimalIntBuilder *res_arr = nullptr;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &tmp_alloc = alloc_guard.get_allocator();
    if (OB_FAIL(get_accuracy_from_parse_node(expr, ctx, out_acc, out_type))) {
      LOG_WARN("get accuracy failed", K(ret));
    } else {
      ObDatumVector result_dv = expr.locate_expr_datumvector(ctx);
      ObDecimalIntBuilder res_val;
      for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
        if (skip.at(i)) {
          continue;
        } else if (result_dv.at(i)->is_null()) {
          // do nothing
        } else if (OB_FAIL(check_decimalint_accuracy(
                     expr.extra_, result_dv.at(i)->get_decimal_int(),
                     result_dv.at(i)->get_int_bytes(), out_acc.get_precision(), out_acc.get_scale(),
                     res_val, warn))) {
          LOG_WARN("check decimal int accuracy failed", K(ret));
        } else {
          result_dv.at(i)->set_decimal_int(res_val.get_decimal_int(),
                                           res_val.get_int_bytes());
        }
      } // end for
    }
    return ret;
  }
};

template <>
struct BatchAccuracyChecker<ObNumberTC>
{
  static int check(const sql::ObExpr &expr, sql::ObEvalCtx &ctx, const ObBitVector &skip,
                   const int64_t batch_size)
  {
    int ret = OB_SUCCESS;
    int warning = ret;
    ObAccuracy out_acc;
    ObObjType out_type;
    if (OB_FAIL(get_accuracy_from_parse_node(expr, ctx, out_acc, out_type))) {
      LOG_WARN("get accuracy failed", K(ret));
    } else {
      ObDatumVector result_dv = expr.locate_expr_datumvector(ctx);
      for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
        if (skip.at(i)) {
          continue;
        } else if (result_dv.at(i)->is_null()) {
          // do nothing
        } else {
          ObDatum in_datum = *result_dv.at(i);
          if (OB_FAIL(number_range_check_v2(expr.extra_, out_acc, out_type, in_datum,
                                            *result_dv.at(i), warning))) {
            LOG_WARN("number range check failed", K(ret));
          }
        }
      }
    }
    return ret;
  }
};

// ==================================================================================
// helper functions

template <ObObjTypeClass in_tc, ObObjTypeClass out_tc>
int ObBatchCast::explicit_batch_cast(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                     const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  batch_func_ batch_cast = implicit_batch_cast<in_tc, out_tc>;
  batch_func_ check_accuracy = BatchAccuracyChecker<out_tc>::check;
  if (OB_FAIL(batch_cast(expr, ctx, skip, batch_size))) {
    LOG_WARN("batch cast failed", K(ret));
  } else if (OB_FAIL(check_accuracy(expr, ctx, skip, batch_size))) {
    LOG_WARN("batch accuracy check failed", K(ret));
  }
  return ret;
}

template <typename in_type, typename out_type, bool is_scale_up>
void batch_implicit_scale(ObDatumVector &arg_dv, ObDatumVector &result_dv, unsigned scale,
                          int64_t batch_size, const ObBitVector &skip, ObBitVector &eval_flags)
{
  static_assert(wide::IsIntegral<out_type>::value, "");
  static_assert(wide::IsIntegral<in_type>::value, "");
  LOG_DEBUG("batch implicit scale", K(lbt()), K(batch_size), K(is_scale_up));
  out_type sf = get_scale_factor<out_type>(scale);
  auto scale_up_task = [&](int i) __attribute__((always_inline))
  {
    if (arg_dv.at(i)->is_null()) {
      result_dv.at(i)->set_null();
      return;
    }
    out_type res = *reinterpret_cast<const in_type *>(arg_dv.at(i)->get_decimal_int());
    res = res * sf;
    result_dv.at(i)->set_decimal_int(reinterpret_cast<const ObDecimalInt *>(&res),
                                     sizeof(out_type));
  };

  auto fast_scale_up_task = [&](int i) __attribute__((always_inline))
  {
    if (arg_dv.at(i)->is_null()) {
      result_dv.at(i)->set_null();
      return;
    }
    if (sizeof(in_type) == sizeof(out_type) && wide::SignedConcept<in_type>::value) {
      result_dv.at(i)->set_decimal_int(arg_dv.at(i)->get_decimal_int(), sizeof(in_type));
    } else {
      out_type res = *reinterpret_cast<const in_type *>(arg_dv.at(i)->get_decimal_int());
      result_dv.at(i)->set_decimal_int(reinterpret_cast<const ObDecimalInt *>(&res),
                                       sizeof(out_type));
    }
  };

  auto scale_down_task = [&](int i) __attribute__((always_inline))
  {
    if (arg_dv.at(i)->is_null()) {
      result_dv.at(i)->set_null();
      return;
    }
    out_type res = *reinterpret_cast<const in_type *>(arg_dv.at(i)->get_decimal_int());
    bool is_neg = res < 0;
    if (is_neg) { res = -res; }
    out_type remain = res % sf;
    res = res / sf;
    if (remain >= (sf >> 1)) { res = res + 1; }
    if (is_neg) { res = -res; }
    result_dv.at(i)->set_decimal_int(reinterpret_cast<const ObDecimalInt *>(&res),
                                     sizeof(out_type));
  };

  if (is_scale_up) {
    if (OB_LIKELY(skip.accumulate_bit_cnt(batch_size) == 0
                  && eval_flags.accumulate_bit_cnt(batch_size) == 0)) {
      if (sf == 1) {
        for (int i = 0; i < batch_size; i++) { fast_scale_up_task(i); }
      } else {
        for (int i = 0; i < batch_size; i++) { scale_up_task(i); }
      }
      eval_flags.set_all(batch_size);
    } else {
      for (int i = 0; i < batch_size; i++) {
        if (skip.at(i) || eval_flags.at(i)) {
          continue;
        } else {
          scale_up_task(i);
          eval_flags.set(i);
        }
      }
    }
  } else {
    if (OB_LIKELY(skip.accumulate_bit_cnt(batch_size) == 0
                  && eval_flags.accumulate_bit_cnt(batch_size) == 0)) {
      for (int i = 0; i < batch_size; i++) { scale_down_task(i); }
      eval_flags.set_all(batch_size);
    } else {
      for (int i = 0; i < batch_size; i++) {
        if (skip.at(i) || eval_flags.at(i)) {
          continue;
        } else {
          scale_down_task(i);
          eval_flags.set(i);
        }
      }
    }
  }
}

template <typename in_type, typename out_type, bool is_scale_up>
void batch_explicit_scale(ObDatumVector &arg_dv, ObDatumVector &result_dv, unsigned scale,
                          ObPrecision out_prec, int64_t batch_size, const ObBitVector &skip,
                          ObBitVector &eval_flags)
{
  static_assert(wide::IsIntegral<in_type>::value, "");
  static_assert(wide::IsIntegral<out_type>::value, "");
  using calc_type = typename wide::CommonType<in_type, out_type>::type;
  LOG_DEBUG("batch explicit scale", K(lbt()), K(batch_size), K(is_scale_up), K(out_prec));
  int ret = OB_SUCCESS;
  OB_ASSERT(out_prec <= OB_MAX_DECIMAL_POSSIBLE_PRECISION);
  const ObDecimalInt *min_v = wide::ObDecimalIntConstValue::get_min_lower(out_prec);
  const ObDecimalInt *max_v = wide::ObDecimalIntConstValue::get_max_upper(out_prec);
  calc_type sf = get_scale_factor<calc_type>(scale);

  auto scale_up_task = [&](int i) __attribute__((always_inline))
  {
    if (arg_dv.at(i)->is_null()) {
      result_dv.at(i)->set_null();
      return;
    }
    calc_type lhs = *reinterpret_cast<const in_type *>(arg_dv.at(i)->get_decimal_int());
    calc_type res;
    bool is_neg = lhs < 0;
    bool overflow = wide::mul_overflow(lhs, sf, res);
    if (overflow) {
      result_dv.at(i)->set_decimal_int((is_neg ? min_v : max_v), sizeof(out_type));
    } else if (res >= *reinterpret_cast<const out_type *>(max_v)) {
      result_dv.at(i)->set_decimal_int(max_v, sizeof(out_type));
    } else if (wide::SignedConcept<calc_type>::value
               && res <= *reinterpret_cast<const out_type *>(min_v)) {
      result_dv.at(i)->set_decimal_int(min_v, sizeof(out_type));
    } else {
      out_type tmp_res = static_cast<out_type>(res);
      result_dv.at(i)->set_decimal_int(reinterpret_cast<const ObDecimalInt *>(&tmp_res),
                                       sizeof(out_type));
    }
  };

  auto scale_down_task = [&](int i) __attribute__((always_inline))
  {
    if (arg_dv.at(i)->is_null()) {
      result_dv.at(i)->set_null();
      return;
    }
    calc_type res = *reinterpret_cast<const in_type *>(arg_dv.at(i)->get_decimal_int());
    bool is_neg = res < 0;
    if (is_neg) { res = -res; }
    calc_type remain = res % sf;
    res = res / sf;
    if (remain >= (sf >> 1)) { res = res + 1; }
    if (is_neg) { res = -res; }
    if (res >= *reinterpret_cast<const out_type *>(max_v)) {
      result_dv.at(i)->set_decimal_int(max_v, sizeof(out_type));
    } else if (wide::SignedConcept<calc_type>::value
               && res <= *reinterpret_cast<const out_type *>(min_v)) {
      result_dv.at(i)->set_decimal_int(min_v, sizeof(out_type));
    } else {
      out_type tmp_res = static_cast<out_type>(res);
      result_dv.at(i)->set_decimal_int(reinterpret_cast<const ObDecimalInt *>(&tmp_res),
                                       sizeof(out_type));
    }
  };

  if (is_scale_up) {
    if (OB_LIKELY(skip.accumulate_bit_cnt(batch_size) == 0
                  && eval_flags.accumulate_bit_cnt(batch_size) == 0)) {
      for (int i = 0; i < batch_size; i++) {
        scale_up_task(i);
      }
      eval_flags.set_all(batch_size);
    } else {
      for (int i = 0; i < batch_size; i++) {
        if (skip.at(i) || eval_flags.at(i)) {
          continue;
        } else {
          scale_up_task(i);
          eval_flags.set(i);
        }
      }
    }
  } else {
    if (OB_LIKELY(skip.accumulate_bit_cnt(batch_size) == 0
                  && eval_flags.accumulate_bit_cnt(batch_size) == 0)) {
      for (int i = 0; i < batch_size; i++) {
        scale_down_task(i);
      }
      eval_flags.set_all(batch_size);
    } else {
      for (int i = 0; i < batch_size; i++) {
        if (skip.at(i) || eval_flags.at(i)) {
          continue;
        } else {
          scale_down_task(i);
          eval_flags.set(i);
        }
      }
    }
  }
}

template <typename in_type, typename out_type, bool is_scale_up>
void batch_const_scale(ObDatumVector &arg_dv, ObDatumVector &result_dv, const int64_t batch_size,
                       const ObBitVector &skip, ObBitVector &eval_flags, unsigned scale,
                       const ObScale out_prec, const ObCastMode cast_mode)
{
  static_assert(wide::IsIntegral<in_type>::value, "");
  static_assert(wide::IsIntegral<out_type>::value, "");
  using calc_type = typename wide::CommonType<in_type, out_type>::type;

  OB_ASSERT(out_prec <= OB_MAX_DECIMAL_POSSIBLE_PRECISION);
  const ObDecimalInt *min_v = wide::ObDecimalIntConstValue::get_min_lower(out_prec);
  const ObDecimalInt *max_v = wide::ObDecimalIntConstValue::get_max_upper(out_prec);
  calc_type sf = get_scale_factor<calc_type>(scale);

  auto scale_up_task = [&](int i) __attribute__((always_inline))
  {
    if (arg_dv.at(i)->is_null()) {
      result_dv.at(i)->set_null();
      return;
    }
    calc_type lhs = *reinterpret_cast<const in_type *>(arg_dv.at(i)->get_decimal_int());
    calc_type res;
    bool is_neg = lhs < 0;
    bool overflow = wide::mul_overflow(lhs, sf, res);
    if (overflow) {
      result_dv.at(i)->set_decimal_int((is_neg ? min_v : max_v), sizeof(out_type));
    } else if (res >= *reinterpret_cast<const out_type *>(max_v)) {
      result_dv.at(i)->set_decimal_int(max_v, sizeof(out_type));
    } else if (wide::SignedConcept<calc_type>::value
               && res <= *reinterpret_cast<const out_type *>(min_v)) {
      result_dv.at(i)->set_decimal_int(min_v, sizeof(out_type));
    } else {
      out_type tmp_res = static_cast<out_type>(res);
      result_dv.at(i)->set_decimal_int(reinterpret_cast<const ObDecimalInt *>(&tmp_res),
                                       sizeof(out_type));
    }
  };

  auto scale_down_task = [&](int i) __attribute__((always_inline))
  {
    if (arg_dv.at(i)->is_null()) {
      result_dv.at(i)->set_null();
      return;
    }
    calc_type res = *reinterpret_cast<const in_type *>(arg_dv.at(i)->get_decimal_int());
    bool is_neg = res < 0;
    if (is_neg) { res = -res; }
    calc_type remain = res % sf;
    res = res / sf;
    if (is_neg) { res = -res; }
    if (remain != 0) {
      if ((cast_mode & CM_CONST_TO_DECIMAL_INT_UP) != 0 && !is_neg) { res = res + 1; }
      if ((cast_mode & CM_CONST_TO_DECIMAL_INT_DOWN) != 0 && is_neg) { res = res - 1; }
    }
    if ((cast_mode & CM_CONST_TO_DECIMAL_INT_EQ) != 0) {
      if (remain != 0) {
        result_dv.at(i)->set_decimal_int((is_neg ? min_v : max_v), sizeof(out_type));
        return;
      }
    }
    if (res >= *reinterpret_cast<const out_type *>(max_v)) {
      result_dv.at(i)->set_decimal_int(max_v, sizeof(out_type));
    } else if (wide::SignedConcept<calc_type>::value
               && res <= *reinterpret_cast<const out_type *>(min_v)) {
      result_dv.at(i)->set_decimal_int(min_v, sizeof(out_type));
    } else {
      out_type tmp_res = static_cast<out_type>(res);
      result_dv.at(i)->set_decimal_int(reinterpret_cast<const ObDecimalInt *>(&tmp_res),
                                       sizeof(out_type));
    }
  };

  LOG_DEBUG("batch const scale", K(lbt()), K(batch_size), K(is_scale_up), K(cast_mode));
  if (is_scale_up) {
    if (OB_LIKELY(skip.accumulate_bit_cnt(batch_size) == 0
        && eval_flags.accumulate_bit_cnt(batch_size) == 0)) {
      for (int i = 0; i < batch_size; i++) { scale_up_task(i); }
      eval_flags.set_all(batch_size);
    } else {
      for (int i = 0; i < batch_size; i++) {
        if (skip.at(i) || eval_flags.at(i)) {
          continue;
        } else {
          scale_up_task(i);
          eval_flags.set(i);
        }
      }
    }
  } else {
    if (OB_LIKELY(skip.accumulate_bit_cnt(batch_size) == 0
                  && eval_flags.accumulate_bit_cnt(batch_size) == 0)) {
      for (int i = 0; i < batch_size; i++) { scale_down_task(i); }
      eval_flags.set_all(batch_size);
    } else {
      for (int i = 0; i < batch_size; i++) {
        if (skip.at(i) || eval_flags.at(i)) {
          continue;
        } else {
          scale_down_task(i);
          eval_flags.set(i);
        }
      }
    }
  }
}

template <typename in_type, typename out_type>
inline static void scale_up_with_types(const ObDecimalInt *decint, unsigned scale,
                                       ObDecimalIntBuilder &res)
{
  static_assert(wide::IsIntegral<out_type>::value, "");
  static_assert(wide::IsIntegral<in_type>::value, "");
  out_type val = get_scale_factor<out_type>(scale);
  val = val * (*reinterpret_cast<const in_type *>(decint));
  res.from(val);
}

template <typename in_type, typename out_type>
inline static void scale_down_with_types(const ObDecimalInt *decint, unsigned scale,
                                         ObDecimalIntBuilder &res, bool &truncated)
{
  static_assert(wide::IsIntegral<out_type>::value, "");
  static_assert(wide::IsIntegral<in_type>::value, "");
  out_type val = *reinterpret_cast<const in_type *>(decint);
  out_type sf = get_scale_factor<out_type>(scale);
  bool is_neg = val < 0;
  if (is_neg) { val = -val; }
  out_type remain = val % sf;
  val = val / sf;
  if (remain >= (sf >> 1)) { val = val + 1; }
  truncated = (remain != 0);
  if (is_neg) { val = -val; }
  res.from(val);
}

// ==================================================================================
// batch cast to decimalint types

template <typename in_type, typename out_type, bool is_up_scale, bool is_explicit>

static int decimalint_fast_batch_cast(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                      const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("fast batch cast to decimal int", K(sizeof(in_type)), K(sizeof(out_type)),
            K(batch_size));
  EVAL_BATCH_ARGS()
  {
    DEF_BATCH_CAST_PARAMS;
    ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
    ObPrecision in_prec = expr.args_[0]->datum_meta_.precision_;
    ObScale out_scale = expr.datum_meta_.scale_;
    ObPrecision out_prec = expr.datum_meta_.precision_;
    if (skip.accumulate_bit_cnt(batch_size) == batch_size
        || eval_flags.accumulate_bit_cnt(batch_size) == batch_size) {
      // do nothing
    } else {
      if (is_up_scale) {
        batch_implicit_scale<in_type, out_type, true>(arg_dv, result_dv, out_scale - in_scale,
                                                      batch_size, skip, eval_flags);
      } else {
        batch_implicit_scale<in_type, out_type, false>(arg_dv, result_dv, in_scale - out_scale,
                                                       batch_size, skip, eval_flags);
      }
    }
  }
  if (is_explicit && OB_SUCC(ret)) {
    if (OB_FAIL(BatchAccuracyChecker<ObDecimalIntTC>::check(expr, ctx, skip, batch_size))) {
      LOG_WARN("batch check accuracy failed", K(ret));
    }
  }
  return ret;
}

template <typename in_type, typename out_type, bool is_up_scale, bool is_explicit>
static int decimalint_fast_cast(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  EVAL_ARG()
  {
    LOG_DEBUG("fast casting routing from decimalint to decimalint");
    ObDecimalIntBuilder res_val;
    ObScale out_scale = expr.datum_meta_.scale_;
    ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
    if (is_up_scale) {
      scale_up_with_types<in_type, out_type>(child_res->get_decimal_int(), out_scale - in_scale,
                                             res_val);
      res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
    } else {
      bool truncated = false;
      scale_down_with_types<in_type, out_type>(child_res->get_decimal_int(), in_scale - out_scale,
                                               res_val, truncated);
      res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
      if (lib::is_mysql_mode() && CM_IS_COLUMN_CONVERT(expr.extra_) & truncated) {
        log_user_warning_truncated(ctx.exec_ctx_.get_user_logging_ctx());
      }
    }
  }
  if (is_explicit && !res_datum.is_null() && OB_SUCC(ret)) {
    ObPrecision out_prec = expr.datum_meta_.precision_;
    ObScale out_scale = expr.datum_meta_.scale_;
    ObDecimalIntBuilder res_val;
    int warning = OB_SUCCESS;
    if (OB_FAIL(check_decimalint_accuracy(expr.extra_, res_datum.get_decimal_int(),
                                          res_datum.get_int_bytes(), out_prec, out_scale, res_val,
                                          warning))) {
      LOG_WARN("check decimal int accuracy failed", K(ret));
    } else {
      res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
    }
  }
  return ret;
}

template <typename in_type, typename out_type, typename int_type, bool is_explicit>
static int int_fast_batch_cast(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                               const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("fsat batch cast from int to decimal int", K(ret), K(sizeof(in_type)),
            K(sizeof(out_type)), K(batch_size));
  EVAL_BATCH_ARGS()
  {
    DEF_BATCH_CAST_PARAMS;
    ObScale in_scale = 0, out_scale = expr.datum_meta_.scale_;
    OB_ASSERT(out_scale >= 0); // scale must be positive for decimal int type
    out_type sf = get_scale_factor<out_type>(out_scale - in_scale);
    auto cast_task = [&](int i) __attribute__((always_inline))
    {
      if (arg_dv.at(i)->is_null()) {
        result_dv.at(i)->set_null();
        return;
      }
      out_type in_val =
        static_cast<out_type>(*(reinterpret_cast<const int_type *>(arg_dv.at(i)->ptr_)));
      in_val = in_val * sf;
      result_dv.at(i)->set_decimal_int(reinterpret_cast<const ObDecimalInt *>(&in_val),
                                       sizeof(out_type));
    };
    int64_t skip_sz = skip.accumulate_bit_cnt(batch_size);
    int64_t eval_sz = eval_flags.accumulate_bit_cnt(batch_size);
    if (skip_sz == batch_size
        || eval_sz == batch_size) {
      // do nothing
    } else {
      if (OB_LIKELY(skip_sz== 0 && eval_sz == 0)) {
        for (int i = 0; i < batch_size; i++) { cast_task(i); }
        eval_flags.set_all(batch_size);
      } else {
        for (int i = 0; i < batch_size; i++) {
          if (skip.at(i) || eval_flags.at(i)) {
            continue;
          } else {
            cast_task(i);
            eval_flags.set(i);
          }
        }
      }
    }
  }
  if (is_explicit && OB_SUCC(ret)) {
    if (OB_FAIL(BatchAccuracyChecker<ObDecimalIntTC>::check(expr, ctx, skip, batch_size))) {
      LOG_WARN("batch accuracy check failed", K(ret));
    }
  }
  return ret;
}
template <typename in_type, typename out_type, typename int_type, bool is_explicit>
static int int_fast_cast(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  LOG_DEBUG("fast casting routine from int to decimal int");
  EVAL_ARG()
  {
    ObScale out_scale = expr.datum_meta_.scale_;
    ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
    out_type res = get_scale_factor<out_type>(out_scale - in_scale);
    in_type rhs = *reinterpret_cast<const int_type *>(child_res->ptr_);
    res = res * rhs;
    res_datum.set_decimal_int(reinterpret_cast<const ObDecimalInt *>(&res), sizeof(out_type));
  }
  if (is_explicit && !res_datum.is_null() && OB_SUCC(ret)) {
    ObPrecision out_prec = expr.datum_meta_.precision_;
    ObScale out_scale = expr.datum_meta_.scale_;
    ObDecimalIntBuilder res_val;
    int warning = OB_SUCCESS;
    if (OB_FAIL(check_decimalint_accuracy(expr.extra_, res_datum.get_decimal_int(),
                                          res_datum.get_int_bytes(), out_prec, out_scale, res_val,
                                          warning))) {
      LOG_WARN("check decimal int accuracy failed", K(ret));
    } else {
      res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
    }
  }
  return ret;
}

// ==================================================================================
// common batch datum casting routines
// ==================================================================================

template <ObObjTypeClass in_tc, ObObjTypeClass out_tc>
int ObBatchCast::implicit_batch_cast(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                     const int64_t batch_size)
{
  return cast_eval_arg_batch(expr, ctx, skip, batch_size);
}

#define DO_EXPLICIT_CAST(in_type, out_type)                                                        \
  if (in_scale <= out_scale) {                                                                     \
    batch_explicit_scale<in_type, out_type, true>(arg_dv, result_dv, out_scale - in_scale,         \
                                                  out_prec, batch_size, skip, eval_flags);         \
  } else {                                                                                         \
    batch_explicit_scale<in_type, out_type, false>(arg_dv, result_dv, in_scale - out_scale,        \
                                                   out_prec, batch_size, skip, eval_flags);        \
  }

#define DO_IMPLICIT_CAST(in_type, out_type)                                                        \
  if (in_scale <= out_scale) {                                                                     \
    batch_implicit_scale<in_type, out_type, true>(arg_dv, result_dv, out_scale - in_scale,         \
                                                  batch_size, skip, eval_flags);                   \
  } else {                                                                                         \
    batch_implicit_scale<in_type, out_type, false>(arg_dv, result_dv, in_scale - out_scale,        \
                                                   batch_size, skip, eval_flags);                  \
  }

#define DO_CONST_CAST(in_type, out_type)                                                           \
  if (in_scale <= out_scale) {                                                                     \
    batch_const_scale<in_type, out_type, true>(arg_dv, result_dv, batch_size, skip, eval_flags,    \
                                               out_scale - in_scale, out_prec, expr.extra_);       \
  } else {                                                                                         \
    batch_const_scale<in_type, out_type, false>(arg_dv, result_dv, batch_size, skip, eval_flags,   \
                                                in_scale - out_scale, out_prec, expr.extra_);      \
  }

DEF_BATCH_CAST_FUNC(ObDecimalIntTC, ObDecimalIntTC)
{
  int ret = OB_SUCCESS;
  EVAL_BATCH_ARGS()
  {
    DEF_BATCH_CAST_PARAMS;
    LOG_DEBUG("common batch scale for decimal int to decimal int", K(batch_size));
    int64_t skip_sz = skip.accumulate_bit_cnt(batch_size);
    int64_t eval_sz = eval_flags.accumulate_bit_cnt(batch_size);
    if (skip_sz == batch_size || eval_sz == batch_size) {
      // do nothing
    } else {
      ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
      ObScale out_scale = expr.datum_meta_.scale_;
      ObPrecision in_prec = expr.args_[0]->datum_meta_.precision_;
      ObPrecision out_prec = expr.datum_meta_.precision_;
      if (ObDatumCast::need_scale_decimalint(in_scale, in_prec, out_scale, out_prec)) {
        int32_t in_width = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(in_prec);
        int32_t out_width = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
        if (CM_IS_CONST_TO_DECIMAL_INT(expr.extra_)) {
          DISPATCH_INOUT_WIDTH_TASK(in_width, out_width, DO_CONST_CAST);
        } else if (CM_IS_COLUMN_CONVERT(expr.extra_) || CM_IS_EXPLICIT_CAST(expr.extra_)) {
          DISPATCH_INOUT_WIDTH_TASK(in_width, out_width, DO_EXPLICIT_CAST);
        } else {
          OB_ASSERT(out_width >= in_width || CM_IS_BY_TRANSFORMER(expr.extra_));
          DISPATCH_INOUT_WIDTH_TASK(in_width, out_width, DO_IMPLICIT_CAST);
        }
      } else {
        if (skip_sz == 0 && eval_sz == 0) {
          for (int i = 0; i < batch_size; i++) {
            result_dv.at(i)->set_datum(*arg_dv.at(i));
          } // end for
          eval_flags.set_all(batch_size);
        } else {
          for (int i = 0; i < batch_size; i++) {
            if (skip.at(i) || eval_flags.at(i)) {
              continue;
            } else {
              result_dv.at(i)->set_datum(*arg_dv.at(i));
              eval_flags.set(i);
            }
          } // end for
        }
      }
    }
  }
  return ret;
}

DEF_BATCH_CAST_FUNC(ObIntTC, ObDecimalIntTC)
{
#define ASSIGN_INT(int_type)                                                                       \
  for (int i = 0; i < batch_size; i++) {                                                           \
    if (skip.at(i) || eval_flags.at(i)) {                                                          \
      continue;                                                                                    \
    } else if (arg_dv.at(i)->is_null()) {                                                          \
      result_dv.at(i)->set_null();                                                                 \
    } else {                                                                                       \
      int_type tmp_v = static_cast<int_type>(arg_dv.at(i)->get_int());                             \
      result_dv.at(i)->set_decimal_int(reinterpret_cast<const ObDecimalInt *>(&tmp_v),             \
                                       sizeof(int_type));                                          \
    }                                                                                              \
    eval_flags.set(i);                                                                             \
  }

  int ret = OB_SUCCESS;
  LOG_DEBUG("common batch scale for int to decimal int", K(batch_size));
  EVAL_BATCH_ARGS()
  {
    DEF_BATCH_CAST_PARAMS;
    if (skip.accumulate_bit_cnt(batch_size) == batch_size
        || eval_flags.accumulate_bit_cnt(batch_size) == batch_size) {
      // do nothing
    } else {
      ObScale in_scale = 0;
      ObScale in_prec =
        ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][expr.args_[0]->datum_meta_.type_]
          .get_precision();
      if (in_prec > MAX_PRECISION_DECIMAL_INT_64) { in_prec = MAX_PRECISION_DECIMAL_INT_64; }
      ObScale out_scale = expr.datum_meta_.scale_;
      ObPrecision out_prec = expr.datum_meta_.precision_;
      int32_t in_width = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(in_prec);
      int32_t out_width = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
      if (ObDatumCast::need_scale_decimalint(in_scale, in_prec, out_scale, out_prec)) {
        if (CM_IS_CONST_TO_DECIMAL_INT(expr.extra_)) {
          DISPATCH_INOUT_WIDTH_TASK(in_width, out_width, DO_CONST_CAST);
        } else if (CM_IS_EXPLICIT_CAST(expr.extra_) || CM_IS_COLUMN_CONVERT(expr.extra_)) {
          DISPATCH_INOUT_WIDTH_TASK(in_width, out_width, DO_EXPLICIT_CAST);
        } else {
          OB_ASSERT(out_width >= in_width || (in_prec > 0 && in_prec <= out_prec));
          DISPATCH_INOUT_WIDTH_TASK(in_width, out_width, DO_IMPLICIT_CAST);
        }
      } else {
        DISPATCH_WIDTH_TASK(out_width, ASSIGN_INT);
      }
    }
  }
  return ret;
#undef ASSIGN_INT
}

DEF_BATCH_CAST_FUNC(ObUIntTC, ObDecimalIntTC)
{
#define ASSIGN_UINT(int_type)                                                                      \
  for (int i = 0; i < batch_size; i++) {                                                           \
    if (skip.at(i) || eval_flags.at(i)) {                                                          \
      continue;                                                                                    \
    } else if (arg_dv.at(i)->is_null()) {                                                          \
      result_dv.at(i)->set_null();                                                                 \
    } else {                                                                                       \
      int_type tmp_v = static_cast<int_type>(arg_dv.at(i)->get_uint64());                          \
      result_dv.at(i)->set_decimal_int(reinterpret_cast<const ObDecimalInt *>(&tmp_v),             \
                                       sizeof(int_type));                                          \
    }                                                                                              \
    eval_flags.set(i);                                                                             \
  }

#define CONST_CAST_UINT(int_type) DO_CONST_CAST(uint64_t, int_type)
#define IMPLICIT_CAST_UINT(int_type) DO_IMPLICIT_CAST(uint64_t, int_type)
#define EXPLICIT_CAST_UINT(int_type) DO_EXPLICIT_CAST(uint64_t, int_type)

  int ret = OB_SUCCESS;
  LOG_DEBUG("common batch scale for uint to decimal int", K(batch_size));
  EVAL_BATCH_ARGS()
  {
    DEF_BATCH_CAST_PARAMS;
    if (skip.accumulate_bit_cnt(batch_size) == batch_size
        || eval_flags.accumulate_bit_cnt(batch_size) == batch_size) {
      // do nothing
    } else {
      ObScale in_scale = 0;
      ObScale in_prec =
        ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][expr.args_[0]->datum_meta_.type_]
          .get_precision();
      ObScale out_scale = expr.datum_meta_.scale_;
      ObPrecision out_prec = expr.datum_meta_.precision_;
      int32_t in_width = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(in_prec);
      int32_t out_width = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
      if (ObDatumCast::need_scale_decimalint(in_scale, in_prec, out_scale, out_prec)) {
        if (CM_IS_CONST_TO_DECIMAL_INT(expr.extra_)) {
          DISPATCH_WIDTH_TASK(out_width, CONST_CAST_UINT);
        } else if (CM_IS_EXPLICIT_CAST(expr.extra_) || CM_IS_COLUMN_CONVERT(expr.extra_)) {
          DISPATCH_WIDTH_TASK(out_width, EXPLICIT_CAST_UINT);
        } else {
          OB_ASSERT(out_width >= in_width || (in_prec > 0 && in_prec <= out_prec));
          DISPATCH_WIDTH_TASK(out_width, IMPLICIT_CAST_UINT);
        }
      } else {
        DISPATCH_WIDTH_TASK(out_width, ASSIGN_UINT);
      }
    }
  }
  return ret;
#undef ASSIGN_UINT
#undef CONST_CAST_UINT
#undef IMPLICIT_CAST_UINT
#undef EXPLICIT_CAST_UINT
}

// ==================================================================================
// helper function to get explicit/implicit cast functions
ObBatchCast::batch_func_ ObBatchCast::get_explicit_cast_func(const ObObjTypeClass tc1,
                                                             const ObObjTypeClass tc2)
{
  OB_ASSERT(tc1 < ObMaxTC && tc2 < ObMaxTC);
  ObExpr::EvalFunc **func_arr = (ObExpr::EvalFunc **)OB_DATUM_CAST_ORACLE_EXPLICIT;
  if (lib::is_mysql_mode()) { func_arr = (ObExpr::EvalFunc **)OB_DATUM_CAST_MYSQL_IMPLICIT; }
  if (func_arr[tc1][tc2] == cast_not_expected || func_arr[tc1][tc2] == cast_not_support
      || func_arr[tc1][tc2] == cast_inconsistent_types
      || func_arr[tc1][tc2] == cast_inconsistent_types_json) {
    return cast_eval_arg_batch;
  } else {
    return explicit_batch_funcs_[tc1][tc2];
  }
}

ObBatchCast::batch_func_ ObBatchCast::get_implicit_cast_func(const ObObjTypeClass tc1,
                                                             const ObObjTypeClass tc2)
{
  OB_ASSERT(tc1 < ObMaxTC && tc2 < ObMaxTC);
  ObExpr::EvalFunc **func_arr = (ObExpr::EvalFunc **)OB_DATUM_CAST_ORACLE_IMPLICIT;
  if (lib::is_mysql_mode()) { func_arr = (ObExpr::EvalFunc **)OB_DATUM_CAST_MYSQL_IMPLICIT; }
  if (func_arr[tc1][tc2] == cast_not_expected || func_arr[tc1][tc2] == cast_not_support
      || func_arr[tc1][tc2] == cast_inconsistent_types
      || func_arr[tc1][tc2] == cast_inconsistent_types_json) {
    return cast_eval_arg_batch;
  } else {
    return implicit_batch_funcs_[tc1][tc2];
  }
}

// init cast batch funcs
ObBatchCast::batch_func_ ObBatchCast::implicit_batch_funcs_[ObMaxTC][ObMaxTC];
ObBatchCast::batch_func_ ObBatchCast::explicit_batch_funcs_[ObMaxTC][ObMaxTC];

template <ObObjTypeClass tc1, ObObjTypeClass tc2>
struct init_batch_func
{
  static int init()
  {
    if (!DefinedBatchCast<tc1, tc2>::value_) {
      ObBatchCast::explicit_batch_funcs_[tc1][tc2] = cast_eval_arg_batch;
      ObBatchCast::implicit_batch_funcs_[tc1][tc2] = cast_eval_arg_batch;
    } else {
      ObBatchCast::explicit_batch_funcs_[tc1][tc2] = ObBatchCast::explicit_batch_cast<tc1, tc2>;
      ObBatchCast::implicit_batch_funcs_[tc1][tc2] = ObBatchCast::implicit_batch_cast<tc1, tc2>;
    }
    if (tc2 >= ObMaxTC - 1) {
      return init_batch_func<static_cast<ObObjTypeClass>(tc1 + 1), ObNullTC>::init();
    } else {
      return init_batch_func<tc1, static_cast<ObObjTypeClass>(tc2 + 1)>::init();
    }
  }
};

template <ObObjTypeClass tc1>
struct init_batch_func<tc1, ObMaxTC>
{
  static int init()
  {
    return 0;
  }
};

template <ObObjTypeClass tc2>
struct init_batch_func<ObMaxTC, tc2>
{
  static int init()
  {
    return 0;
  }
};

static int init_batch_cast_funcs_ret = init_batch_func<ObNullTC, ObNullTC>::init();

// ==================================================================================
// get_decint_cast
void ObDatumCast::get_decint_cast(ObObjTypeClass in_tc, ObPrecision in_prec, ObScale in_scale,
                                  ObPrecision out_prec, ObScale out_scale, bool is_explicit,
                                  ObExpr::EvalBatchFunc &batch_cast_func,
                                  ObExpr::EvalFunc &cast_func)
{
#define SET_FUNC_PTR_DECINT(in_type, out_type)                                                     \
  if (in_scale <= out_scale) {                                                                     \
    if (is_explicit) {                                                                             \
      cast_func = decimalint_fast_cast<in_type, out_type, true, true>;                             \
      batch_cast_func = decimalint_fast_batch_cast<in_type, out_type, true, true>;                 \
    } else {                                                                                       \
      cast_func = decimalint_fast_cast<in_type, out_type, true, false>;                            \
      batch_cast_func = decimalint_fast_batch_cast<in_type, out_type, true, false>;                \
    }                                                                                              \
  } else {                                                                                         \
    if (is_explicit) {                                                                             \
      cast_func = decimalint_fast_cast<in_type, out_type, false, true>;                            \
      batch_cast_func = decimalint_fast_batch_cast<in_type, out_type, false, true>;                \
    } else {                                                                                       \
      cast_func = decimalint_fast_cast<in_type, out_type, false, false>;                           \
      batch_cast_func = decimalint_fast_batch_cast<in_type, out_type, false, false>;               \
    }                                                                                              \
  }

#define SET_FUNC_PTR_INT(int_type, in_type, out_type)                                              \
  if (is_explicit) {                                                                               \
    cast_func = int_fast_cast<in_type, out_type, int_type, true>;                                  \
    batch_cast_func = int_fast_batch_cast<in_type, out_type, int_type, true>;                      \
  } else {                                                                                         \
    cast_func = int_fast_cast<in_type, out_type, int_type, false>;                                 \
    batch_cast_func = int_fast_batch_cast<in_type, out_type, int_type, false>;                     \
  }

#define SET_FUNC_PTR(in_type, out_type)                                                            \
  if (in_tc == ObIntTC) {                                                                          \
    SET_FUNC_PTR_INT(int64_t, in_type, out_type);                                                  \
  } else if (in_tc == ObUIntTC) {                                                                  \
    SET_FUNC_PTR_INT(uint64_t, in_type, out_type);                                                 \
  } else if (in_tc == ObDecimalIntTC) {                                                            \
    SET_FUNC_PTR_DECINT(in_type, out_type);                                                        \
  }

#define CASE_DEF(size, size0)                                                                      \
  case common::DECIMAL_INT_##size0: {                                                              \
    SET_FUNC_PTR(int##size##_t, int##size0##_t);                                                   \
    break;                                                                                         \
  }

  int32_t in_type = get_decimalint_type(in_prec);
  int32_t out_type = get_decimalint_type(out_prec);
  batch_cast_func = nullptr;
  cast_func = nullptr;
  switch (in_type) {
  case common::DECIMAL_INT_32: {
    switch (out_type) {
      CASE_DEF(32, 32);
      CASE_DEF(32, 64);
      CASE_DEF(32, 128);
      CASE_DEF(32, 256);
      CASE_DEF(32, 512);
    }
    break;
  }
  case common::DECIMAL_INT_64: {
    switch (out_type) {
      CASE_DEF(64, 64);
      CASE_DEF(64, 128);
      CASE_DEF(64, 256);
      CASE_DEF(64, 512);
    }
    break;
  }
  case common::DECIMAL_INT_128: {
    switch (out_type) {
      CASE_DEF(128, 128);
      CASE_DEF(128, 256);
      CASE_DEF(128, 512);
    }
    break;
  }
  case common::DECIMAL_INT_256: {
    switch (out_type) {
      CASE_DEF(256, 256);
      CASE_DEF(256, 512);
    }
    break;
  }
  case common::DECIMAL_INT_512: {
    switch (out_type) {
      CASE_DEF(512, 512);
    }
    break;
  }
  }
  OB_ASSERT(batch_cast_func != nullptr);
  OB_ASSERT(cast_func != nullptr);
#undef CASE_DEF
#undef SET_FUNC_PTR
#undef SET_FUNC_PTR_TC
}

// ==================================================================================
// register functions
static ObExpr::EvalFunc g_decimalint_cast_functions[] = {
  int_fast_cast<int32_t, int32_t, int64_t, true>,
  int_fast_cast<int32_t, int64_t, int64_t, true>,
  int_fast_cast<int32_t, int128_t, int64_t, true>,
  int_fast_cast<int32_t, int256_t, int64_t, true>,
  int_fast_cast<int32_t, int512_t, int64_t, true>,
  int_fast_cast<int64_t, int64_t, int64_t, true>,
  int_fast_cast<int64_t, int128_t, int64_t, true>,
  int_fast_cast<int64_t, int256_t, int64_t, true>,
  int_fast_cast<int64_t, int512_t, int64_t, true>,
  int_fast_cast<int128_t, int128_t, int64_t, true>,
  int_fast_cast<int128_t, int256_t, int64_t, true>,
  int_fast_cast<int128_t, int512_t, int64_t, true>,

  int_fast_cast<int32_t, int32_t, uint64_t, true>,
  int_fast_cast<int32_t, int64_t, uint64_t, true>,
  int_fast_cast<int32_t, int128_t, uint64_t, true>,
  int_fast_cast<int32_t, int256_t, uint64_t, true>,
  int_fast_cast<int32_t, int512_t, uint64_t, true>,
  int_fast_cast<int64_t, int64_t, uint64_t, true>,
  int_fast_cast<int64_t, int128_t, uint64_t, true>,
  int_fast_cast<int64_t, int256_t, uint64_t, true>,
  int_fast_cast<int64_t, int512_t, uint64_t, true>,
  int_fast_cast<int128_t, int128_t, uint64_t, true>,
  int_fast_cast<int128_t, int256_t, uint64_t, true>,
  int_fast_cast<int128_t, int512_t, uint64_t, true>,

  int_fast_cast<int32_t, int32_t, int64_t, false>,
  int_fast_cast<int32_t, int64_t, int64_t, false>,
  int_fast_cast<int32_t, int128_t, int64_t, false>,
  int_fast_cast<int32_t, int256_t, int64_t, false>,
  int_fast_cast<int32_t, int512_t, int64_t, false>,
  int_fast_cast<int64_t, int64_t, int64_t, false>,
  int_fast_cast<int64_t, int128_t, int64_t, false>,
  int_fast_cast<int64_t, int256_t, int64_t, false>,
  int_fast_cast<int64_t, int512_t, int64_t, false>,
  int_fast_cast<int128_t, int128_t, int64_t, false>,
  int_fast_cast<int128_t, int256_t, int64_t, false>,
  int_fast_cast<int128_t, int512_t, int64_t, false>,

  int_fast_cast<int32_t, int32_t, uint64_t, false>,
  int_fast_cast<int32_t, int64_t, uint64_t, false>,
  int_fast_cast<int32_t, int128_t, uint64_t, false>,
  int_fast_cast<int32_t, int256_t, uint64_t, false>,
  int_fast_cast<int32_t, int512_t, uint64_t, false>,
  int_fast_cast<int64_t, int64_t, uint64_t, false>,
  int_fast_cast<int64_t, int128_t, uint64_t, false>,
  int_fast_cast<int64_t, int256_t, uint64_t, false>,
  int_fast_cast<int64_t, int512_t, uint64_t, false>,
  int_fast_cast<int128_t, int128_t, uint64_t, false>,
  int_fast_cast<int128_t, int256_t, uint64_t, false>,
  int_fast_cast<int128_t, int512_t, uint64_t, false>,

  decimalint_fast_cast<int32_t, int32_t, true, true>,
  decimalint_fast_cast<int32_t, int64_t, true, true>,
  decimalint_fast_cast<int32_t, int128_t, true, true>,
  decimalint_fast_cast<int32_t, int256_t, true, true>,
  decimalint_fast_cast<int32_t, int512_t, true, true>,
  decimalint_fast_cast<int64_t, int64_t, true, true>,
  decimalint_fast_cast<int64_t, int128_t, true, true>,
  decimalint_fast_cast<int64_t, int256_t, true, true>,
  decimalint_fast_cast<int64_t, int512_t, true, true>,
  decimalint_fast_cast<int128_t, int128_t, true, true>,
  decimalint_fast_cast<int128_t, int256_t, true, true>,
  decimalint_fast_cast<int128_t, int512_t, true, true>,
  decimalint_fast_cast<int256_t, int256_t, true, true>,
  decimalint_fast_cast<int256_t, int512_t, true, true>,
  decimalint_fast_cast<int512_t, int512_t, true, true>,

  decimalint_fast_cast<int32_t, int32_t, true, false>,
  decimalint_fast_cast<int32_t, int64_t, true, false>,
  decimalint_fast_cast<int32_t, int128_t, true, false>,
  decimalint_fast_cast<int32_t, int256_t, true, false>,
  decimalint_fast_cast<int32_t, int512_t, true, false>,
  decimalint_fast_cast<int64_t, int64_t, true, false>,
  decimalint_fast_cast<int64_t, int128_t, true, false>,
  decimalint_fast_cast<int64_t, int256_t, true, false>,
  decimalint_fast_cast<int64_t, int512_t, true, false>,
  decimalint_fast_cast<int128_t, int128_t, true, false>,
  decimalint_fast_cast<int128_t, int256_t, true, false>,
  decimalint_fast_cast<int128_t, int512_t, true, false>,
  decimalint_fast_cast<int256_t, int256_t, true, false>,
  decimalint_fast_cast<int256_t, int512_t, true, false>,
  decimalint_fast_cast<int512_t, int512_t, true, false>,

  decimalint_fast_cast<int32_t, int32_t, false, true>,
  decimalint_fast_cast<int32_t, int64_t, false, true>,
  decimalint_fast_cast<int32_t, int128_t, false, true>,
  decimalint_fast_cast<int32_t, int256_t, false, true>,
  decimalint_fast_cast<int32_t, int512_t, false, true>,
  decimalint_fast_cast<int64_t, int64_t, false, true>,
  decimalint_fast_cast<int64_t, int128_t, false, true>,
  decimalint_fast_cast<int64_t, int256_t, false, true>,
  decimalint_fast_cast<int64_t, int512_t, false, true>,
  decimalint_fast_cast<int128_t, int128_t, false, true>,
  decimalint_fast_cast<int128_t, int256_t, false, true>,
  decimalint_fast_cast<int128_t, int512_t, false, true>,
  decimalint_fast_cast<int256_t, int256_t, false, true>,
  decimalint_fast_cast<int256_t, int512_t, false, true>,
  decimalint_fast_cast<int512_t, int512_t, false, true>,

  decimalint_fast_cast<int32_t, int32_t, false, false>,
  decimalint_fast_cast<int32_t, int64_t, false, false>,
  decimalint_fast_cast<int32_t, int128_t, false, false>,
  decimalint_fast_cast<int32_t, int256_t, false, false>,
  decimalint_fast_cast<int32_t, int512_t, false, false>,
  decimalint_fast_cast<int64_t, int64_t, false, false>,
  decimalint_fast_cast<int64_t, int128_t, false, false>,
  decimalint_fast_cast<int64_t, int256_t, false, false>,
  decimalint_fast_cast<int64_t, int512_t, false, false>,
  decimalint_fast_cast<int128_t, int128_t, false, false>,
  decimalint_fast_cast<int128_t, int256_t, false, false>,
  decimalint_fast_cast<int128_t, int512_t, false, false>,
  decimalint_fast_cast<int256_t, int256_t, false, false>,
  decimalint_fast_cast<int256_t, int512_t, false, false>,
  decimalint_fast_cast<int512_t, int512_t, false, false>,
};

static ObExpr::EvalBatchFunc g_decimalint_cast_batch_functions[] = {
  int_fast_batch_cast<int32_t, int32_t, int64_t, true>,
  int_fast_batch_cast<int32_t, int64_t, int64_t, true>,
  int_fast_batch_cast<int32_t, int128_t, int64_t, true>,
  int_fast_batch_cast<int32_t, int256_t, int64_t, true>,
  int_fast_batch_cast<int32_t, int512_t, int64_t, true>,
  int_fast_batch_cast<int64_t, int64_t, int64_t, true>,
  int_fast_batch_cast<int64_t, int128_t, int64_t, true>,
  int_fast_batch_cast<int64_t, int256_t, int64_t, true>,
  int_fast_batch_cast<int64_t, int512_t, int64_t, true>,
  int_fast_batch_cast<int128_t, int128_t, int64_t, true>,
  int_fast_batch_cast<int128_t, int256_t, int64_t, true>,
  int_fast_batch_cast<int128_t, int512_t, int64_t, true>,

  int_fast_batch_cast<int32_t, int32_t, uint64_t, true>,
  int_fast_batch_cast<int32_t, int64_t, uint64_t, true>,
  int_fast_batch_cast<int32_t, int128_t, uint64_t, true>,
  int_fast_batch_cast<int32_t, int256_t, uint64_t, true>,
  int_fast_batch_cast<int32_t, int512_t, uint64_t, true>,
  int_fast_batch_cast<int64_t, int64_t, uint64_t, true>,
  int_fast_batch_cast<int64_t, int128_t, uint64_t, true>,
  int_fast_batch_cast<int64_t, int256_t, uint64_t, true>,
  int_fast_batch_cast<int64_t, int512_t, uint64_t, true>,
  int_fast_batch_cast<int128_t, int128_t, uint64_t, true>,
  int_fast_batch_cast<int128_t, int256_t, uint64_t, true>,
  int_fast_batch_cast<int128_t, int512_t, uint64_t, true>,

  int_fast_batch_cast<int32_t, int32_t, int64_t, false>,
  int_fast_batch_cast<int32_t, int64_t, int64_t, false>,
  int_fast_batch_cast<int32_t, int128_t, int64_t, false>,
  int_fast_batch_cast<int32_t, int256_t, int64_t, false>,
  int_fast_batch_cast<int32_t, int512_t, int64_t, false>,
  int_fast_batch_cast<int64_t, int64_t, int64_t, false>,
  int_fast_batch_cast<int64_t, int128_t, int64_t, false>,
  int_fast_batch_cast<int64_t, int256_t, int64_t, false>,
  int_fast_batch_cast<int64_t, int512_t, int64_t, false>,
  int_fast_batch_cast<int128_t, int128_t, int64_t, false>,
  int_fast_batch_cast<int128_t, int256_t, int64_t, false>,
  int_fast_batch_cast<int128_t, int512_t, int64_t, false>,

  int_fast_batch_cast<int32_t, int32_t, uint64_t, false>,
  int_fast_batch_cast<int32_t, int64_t, uint64_t, false>,
  int_fast_batch_cast<int32_t, int128_t, uint64_t, false>,
  int_fast_batch_cast<int32_t, int256_t, uint64_t, false>,
  int_fast_batch_cast<int32_t, int512_t, uint64_t, false>,
  int_fast_batch_cast<int64_t, int64_t, uint64_t, false>,
  int_fast_batch_cast<int64_t, int128_t, uint64_t, false>,
  int_fast_batch_cast<int64_t, int256_t, uint64_t, false>,
  int_fast_batch_cast<int64_t, int512_t, uint64_t, false>,
  int_fast_batch_cast<int128_t, int128_t, uint64_t, false>,
  int_fast_batch_cast<int128_t, int256_t, uint64_t, false>,
  int_fast_batch_cast<int128_t, int512_t, uint64_t, false>,

  decimalint_fast_batch_cast<int32_t, int32_t, true, true>,
  decimalint_fast_batch_cast<int32_t, int64_t, true, true>,
  decimalint_fast_batch_cast<int32_t, int128_t, true, true>,
  decimalint_fast_batch_cast<int32_t, int256_t, true, true>,
  decimalint_fast_batch_cast<int32_t, int512_t, true, true>,
  decimalint_fast_batch_cast<int64_t, int64_t, true, true>,
  decimalint_fast_batch_cast<int64_t, int128_t, true, true>,
  decimalint_fast_batch_cast<int64_t, int256_t, true, true>,
  decimalint_fast_batch_cast<int64_t, int512_t, true, true>,
  decimalint_fast_batch_cast<int128_t, int128_t, true, true>,
  decimalint_fast_batch_cast<int128_t, int256_t, true, true>,
  decimalint_fast_batch_cast<int128_t, int512_t, true, true>,
  decimalint_fast_batch_cast<int256_t, int256_t, true, true>,
  decimalint_fast_batch_cast<int256_t, int512_t, true, true>,
  decimalint_fast_batch_cast<int512_t, int512_t, true, true>,

  decimalint_fast_batch_cast<int32_t, int32_t, true, false>,
  decimalint_fast_batch_cast<int32_t, int64_t, true, false>,
  decimalint_fast_batch_cast<int32_t, int128_t, true, false>,
  decimalint_fast_batch_cast<int32_t, int256_t, true, false>,
  decimalint_fast_batch_cast<int32_t, int512_t, true, false>,
  decimalint_fast_batch_cast<int64_t, int64_t, true, false>,
  decimalint_fast_batch_cast<int64_t, int128_t, true, false>,
  decimalint_fast_batch_cast<int64_t, int256_t, true, false>,
  decimalint_fast_batch_cast<int64_t, int512_t, true, false>,
  decimalint_fast_batch_cast<int128_t, int128_t, true, false>,
  decimalint_fast_batch_cast<int128_t, int256_t, true, false>,
  decimalint_fast_batch_cast<int128_t, int512_t, true, false>,
  decimalint_fast_batch_cast<int256_t, int256_t, true, false>,
  decimalint_fast_batch_cast<int256_t, int512_t, true, false>,
  decimalint_fast_batch_cast<int512_t, int512_t, true, false>,

  decimalint_fast_batch_cast<int32_t, int32_t, false, true>,
  decimalint_fast_batch_cast<int32_t, int64_t, false, true>,
  decimalint_fast_batch_cast<int32_t, int128_t, false, true>,
  decimalint_fast_batch_cast<int32_t, int256_t, false, true>,
  decimalint_fast_batch_cast<int32_t, int512_t, false, true>,
  decimalint_fast_batch_cast<int64_t, int64_t, false, true>,
  decimalint_fast_batch_cast<int64_t, int128_t, false, true>,
  decimalint_fast_batch_cast<int64_t, int256_t, false, true>,
  decimalint_fast_batch_cast<int64_t, int512_t, false, true>,
  decimalint_fast_batch_cast<int128_t, int128_t, false, true>,
  decimalint_fast_batch_cast<int128_t, int256_t, false, true>,
  decimalint_fast_batch_cast<int128_t, int512_t, false, true>,
  decimalint_fast_batch_cast<int256_t, int256_t, false, true>,
  decimalint_fast_batch_cast<int256_t, int512_t, false, true>,
  decimalint_fast_batch_cast<int512_t, int512_t, false, true>,

  decimalint_fast_batch_cast<int32_t, int32_t, false, false>,
  decimalint_fast_batch_cast<int32_t, int64_t, false, false>,
  decimalint_fast_batch_cast<int32_t, int128_t, false, false>,
  decimalint_fast_batch_cast<int32_t, int256_t, false, false>,
  decimalint_fast_batch_cast<int32_t, int512_t, false, false>,
  decimalint_fast_batch_cast<int64_t, int64_t, false, false>,
  decimalint_fast_batch_cast<int64_t, int128_t, false, false>,
  decimalint_fast_batch_cast<int64_t, int256_t, false, false>,
  decimalint_fast_batch_cast<int64_t, int512_t, false, false>,
  decimalint_fast_batch_cast<int128_t, int128_t, false, false>,
  decimalint_fast_batch_cast<int128_t, int256_t, false, false>,
  decimalint_fast_batch_cast<int128_t, int512_t, false, false>,
  decimalint_fast_batch_cast<int256_t, int256_t, false, false>,
  decimalint_fast_batch_cast<int256_t, int512_t, false, false>,
  decimalint_fast_batch_cast<int512_t, int512_t, false, false>,
};

REG_SER_FUNC_ARRAY(OB_SFA_DECIMAL_INT_CAST_EXPR_EVAL, g_decimalint_cast_functions,
                   ARRAYSIZEOF(g_decimalint_cast_functions));

REG_SER_FUNC_ARRAY(OB_SFA_DECIMAL_INT_CAST_EXPR_EVAL_BATCH, g_decimalint_cast_batch_functions,
                   ARRAYSIZEOF(g_decimalint_cast_batch_functions));

int eval_questionmark_decint2nmb(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  // child is questionmark, do not need evaluation.
  const ObDatum &child = expr.args_[0]->locate_expr_datum(ctx);
  ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
  ObNumStackOnceAlloc tmp_alloc;
  number::ObNumber out_nmb;
  if (OB_FAIL(wide::to_number(child.get_decimal_int(), child.get_int_bytes(), in_scale,
                              tmp_alloc, out_nmb))) {
    LOG_WARN("to_number failed", K(ret));
  } else {
    expr_datum.set_number(out_nmb);
  }
  return ret;
}

static int _eval_questionmark_nmb2decint(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum,
                                         const ObCastMode cm)
{
  int ret = OB_SUCCESS;
  // child is questionmark, do not need evaluation.
  const ObDatum &child = expr.args_[0]->locate_expr_datum(ctx);
  ObDecimalIntBuilder tmp_alloc, res_val;
  number::ObNumber in_nmb(child.get_number());
  ObScale in_scale = in_nmb.get_scale();
  ObPrecision out_prec = expr.datum_meta_.precision_;
  ObScale out_scale = expr.datum_meta_.scale_;
  ObDecimalInt *decint = nullptr;
  int32_t int_bytes = 0;
  if (OB_FAIL(wide::from_number(in_nmb, tmp_alloc, in_scale, decint, int_bytes))) {
    LOG_WARN("from number failed", K(ret));
  } else if (OB_FAIL(scale_const_decimalint_expr(decint, int_bytes, in_scale, out_scale, out_prec, cm, res_val))) {
    LOG_WARN("scale const decimal int failed", K(ret));
  } else {
    expr_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
  }
  return ret;
}

static int _eval_questionmark_decint2decint(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum,
                                            const ObCastMode cm)
{
  int ret = OB_SUCCESS;
  ObScale out_scale = expr.datum_meta_.scale_;
  ObPrecision out_prec = expr.datum_meta_.precision_;
  ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
  ObDecimalIntBuilder res_val;
  const ObDatum &child = expr.args_[0]->locate_expr_datum(ctx);
  if (OB_FAIL(ObDatumCast::common_scale_decimalint(child.get_decimal_int(), child.get_int_bytes(),
                                                   in_scale, out_scale, out_prec, cm, res_val))) {
    LOG_WARN("common scale decimal int failed", K(ret));
  } else {
    expr_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
  }
  return ret;
}

// nmb2decint

int eval_questionmark_nmb2decint_eqcast(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  return _eval_questionmark_nmb2decint(expr, ctx, expr_datum, CM_CONST_TO_DECIMAL_INT_EQ);
}

// decint2decint

int eval_questionmark_decint2decint_eqcast(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  return _eval_questionmark_decint2decint(expr, ctx, expr_datum, CM_CONST_TO_DECIMAL_INT_EQ);
}

int eval_questionmark_decint2decint_normalcast(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  return _eval_questionmark_decint2decint(expr, ctx, expr_datum, CM_NONE);
}

} // namespace sql
} // namespace oceanbase
#endif