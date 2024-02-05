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
#include "sql/engine/expr/vector_cast/vector_cast.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "share/object/ob_obj_cast_util.h"

namespace oceanbase
{
namespace sql
{
template<typename Vector, typename ResFmt>
struct __decint_cast_impl
{
  template<typename in_type, typename out_type, bool is_scale_up>
  static void implicit_scale(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                             const unsigned scale, const EvalBound &bound)
  {
    static_assert(wide::IsIntegral<out_type>::value && wide::IsIntegral<in_type>::value, "");
    SQL_LOG(DEBUG, "batch implicit scale decimal int", K(lbt()), K(bound), K(is_scale_up));
    out_type sf = get_scale_factor<out_type>(scale);
    Vector *input_vector = static_cast<Vector *>(expr.args_[0]->get_vector(ctx));
    ResFmt *output_vector = static_cast<ResFmt *>(expr.get_vector(ctx));
    auto scale_up_task = [&](int i) __attribute__((always_inline))
    {
      out_type res = *reinterpret_cast<const in_type *>(input_vector->get_payload(i));
      res = res * sf;
      output_vector->set_payload(i, &res, sizeof(out_type));
    };

    auto fast_scale_up_task = [&](int i) __attribute__((always_inline))
    {
      if (sizeof(in_type) == sizeof(out_type) && wide::SignedConcept<in_type>::value) {
        output_vector->set_payload(i, input_vector->get_payload(i), sizeof(in_type));
      } else {
        out_type res = *reinterpret_cast<const in_type *>(input_vector->get_payload(i));
        output_vector->set_payload(i, &res, sizeof(out_type));
      }
    };

    auto scale_down_task = [&](int i) __attribute__((always_inline))
    {
      out_type res = *reinterpret_cast<const in_type *>(input_vector->get_payload(i));
      bool is_neg = (res < 0);
      if (is_neg) { res = -res; }
      out_type remain = res % sf;
      res = res / sf;
      if (remain >= (sf >> 1)) { res = res + 1; }
      if (is_neg) { res = -res; }
      output_vector->set_payload(i, &res, sizeof(out_type));
    };

    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    if (is_scale_up) {
      if (OB_LIKELY(bound.get_all_rows_active() && eval_flags.accumulate_bit_cnt(bound) == 0)) {
        if (sf == 1) {
          if (!input_vector->has_null()) {
            for (int i = bound.start(); i < bound.end(); i++) {
              fast_scale_up_task(i);
            }
          } else {
            for(int i = bound.start(); i < bound.end(); i++) {
              if (input_vector->is_null(i)) {
                output_vector->set_null(i);
              } else {
                fast_scale_up_task(i);
              }
            }
          }
        } else {
          if (!input_vector->has_null()) {
            for (int i = bound.start(); i < bound.end(); i++) {
              scale_up_task(i);
            }
          } else {
            for (int i = bound.start(); i < bound.end(); i++) {
              if (input_vector->is_null(i)) {
                output_vector->set_null(i);
              } else {
                scale_up_task(i);
              }
            }
          }
        }
        eval_flags.set_all(bound.start(), bound.end());
      } else {
        for (int i = bound.start(); i < bound.end(); i++) {
          if (skip.at(i) || eval_flags.at(i)) { continue; }
          if (input_vector->is_null(i)) {
            output_vector->set_null(i);
          } else {
            scale_up_task(i);
          }
          eval_flags.set(i);
        }
      }
    } else {
      if (OB_LIKELY(bound.get_all_rows_active() && eval_flags.accumulate_bit_cnt(bound) == 0)) {
        if (!input_vector->has_null()) {
          for (int i = bound.start(); i < bound.end(); i++) { scale_down_task(i); }
        } else {
          for(int i = bound.start(); i < bound.end(); i++) {
            if (input_vector->is_null(i)) {
              output_vector->set_null(i);
            } else {
              scale_down_task(i);
            }
          }
        }
        eval_flags.set_all(bound.start(), bound.end());
      } else {
        for (int i = bound.start(); i < bound.end(); i++) {
          if (skip.at(i) || eval_flags.at(i)) { continue; }
          if (input_vector->is_null(i)) {
            output_vector->set_null(i);
          } else {
            scale_down_task(i);
          }
          eval_flags.set(i);
        }
      }
    }
  }

  template<typename in_type, typename out_type, bool is_scale_up>
  static void explicit_scale(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                             const unsigned scale, const EvalBound &bound)
  {
    using calc_type = typename wide::CommonType<in_type, out_type>::type;
    static_assert(wide::IsIntegral<out_type>::value && wide::IsIntegral<in_type>::value, "");
    SQL_LOG(DEBUG, "batch explicit scale decimal int", K(lbt()), K(bound), K(is_scale_up));
    ObPrecision out_prec = expr.datum_meta_.precision_;
    OB_ASSERT(out_prec <= OB_MAX_DECIMAL_POSSIBLE_PRECISION);
    const ObDecimalInt *min_v = wide::ObDecimalIntConstValue::get_min_lower(out_prec);
    const ObDecimalInt *max_v = wide::ObDecimalIntConstValue::get_max_upper(out_prec);

    calc_type sf = get_scale_factor<calc_type>(scale);
    Vector *input_vector = static_cast<Vector *>(expr.args_[0]->get_vector(ctx));
    ResFmt *output_vector = static_cast<ResFmt *>(expr.get_vector(ctx));
    auto scale_up_task = [&](int i) __attribute__((always_inline))
    {
      calc_type lhs = *reinterpret_cast<const in_type *>(input_vector->get_payload(i));
      calc_type res;
      bool is_neg = lhs < 0;
      bool overflow = wide::mul_overflow(lhs, sf, res);
      if (overflow) {
        output_vector->set_payload(i, is_neg ? min_v : max_v, sizeof(out_type));
      } else if (res >= *reinterpret_cast<const out_type *>(max_v)) {
        output_vector->set_payload(i, max_v, sizeof(out_type));
      } else if (wide::SignedConcept<calc_type>::value
                 && res <= *reinterpret_cast<const out_type *>(min_v)) {
        output_vector->set_payload(i, min_v, sizeof(out_type));
      } else {
        out_type tmp_res = static_cast<out_type>(res);
        output_vector->set_payload(i, &tmp_res, sizeof(out_type));
      }
    };

    auto scale_down_task = [&](int i) __attribute__((always_inline))
    {
      calc_type res = *reinterpret_cast<const in_type *>(input_vector->get_payload(i));
      bool is_neg = (res < 0);
      if (is_neg) { res = -res; }
      calc_type remain = res % sf;
      res = res / sf;
      if (remain >= (sf >> 1)) { res = res + 1; }
      if (is_neg) { res = -res; }
      if (res >= *reinterpret_cast<const out_type *>(max_v)) {
        output_vector->set_payload(i, max_v, sizeof(out_type));
      } else if (wide::SignedConcept<calc_type>::value
                 && res <= *reinterpret_cast<const out_type *>(min_v)) {
        output_vector->set_payload(i, min_v, sizeof(out_type));
      } else {
        out_type tmp_res = static_cast<out_type>(res);
        output_vector->set_payload(i, &tmp_res, sizeof(out_type));
      }
    };
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    if (is_scale_up) {
      if (OB_LIKELY(bound.get_all_rows_active() && eval_flags.accumulate_bit_cnt(bound) == 0)) {
        if (!input_vector->has_null()) {
          for (int i = bound.start(); i < bound.end(); i++) {
            scale_up_task(i);
          }
        } else {
          for (int i = bound.start(); i < bound.end(); i++) {
            if (input_vector->is_null(i)) {
              output_vector->set_null(i);
            } else {
              scale_up_task(i);
            }
          }
        }
        eval_flags.set_all(bound.start(), bound.end());
      } else {
        for (int i = bound.start(); i < bound.end(); i++) {
          if (skip.at(i) || eval_flags.at(i)) {
            continue;
          } else if (input_vector->is_null(i)) {
            output_vector->set_null(i);
          } else {
            scale_up_task(i);
          }
          eval_flags.set(i);
        }
      }
    } else {
      if (OB_LIKELY(bound.get_all_rows_active() && eval_flags.accumulate_bit_cnt(bound) == 0)) {
        if (!input_vector->has_null()) {
          for (int i = bound.start(); i < bound.end(); i++) {
            scale_down_task(i);
          }
        } else {
          for (int i = bound.start(); i < bound.end(); i++) {
            if (input_vector->is_null(i)) {
              output_vector->set_null(i);
            } else {
              scale_down_task(i);
            }
          }
        }
        eval_flags.set_all(bound.start(), bound.end());
      } else {
        for (int i = bound.start(); i < bound.end(); i++) {
          if (skip.at(i) || eval_flags.at(i)) {
            continue;
          } else if(input_vector->is_null(i)) {
            output_vector->set_null(i);
          } else {
            scale_down_task(i);
          }
          eval_flags.set(i);
        }
      }
    }
  }

  template<typename in_type, typename out_type, bool is_scale_up>
  static void const_scale(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                          const unsigned scale, const EvalBound &bound)
  {
    using calc_type = typename wide::CommonType<in_type, out_type>::type;
    static_assert(wide::IsIntegral<in_type>::value && wide::IsIntegral<out_type>::value, "");
    ObPrecision out_prec = expr.datum_meta_.precision_;
    const ObDecimalInt *min_v = wide::ObDecimalIntConstValue::get_min_lower(out_prec);
    const ObDecimalInt *max_v = wide::ObDecimalIntConstValue::get_max_upper(out_prec);
    calc_type sf = get_scale_factor<calc_type>(scale);
    Vector *input_vector = static_cast<Vector *>(expr.args_[0]->get_vector(ctx));
    ResFmt *output_vector = static_cast<ResFmt *>(expr.get_vector(ctx));
    const ObCastMode cast_mode = expr.extra_;
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    auto scale_up_task = [&](int i) __attribute__((always_inline))
    {
      calc_type lhs = *reinterpret_cast<const in_type *>(input_vector->get_payload(i));
      calc_type res;
      bool is_neg = (lhs < 0);
      bool overflow = wide::mul_overflow(lhs, sf, res);
      if (overflow) {
        output_vector->set_payload(i, is_neg ? min_v : max_v, sizeof(out_type));
      } else if (res >= *reinterpret_cast<const out_type *>(max_v)) {
        output_vector->set_payload(i, max_v, sizeof(out_type));
      } else if (wide::SignedConcept<calc_type>::value
                 && res <= *reinterpret_cast<const out_type *>(min_v)) {
        output_vector->set_payload(i, min_v, sizeof(out_type));
      } else {
        out_type tmp_res = static_cast<out_type>(res);
        output_vector->set_payload(i, &tmp_res, sizeof(out_type));
      }
    };

    auto scale_down_task = [&](int i) __attribute__((always_inline))
    {
      calc_type res = *reinterpret_cast<const in_type *>(input_vector->get_payload(i));
      bool is_neg = (res < 0);
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
          output_vector->set_payload(i, is_neg ? min_v : max_v, sizeof(out_type));
          return;
        }
      }
      if (res >= *reinterpret_cast<const out_type *>(max_v)) {
        output_vector->set_payload(i, max_v, sizeof(out_type));
      } else if (wide::SignedConcept<calc_type>::value
                 && res <= *reinterpret_cast<const out_type *>(min_v)) {
        output_vector->set_payload(i, min_v, sizeof(out_type));
      } else {
        out_type tmp_res = static_cast<out_type>(res);
        output_vector->set_payload(i, &tmp_res, sizeof(out_type));
      }
    };

    SQL_LOG(DEBUG, "batch const scale", K(lbt()), K(bound), K(is_scale_up), K(cast_mode));
    if (is_scale_up) {
      if (OB_LIKELY(bound.get_all_rows_active() && eval_flags.accumulate_bit_cnt(bound) == 0)) {
        if (!input_vector->has_null()) {
          for (int i = bound.start(); i < bound.end(); i++) {
            scale_up_task(i);
          }
        } else {
          for (int i = bound.start(); i < bound.end(); i++) {
            if (input_vector->is_null(i)) {
              output_vector->set_null(i);
            } else {
              scale_up_task(i);
            }
          }
        }
        eval_flags.set_all(bound.start(), bound.end());
      } else {
        for (int i = bound.start(); i < bound.end(); i++) {
          if (skip.at(i) || eval_flags.at(i)) {
            continue;
          } else if (input_vector->is_null(i)) {
            output_vector->set_null(i);
          } else {
            scale_up_task(i);
          }
          eval_flags.set(i);
        }
      }
    } else {
      if (OB_LIKELY(bound.get_all_rows_active() && eval_flags.accumulate_bit_cnt(bound) == 0)) {
        if (!input_vector->has_null()) {
          for (int i = bound.start(); i < bound.end(); i++) { scale_down_task(i); }
        } else {
          for (int i = bound.start(); i < bound.end(); i++) {
            if (input_vector->is_null(i)) {
              output_vector->set_null(i);
            } else {
              scale_down_task(i);
            }
          }
        }
        eval_flags.set_all(bound.start(), bound.end());
      } else {
        for (int i = bound.start(); i < bound.end(); i++) {
          if (skip.at(i) || eval_flags.at(i)) {
            continue;
          } else if (input_vector->is_null(i)) {
            output_vector->set_null(i);
          } else {
            scale_down_task(i);
          }
          eval_flags.set(i);
        }
      }
    }
  }

  template<typename in_type, typename out_type>
  static void no_scale_cp(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                          const EvalBound &bound)
  {
    static_assert(wide::IsIntegral<in_type>::value && wide::IsIntegral<out_type>::value, "");
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    Vector *input_vector = static_cast<Vector *>(expr.args_[0]->get_vector(ctx));
    ResFmt *output_vector = static_cast<ResFmt *>(expr.get_vector(ctx));
    if (OB_LIKELY(bound.get_all_rows_active() && eval_flags.accumulate_bit_cnt(bound) == 0)) {
      if(!input_vector->has_null()) {
        for (int i = bound.start(); i < bound.end(); i++) {
          output_vector->set_payload(i, input_vector->get_payload(i), sizeof(in_type));
        }
      } else {
        for (int i = bound.start(); i < bound.end(); i++) {
          if (input_vector->is_null(i)) {
            output_vector->set_null(i);
          } else {
            output_vector->set_payload(i, input_vector->get_payload(i), sizeof(in_type));
          }
        }
      }
      eval_flags.set_all(bound.start(), bound.end());
    } else {
      for (int i = bound.start(); i < bound.end(); i++) {
        if (skip.at(i) || eval_flags.at(i)) {
          continue;;
        } else if (input_vector->is_null(i)) {
          output_vector->set_null(i);
        } else {
          output_vector->set_payload(i, input_vector->get_payload(i), sizeof(in_type));
        }
        eval_flags.set(i);
      }
    }
  }

  template<typename in_type, typename out_type>
  static int eval_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                         const EvalBound &bound)
  {
    int ret = OB_SUCCESS;
    SQL_LOG(DEBUG, "eval vector for decint to decint", K(ret), K(bound));
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    if (eval_flags.accumulate_bit_cnt(bound) == bound.range_size()) {
      // do nothing
    } else {
      bool is_integer_input = ob_is_integer_type(expr.args_[0]->datum_meta_.type_);
      ObScale in_scale = (is_integer_input ? 0 : expr.args_[0]->datum_meta_.scale_);
      ObScale out_scale = expr.datum_meta_.scale_;
      ObPrecision out_prec = expr.datum_meta_.precision_;
      ObPrecision in_prec = expr.args_[0]->datum_meta_.precision_;
      if (is_integer_input) {
        if (ob_is_int_tc(expr.args_[0]->datum_meta_.type_)) {
          if (in_prec > MAX_PRECISION_DECIMAL_INT_64) { in_prec = MAX_PRECISION_DECIMAL_INT_64; }
        } else { // uint tc
          in_prec =
            ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][expr.args_[0]->datum_meta_.type_].get_precision();
        }
      }
      bool is_scale_up = (out_scale >= in_scale);
      if (ObDatumCast::need_scale_decimalint(in_scale, in_prec, out_scale, out_prec)
          || sizeof(in_type) != sizeof(out_type)) { // need scale decimal int
        if (CM_IS_CONST_TO_DECIMAL_INT(expr.extra_)) {
          if (is_scale_up) {
            const_scale<in_type, out_type, true>(expr, ctx, skip, out_scale - in_scale, bound);
          } else {
            const_scale<in_type, out_type, false>(expr, ctx, skip, in_scale - out_scale, bound);
          }
        } else if (CM_IS_COLUMN_CONVERT(expr.extra_) || CM_IS_EXPLICIT_CAST(expr.extra_)) {
          if (is_scale_up) {
            explicit_scale<in_type, out_type, true>(expr, ctx, skip, out_scale - in_scale, bound);
          } else {
            explicit_scale<in_type, out_type, false>(expr, ctx, skip, in_scale - out_scale, bound);
          }
        } else {
          ObPrecision in_prec = expr.args_[0]->datum_meta_.precision_;
          ObPrecision out_prec = expr.datum_meta_.precision_;
          // integer const expr may have a small precision, e.g. `1234`  results in (P, S) = (4, 0).
          // however, the value type of `1234` is `int64_t`, which has a wider precision range ([10, 18]).
          // we add condition `(in_prec > 0 && in_prec <= out_prec)` here to deal this case.
          OB_ASSERT(sizeof(out_type) >= sizeof(in_type) || CM_IS_BY_TRANSFORMER(expr.extra_)
                    || (in_prec > 0 && in_prec <= out_prec));
          if (is_scale_up) {
            implicit_scale<in_type, out_type, true>(expr, ctx, skip, out_scale - in_scale, bound);
          } else {
            implicit_scale<in_type, out_type, false>(expr, ctx, skip, in_scale - out_scale, bound);
          }
        }
      } else {
        no_scale_cp<in_type, out_type>(expr, ctx, skip, bound);
      }
    }
    return ret;
  }

  template<typename in_type>
  static int to_nmb_eval_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                         const EvalBound &bound)
  {
    int ret = OB_SUCCESS;
    SQL_LOG(DEBUG, "eval vectro for decint to number", K(ret), K(bound), K(sizeof(in_type)));
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    Vector *input_vector = static_cast<Vector *>(expr.args_[0]->get_vector(ctx));
    ResFmt *output_vector = static_cast<ResFmt *>(expr.get_vector(ctx));
    if (eval_flags.accumulate_bit_cnt(bound) == bound.range_size()) {
      // do nothing
    } else {
      ObScale in_scale = expr.args_[0]->datum_meta_.scale_;
      // ObScale out_scale = expr.datum_meta_.scale_;
      if (OB_UNLIKELY(in_scale < 0)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "invalid input scale", K(ret), K(in_scale));
      } else if (sizeof(in_type) <= sizeof(int64_t) && in_scale <= 9) {
        number::ObNumber out_nmb;
        uint32_t digits[3] = {0};
        auto cast_unmb = [&](int64_t idx) __attribute__((always_inline))
        {
          // fast cast to unumber
          int ret = OB_SUCCESS;
          int warning = ret;
          int64_t input = *reinterpret_cast<const in_type *>(input_vector->get_payload(idx));
          if (OB_FAIL(wide::to_number(input, in_scale, (uint32_t *)digits, 3, out_nmb))) {
            SQL_LOG(WARN, "to_number failed", K(ret));
          } else if (CAST_FAIL(numeric_negative_check(out_nmb))) {
            SQL_LOG(WARN, "numeric negative check failed", K(ret));
          } else {
            output_vector->set_number(idx, out_nmb);
          }
          return ret;
        };
        auto cast_nmb = [&](int64_t idx) __attribute__((always_inline))
        {
          // fast cast to number
          int ret = OB_SUCCESS;
          int64_t input = *reinterpret_cast<const in_type *>(input_vector->get_payload(idx));
          if (OB_FAIL(wide::to_number(input, in_scale, (uint32_t *)digits, 3, out_nmb))) {
            SQL_LOG(WARN, "to_number failed", K(ret));
          } else {
            output_vector->set_number(idx, out_nmb);
          }
          return ret;
        };
        bool is_cast_unmb = (expr.datum_meta_.type_ == ObUNumberType);
        if (OB_LIKELY(bound.get_all_rows_active() && eval_flags.accumulate_bit_cnt(bound) == 0)) {
          if (!input_vector->has_null()) {
            if (!is_cast_unmb) {
              for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
                ret = cast_nmb(i);
              }
            } else {
              for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
                ret = cast_unmb(i);
              }
            }
          } else {
            if (!is_cast_unmb) {
              for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
                if (input_vector->is_null(i)) {
                  output_vector->set_null(i);
                } else {
                  ret = cast_nmb(i);
                }
              }
            } else {
              for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
                if (input_vector->is_null(i)) {
                  output_vector->set_null(i);
                } else {
                  ret = cast_unmb(i);
                }
              }
            }
          }
          if (OB_SUCC(ret)) {
            eval_flags.set_all(bound.start(), bound.end());
          }
        } else {
          if (!is_cast_unmb) {
            for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
              if (eval_flags.at(i) || skip.at(i)) {
                continue;
              } else if (input_vector->is_null(i)){
                output_vector->set_null(i);
              } else {
                ret = cast_nmb(i);
              }
              if (OB_SUCC(ret)) {
                eval_flags.set(i);
              }
            }
          } else {
            for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
              if (eval_flags.at(i) || skip.at(i)) {
                continue;
              } else if (input_vector->is_null(i)) {
                output_vector->set_null(i);
              } else {
                ret = cast_unmb(i);
              }
              if (OB_SUCC(ret)) { eval_flags.set(i); }
            }
          }
        }
      } else {
        number::ObNumber out_nmb;
        ObNumStackOnceAlloc tmp_alloc;
        for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
          if (eval_flags.at(i) || skip.at(i)) {
            continue;
          } else if (input_vector->is_null(i)) {
            output_vector->set_null(i);
          } else {
            const in_type *in_val = reinterpret_cast<const in_type *>(input_vector->get_payload(i));
            if (OB_FAIL(wide::to_number(*in_val, in_scale, tmp_alloc, out_nmb))) {
              SQL_LOG(WARN, "to number failed", K(ret));
            } else if (ObUNumberType == expr.datum_meta_.type_) {
              int warning = OB_SUCCESS;
              if (CAST_FAIL(numeric_negative_check(out_nmb))) {
                SQL_LOG(WARN, "numeric negative check failed", K(ret));
              } else {
                output_vector->set_number(i, out_nmb);
              }
            } else {
              output_vector->set_number(i, out_nmb);
            }
          }
          if (OB_SUCC(ret)) {
            eval_flags.set(i);
            tmp_alloc.free();
          }
        }
      }
    }
    return ret;
  }
};

// define implicit cast functions
#define DEF_DECINT_IMPLICIT_CASTS(in_tc)                                                           \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_DEC_INT32)                                           \
  {                                                                                                \
    return __decint_cast_impl<IN_VECTOR, OUT_VECTOR>::template eval_vector<RTCType<in_tc>,         \
                                                                           int32_t>(expr, ctx,     \
                                                                                    skip, bound);  \
  }                                                                                                \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_DEC_INT64)                                           \
  {                                                                                                \
    return __decint_cast_impl<IN_VECTOR, OUT_VECTOR>::template eval_vector<RTCType<in_tc>,         \
                                                                           int64_t>(expr, ctx,     \
                                                                                    skip, bound);  \
  }                                                                                                \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_DEC_INT128)                                          \
  {                                                                                                \
    return __decint_cast_impl<IN_VECTOR, OUT_VECTOR>::template eval_vector<RTCType<in_tc>,         \
                                                                           int128_t>(expr, ctx,    \
                                                                                     skip, bound); \
  }                                                                                                \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_DEC_INT256)                                          \
  {                                                                                                \
    return __decint_cast_impl<IN_VECTOR, OUT_VECTOR>::template eval_vector<RTCType<in_tc>,         \
                                                                           int256_t>(expr, ctx,    \
                                                                                     skip, bound); \
  }                                                                                                \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_DEC_INT512)                                          \
  {                                                                                                \
    return __decint_cast_impl<IN_VECTOR, OUT_VECTOR>::template eval_vector<RTCType<in_tc>,         \
                                                                           int512_t>(expr, ctx,    \
                                                                                     skip, bound); \
  }

LST_DO_CODE(DEF_DECINT_IMPLICIT_CASTS,
            VEC_TC_DEC_INT32,
            VEC_TC_DEC_INT64,
            VEC_TC_DEC_INT128,
            VEC_TC_DEC_INT256,
            VEC_TC_DEC_INT512);

#define DEF_INTEGER_DECINT_CAST(out_tc)                                                            \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_INTEGER, out_tc)                                            \
  {                                                                                                \
    return __decint_cast_impl<IN_VECTOR, OUT_VECTOR>::template eval_vector<int64_t,                \
                                                                           RTCType<out_tc>>(       \
      expr, ctx, skip, bound);                                                                     \
  }

#define DEF_UINTEGER_DECINT_CAST(out_tc)                                                           \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_UINTEGER, out_tc)                                           \
  {                                                                                                \
    return __decint_cast_impl<IN_VECTOR, OUT_VECTOR>::template eval_vector<uint64_t,               \
                                                                           RTCType<out_tc>>(       \
      expr, ctx, skip, bound);                                                                     \
  }

LST_DO_CODE(DEF_INTEGER_DECINT_CAST,
            VEC_TC_DEC_INT32,
            VEC_TC_DEC_INT64,
            VEC_TC_DEC_INT128,
            VEC_TC_DEC_INT256,
            VEC_TC_DEC_INT512);

LST_DO_CODE(DEF_UINTEGER_DECINT_CAST,
            VEC_TC_DEC_INT32,
            VEC_TC_DEC_INT64,
            VEC_TC_DEC_INT128,
            VEC_TC_DEC_INT256,
            VEC_TC_DEC_INT512);

#define DEF_DECINT_NMB_CAST(in_tc)                                                                 \
  DEF_VECTOR_IMPLICIT_CAST_FUNC(in_tc, VEC_TC_NUMBER)                                              \
  {                                                                                                \
    return __decint_cast_impl<IN_VECTOR, OUT_VECTOR>::template to_nmb_eval_vector<RTCType<in_tc>>( \
      expr, ctx, skip, bound);                                                                     \
  }

LST_DO_CODE(DEF_DECINT_NMB_CAST,
            VEC_TC_DEC_INT32,
            VEC_TC_DEC_INT64,
            VEC_TC_DEC_INT128,
            VEC_TC_DEC_INT256,
            VEC_TC_DEC_INT512)
} // end sql
} // namespace oceanbase

#undef DEF_DECINT_IMPLICIT_CASTS
#undef DEF_INTEGER_DECINT_CAST
#undef DEF_UINTEGER_DECINT_CAST
#undef DEF_DECINT_NMB_CAST