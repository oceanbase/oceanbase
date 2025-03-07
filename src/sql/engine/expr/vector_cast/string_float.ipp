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
struct StringFloatCastImpl
{
  template<typename OUT_TYPE>
  static int eval_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                         const EvalBound &bound)
  {
    int ret = OB_SUCCESS;
    SQL_LOG(DEBUG, "eval vector for decint to decint", K(ret), K(bound));
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    if (eval_flags.accumulate_bit_cnt(bound) == bound.range_size()) {
      // do nothing
    } else {
      Vector *input_vector = static_cast<Vector *>(expr.args_[0]->get_vector(ctx));
      ResFmt *output_vector = static_cast<ResFmt *>(expr.get_vector(ctx));
      ObObjType in_type = expr.args_[0]->datum_meta_.type_;
      ObObjType out_type = expr.datum_meta_.type_;
      ObCollationType cs_type = expr.args_[0]->datum_meta_.cs_type_;
      double tmp_double = 0.0;
      ObDatum tmp_datum;
      tmp_datum.ptr_ = reinterpret_cast<const char *>(&tmp_double);
      tmp_datum.pack_ = sizeof(double);

      auto convert_string_to_float_task = [&](int i) __attribute__((always_inline))
      {
        ObString in_str(input_vector->get_length(i), input_vector->get_payload(i));
        if (std::is_same<OUT_TYPE, float>::value) {
          float out_f = 0.0;
          if (OB_FAIL(ObDataTypeCastUtil::common_string_float_wrap(expr, in_str, out_f))) {
            SQL_LOG(WARN, "common_string_float_fastfloat failed", K(ret));
          } else {
            output_vector->set_float(i, out_f);
          }
        } else if (std::is_same<OUT_TYPE, double>::value) {
          if (OB_FAIL(common_string_double(expr, in_type, cs_type, out_type, in_str, tmp_datum))) {
            SQL_LOG(WARN, "common_string_double failed", K(ret));
          } else {
            double out_d = tmp_datum.get_double();
            output_vector->set_double(i, out_d);
          }
        } else {
          ret = OB_UNEXPECT_INTERNAL_ERROR;
          SQL_LOG(WARN, "the OUT_TYPE should be float or double.", K(ret));
        }
      };

      if (OB_LIKELY(bound.get_all_rows_active() && eval_flags.accumulate_bit_cnt(bound) == 0)) {
        if (!input_vector->has_null()) {
          for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
            convert_string_to_float_task(i);
          }
          eval_flags.set_all(bound.start(), bound.end());
        } else {
          for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
            if (input_vector->is_null(i)) {
              output_vector->set_null(i);
            } else {
              convert_string_to_float_task(i);
            }
          }
          eval_flags.set_all(bound.start(), bound.end());
        }
      } else {
        for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
          if (skip.at(i) || eval_flags.at(i)) {
            continue;
          } else if (input_vector->is_null(i)) {
            output_vector->set_null(i);
          } else {
            convert_string_to_float_task(i);
          }
          eval_flags.set(i);
        }
      }
    }
    return ret;
  }
};

// define implicit cast functions
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_STRING, VEC_TC_FLOAT)
{                                                                 \
  return StringFloatCastImpl<IN_VECTOR, OUT_VECTOR>::template     \
            eval_vector<float>(expr, ctx, skip, bound);           \
}
DEF_VECTOR_IMPLICIT_CAST_FUNC(VEC_TC_STRING, VEC_TC_DOUBLE)
{                                                                 \
  return StringFloatCastImpl<IN_VECTOR, OUT_VECTOR>::template     \
            eval_vector<double>(expr, ctx, skip, bound);          \
}

} // end sql
} // namespace oceanbase
