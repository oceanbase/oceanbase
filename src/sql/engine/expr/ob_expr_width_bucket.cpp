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

#include "sql/ob_sql_utils.h"
#include "lib/utility/utility.h"
#include "sql/engine/expr/ob_expr_width_bucket.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/charset/ob_dtoa.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

ObExprWidthBucket::ObExprWidthBucket(common::ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_WIDTH_BUCKET, N_WIDTH_BUCKET, 4, NOT_ROW_DIMENSION)
{}

int ObExprWidthBucket::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(types) || 4 != param_num)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(types), K(param_num), K(ret));
  } else if (OB_UNLIKELY(ObNullType == types[0].get_type())) {
    ret = OB_ERR_ARGUMENT_SHOULD_NUMERIC_DATE_DATETIME_TYPE;
    LOG_WARN("first argument should be of numeric or date/datetime type", K(ret));
  } else if (OB_LIKELY(NOT_ROW_DIMENSION == row_dimension_)) {
    type.set_type(ObNumberType);
    common::ObObjType calc_type = types[0].get_type();
    switch (calc_type) {
      case ObNumberType:
        break;
      case ObDateTimeType:
        calc_type = ObDateTimeType;
        break;
      case ObTimestampNanoType:
        calc_type = ObTimestampNanoType;
        break;
      case ObTimestampTZType:
        calc_type = ObTimestampLTZType;
        break;
      case ObTimestampLTZType:
        calc_type = ObTimestampLTZType;
        break;
      default:
        ret = OB_ERR_ARGUMENT_SHOULD_NUMERIC_DATE_DATETIME_TYPE;
        LOG_WARN("first parameter must be number, date or timestamp type", K(ret), K(calc_type));
        break;
    }
    if (OB_SUCC(ret)) {
      types[0].set_calc_type(calc_type);
      types[1].set_calc_type(calc_type);
      types[2].set_calc_type(calc_type);
      types[3].set_calc_type(ObNumberType);
      types[3].set_scale(0);
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  }
  return ret;
}

int ObExprWidthBucket::calc_resultN(ObObj& res, const ObObj* obj_array, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(4 != param_num)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param num", K(ret), K(param_num));
  } else if (OB_UNLIKELY(OB_ISNULL(obj_array) || OB_ISNULL(expr_ctx.calc_buf_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("obj_array or calc_buf_ is null", K(ret));
  } else if (obj_array[0].is_null()) {
    res.set_null();
  } else if (OB_UNLIKELY(obj_array[1].is_null())) {
    ret = OB_ERR_INVALID_ARGUMENT_FOR_WIDTH_BUCKET;
    LOG_USER_ERROR(OB_ERR_INVALID_ARGUMENT_FOR_WIDTH_BUCKET, "2");
  } else if (OB_UNLIKELY(obj_array[2].is_null())) {
    ret = OB_ERR_INVALID_ARGUMENT_FOR_WIDTH_BUCKET;
    LOG_USER_ERROR(OB_ERR_INVALID_ARGUMENT_FOR_WIDTH_BUCKET, "3");
  } else if (OB_UNLIKELY(obj_array[3].is_null())) {
    ret = OB_ERR_INVALID_ARGUMENT_FOR_WIDTH_BUCKET;
    LOG_USER_ERROR(OB_ERR_INVALID_ARGUMENT_FOR_WIDTH_BUCKET, "4");
  } else {
    number::ObNumber result;
    number::ObNumber expr;
    number::ObNumber start;
    number::ObNumber end;
    number::ObNumber bucket = obj_array[3].get_number();
    const ObObjType target_type = obj_array[0].get_type();
    if (ObNumberType == target_type) {
      expr = obj_array[0].get_number();
      start = obj_array[1].get_number();
      end = obj_array[2].get_number();
    } else {
      const int64_t time_usec1 = obj_array[0].get_datetime();
      const int64_t time_usec2 = obj_array[1].get_datetime();
      const int64_t time_usec3 = obj_array[2].get_datetime();
      if (ObDateTimeType == target_type) {
        if (OB_FAIL(expr.from(time_usec1, *expr_ctx.calc_buf_))) {
          LOG_WARN("expr number from int64 failed", K(expr), K(time_usec1), K(ret));
        } else if (OB_FAIL(start.from(time_usec2, *expr_ctx.calc_buf_))) {
          LOG_WARN("start number from int64 failed", K(start), K(time_usec2), K(ret));
        } else if (OB_FAIL(end.from(time_usec3, *expr_ctx.calc_buf_))) {
          LOG_WARN("end number from int64 failed", K(end), K(time_usec3), K(ret));
        }
      } else {
        const int16_t time_nsec1 = obj_array[0].get_tz_desc().tail_nsec_;
        const int16_t time_nsec2 = obj_array[1].get_tz_desc().tail_nsec_;
        const int16_t time_nsec3 = obj_array[2].get_tz_desc().tail_nsec_;
        if (OB_FAIL(calc_time_type(time_usec1,
                time_usec2,
                time_usec3,
                time_nsec1,
                time_nsec2,
                time_nsec3,
                target_type,
                *expr_ctx.calc_buf_,
                expr,
                start,
                end))) {
          LOG_WARN("calc_time_type faied", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(expr.width_bucket(start, end, bucket, result, *(expr_ctx.calc_buf_)))) {
        LOG_WARN("call width_bucket failed", K(ret));
      } else {
        res.set_number(result);
      }
    }
  }
  return ret;
}

int ObExprWidthBucket::calc_time_type(const int64_t time_usec1, const int64_t time_usec2, const int64_t time_usec3,
    const int16_t time_nsec1, const int16_t time_nsec2, const int16_t time_nsec3, const ObObjType& target_type,
    ObIAllocator& alloc, number::ObNumber& e_nmb, number::ObNumber& start_nmb, number::ObNumber& end_nmb)
{
  int ret = OB_SUCCESS;
  if (ObTimestampNanoType == target_type || ObTimestampLTZType == target_type) {
    number::ObNumber time_usec1_nmb;
    number::ObNumber time_usec2_nmb;
    number::ObNumber time_usec3_nmb;
    number::ObNumber time_nsec1_nmb;
    number::ObNumber time_nsec2_nmb;
    number::ObNumber time_nsec3_nmb;
    number::ObNumber thousand_nmb;
    if (OB_FAIL(time_usec1_nmb.from(time_usec1, alloc))) {
      LOG_WARN("time_usec1_nmb from const int64 failed", K(ret));
    } else if (OB_FAIL(time_usec2_nmb.from(time_usec2, alloc))) {
      LOG_WARN("time_usec2_nmb from const int64 failed", K(ret));
    } else if (OB_FAIL(time_usec3_nmb.from(time_usec3, alloc))) {
      LOG_WARN("time_usec3_nmb from const int64 failed", K(ret));
    } else if (OB_FAIL(time_nsec1_nmb.from(static_cast<int64_t>(time_nsec1), alloc))) {
      LOG_WARN("time_nsec1_nmb from const int64 failed", K(ret));
    } else if (OB_FAIL(time_nsec2_nmb.from(static_cast<int64_t>(time_nsec2), alloc))) {
      LOG_WARN("time_nsec2_nmb from const int64 failed", K(ret));
    } else if (OB_FAIL(time_nsec3_nmb.from(static_cast<int64_t>(time_nsec3), alloc))) {
      LOG_WARN("time_nsec3_nmb from const int64 failed", K(ret));
    } else if (OB_FAIL(thousand_nmb.from("1000", alloc))) {
      LOG_WARN("create obnumber from const int64 1000 failed", K(ret));
    } else if (OB_FAIL(time_usec1_nmb.mul(thousand_nmb, time_usec1_nmb, alloc))) {
      LOG_WARN("time_usec1_nmb mul 1000 failed", K(ret));
    } else if (OB_FAIL(time_usec1_nmb.add(time_nsec1_nmb, e_nmb, alloc))) {
      LOG_WARN("time_usec1_nmb add time_nsec1_nmb failed", K(ret));
    } else if (OB_FAIL(time_usec2_nmb.mul(thousand_nmb, time_usec2_nmb, alloc))) {
      LOG_WARN("time_usec2_nmb mul 1000 failed", K(ret));
    } else if (OB_FAIL(time_usec2_nmb.add(time_nsec2_nmb, start_nmb, alloc))) {
      LOG_WARN("time_usec2_nmb add time_nsec2_nmb failed", K(ret));
    } else if (OB_FAIL(time_usec3_nmb.mul(thousand_nmb, time_usec3_nmb, alloc))) {
      LOG_WARN("time_usec3_nmb mul 1000 failed", K(ret));
    } else if (OB_FAIL(time_usec3_nmb.add(time_nsec3_nmb, end_nmb, alloc))) {
      LOG_WARN("time_usec3_nmb add time_nsec3_nmb failed", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(ret), K(target_type));
  }
  return ret;
}

int ObExprWidthBucket::calc_width_bucket_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res)
{
  int ret = OB_SUCCESS;
  // width_bucket(e, start, end, bucket);
  ObDatum* e = NULL;
  ObDatum* start = NULL;
  ObDatum* end = NULL;
  ObDatum* bucket = NULL;
  if (OB_UNLIKELY(4 != expr.arg_cnt_)) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, e, start, end, bucket))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (e->is_null()) {
    res.set_null();
  } else if (OB_UNLIKELY(start->is_null())) {
    ret = OB_ERR_INVALID_ARGUMENT_FOR_WIDTH_BUCKET;
    LOG_USER_ERROR(OB_ERR_INVALID_ARGUMENT_FOR_WIDTH_BUCKET, "2");
  } else if (OB_UNLIKELY(end->is_null())) {
    ret = OB_ERR_INVALID_ARGUMENT_FOR_WIDTH_BUCKET;
    LOG_USER_ERROR(OB_ERR_INVALID_ARGUMENT_FOR_WIDTH_BUCKET, "3");
  } else if (OB_UNLIKELY(bucket->is_null())) {
    ret = OB_ERR_INVALID_ARGUMENT_FOR_WIDTH_BUCKET;
    LOG_USER_ERROR(OB_ERR_INVALID_ARGUMENT_FOR_WIDTH_BUCKET, "4");
  } else {
    number::ObNumber res_nmb;
    number::ObNumber e_nmb;
    number::ObNumber start_nmb;
    number::ObNumber end_nmb;
    number::ObNumber bucket_nmb(bucket->get_number());
    const ObObjType target_type = expr.args_[0]->datum_meta_.type_;
    ObIAllocator& calc_alloc = ctx.get_reset_tmp_alloc();
    if (ObNumberType == target_type) {
      e_nmb = number::ObNumber(e->get_number());
      start_nmb = number::ObNumber(start->get_number());
      end_nmb = number::ObNumber(end->get_number());
    } else if (ObDateTimeType == target_type) {
      const int64_t time_usec1 = e->get_datetime();
      const int64_t time_usec2 = start->get_datetime();
      const int64_t time_usec3 = end->get_datetime();
      if (OB_FAIL(e_nmb.from(time_usec1, calc_alloc))) {
        LOG_WARN("expr number from int64 failed", K(e_nmb), K(time_usec1), K(ret));
      } else if (OB_FAIL(start_nmb.from(time_usec2, calc_alloc))) {
        LOG_WARN("start number from int64 failed", K(start_nmb), K(time_usec2), K(ret));
      } else if (OB_FAIL(end_nmb.from(time_usec3, calc_alloc))) {
        LOG_WARN("end number from int64 failed", K(end_nmb), K(time_usec3), K(ret));
      }
    } else if (ObTimestampNanoType == target_type || ObTimestampLTZType == target_type) {
      const int64_t time_usec1 = e->get_otimestamp_tiny().time_us_;
      const int64_t time_usec2 = start->get_otimestamp_tiny().time_us_;
      const int64_t time_usec3 = end->get_otimestamp_tiny().time_us_;
      const int16_t time_nsec1 = e->get_otimestamp_tiny().time_ctx_.tail_nsec_;
      const int16_t time_nsec2 = start->get_otimestamp_tiny().time_ctx_.tail_nsec_;
      const int16_t time_nsec3 = end->get_otimestamp_tiny().time_ctx_.tail_nsec_;
      if (OB_FAIL(calc_time_type(time_usec1,
              time_usec2,
              time_usec3,
              time_nsec1,
              time_nsec2,
              time_nsec3,
              target_type,
              calc_alloc,
              e_nmb,
              start_nmb,
              end_nmb))) {
        LOG_WARN("calc_time_type faied", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      number::ObNumber res_nmb;
      ObNumStackOnceAlloc res_alloc;
      if (OB_FAIL(e_nmb.width_bucket(start_nmb, end_nmb, bucket_nmb, res_nmb, res_alloc))) {
        LOG_WARN("call width_bucket failed", K(ret));
      } else {
        res.set_number(res_nmb);
      }
    }
  }
  return ret;
}

int ObExprWidthBucket::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_width_bucket_expr;
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
