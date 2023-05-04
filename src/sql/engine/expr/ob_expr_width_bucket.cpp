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

namespace oceanbase
{
namespace sql
{

ObExprWidthBucket::ObExprWidthBucket(common::ObIAllocator &alloc) :
    ObFuncExprOperator(alloc, T_FUN_SYS_WIDTH_BUCKET, N_WIDTH_BUCKET, 4, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprWidthBucket::calc_result_typeN(ObExprResType &type,
                                         ObExprResType *types,
                                         int64_t param_num,
                                         ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(types) || 4 != param_num)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(types), K(param_num), K(ret));
  } else if (OB_UNLIKELY(ObNullType == types[0].get_type())) {
    ret = OB_ERR_ARGUMENT_SHOULD_NUMERIC_DATE_DATETIME_TYPE;
    LOG_WARN("first argument should be of numeric or date/datetime type",K(ret));
  } else if (OB_LIKELY(NOT_ROW_DIMENSION == row_dimension_)) {
    type.set_type(ObNumberType);
    const ObAccuracy &acc =
            ObAccuracy::DDL_DEFAULT_ACCURACY2[common::ORACLE_MODE][common::ObNumberType];
    common::ObObjType calc_type = types[0].get_type();
    type.set_scale(acc.get_scale());
    type.set_precision(acc.get_precision());
    if (ob_is_numeric_type(calc_type)) {
      calc_type = ObNumberType;
    } else {
      switch (calc_type) {
        case ObDateTimeType: break;
        case ObTimestampNanoType: break;
        case ObIntervalYMType: break;
        case ObIntervalDSType: break;
        case ObTimestampTZType:
        case ObTimestampLTZType:
          calc_type = ObTimestampLTZType;
          break;
        default:
          ret = OB_ERR_ARGUMENT_SHOULD_NUMERIC_DATE_DATETIME_TYPE;
          LOG_WARN("first parameter must be number, date or timestamp type", K(ret), K(calc_type));
          break;
      }
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

int ObExprWidthBucket::construct_param_nmb(const int64_t expr_value,
                                          const int64_t start_value,
                                          const int64_t end_value,
                                          common::ObIAllocator &alloc,
                                          common::number::ObNumber &expr_nmb,
                                          common::number::ObNumber &start_nmb,
                                          common::number::ObNumber &end_nmb)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_nmb.from(expr_value, alloc))) {
    LOG_WARN("expr number from int64 failed", K(expr_value), K(ret));
  } else if (OB_FAIL(start_nmb.from(start_value, alloc))) {
    LOG_WARN("start number from int64 failed", K(start_value), K(ret));
  } else if (OB_FAIL(end_nmb.from(end_value, alloc))) {
    LOG_WARN("end number from int64 failed", K(end_value), K(ret));
  }
  return ret;
}

int ObExprWidthBucket::calc_time_type(const int64_t time_sec1, const int64_t time_frac_sec1,
                                      const int64_t time_sec2, const int64_t time_frac_sec2,
                                      const int64_t time_sec3, const int64_t time_frac_sec3,
                                      const int64_t multiple,
                                      ObIAllocator &alloc,
                                      number::ObNumber &e_nmb, number::ObNumber &start_nmb,
                                      number::ObNumber &end_nmb)
{
  int ret = OB_SUCCESS;
  number::ObNumber time_sec1_nmb;
  number::ObNumber time_frac_sec1_nmb;
  number::ObNumber time_sec2_nmb;
  number::ObNumber time_frac_sec2_nmb;
  number::ObNumber time_sec3_nmb;
  number::ObNumber time_frac_sec3_nmb;

  number::ObNumber multiple_nmb;
  if (OB_FAIL(construct_param_nmb(time_sec1, time_sec2, time_sec3,
                                  alloc, time_sec1_nmb, time_sec2_nmb, time_sec3_nmb))) {
    LOG_WARN("construct param nmb failed", K(ret));
  } else if (OB_FAIL(construct_param_nmb(time_frac_sec1, time_frac_sec2, time_frac_sec3,
                          alloc, time_frac_sec1_nmb, time_frac_sec2_nmb, time_frac_sec3_nmb))) {
    LOG_WARN("construct param nmb failed", K(ret));
  } else if (OB_FAIL(multiple_nmb.from(multiple, alloc))) {
    LOG_WARN("create obnumber from const int64 failed", K(ret));
  } else if (OB_FAIL(time_sec1_nmb.mul(multiple_nmb, time_sec1_nmb, alloc))) {
    LOG_WARN("time_sec1_nmb mul 1000 failed", K(ret));
  } else if (OB_FAIL(time_sec1_nmb.add(time_frac_sec1_nmb, e_nmb, alloc))) {
    LOG_WARN("time_sec1_nmb add time_frac_sec1_nmb failed", K(ret));
  } else if (OB_FAIL(time_sec2_nmb.mul(multiple_nmb, time_sec2_nmb, alloc))) {
    LOG_WARN("time_sec2_nmb mul 1000 failed", K(ret));
  } else if (OB_FAIL(time_sec2_nmb.add(time_frac_sec2_nmb, start_nmb, alloc))) {
    LOG_WARN("time_sec2_nmb add time_frac_sec2_nmb failed", K(ret));
  } else if (OB_FAIL(time_sec3_nmb.mul(multiple_nmb, time_sec3_nmb, alloc))) {
    LOG_WARN("time_sec3_nmb mul 1000 failed", K(ret));
  } else if (OB_FAIL(time_sec3_nmb.add(time_frac_sec3_nmb, end_nmb, alloc))) {
    LOG_WARN("time_sec3_nmb add time_frac_sec3_nmb failed", K(ret));
  }
  return ret;
}

int ObExprWidthBucket::calc_width_bucket_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                              ObDatum &res)
{
  int ret = OB_SUCCESS;
  // width_bucket(e, start, end, bucket);
  ObDatum *e = NULL;
  ObDatum *start = NULL;
  ObDatum *end = NULL;
  ObDatum *bucket = NULL;
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
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    if (ObNumberType == target_type) {
      e_nmb = number::ObNumber(e->get_number());
      start_nmb = number::ObNumber(start->get_number());
      end_nmb = number::ObNumber(end->get_number());
    } else if (ObDateTimeType == target_type) {
      if (OB_FAIL(construct_param_nmb(e->get_datetime(),
                                      start->get_datetime(),
                                      end->get_datetime(),
                                      calc_alloc, e_nmb, start_nmb, end_nmb))) {
        LOG_WARN("construct param number failed", K(ret));
      }
    } else if (ObTimestampNanoType == target_type || ObTimestampLTZType == target_type) {
      const int64_t time_usec1 = e->get_otimestamp_tiny().time_us_;
      const int64_t time_usec2 = start->get_otimestamp_tiny().time_us_;
      const int64_t time_usec3 = end->get_otimestamp_tiny().time_us_;
      const int16_t time_nsec1 = e->get_otimestamp_tiny().time_ctx_.tail_nsec_;
      const int16_t time_nsec2 = start->get_otimestamp_tiny().time_ctx_.tail_nsec_;
      const int16_t time_nsec3 = end->get_otimestamp_tiny().time_ctx_.tail_nsec_;
      if (OB_FAIL(calc_time_type(time_usec1, time_nsec1, time_usec2, time_nsec2,
                                 time_usec3, time_nsec3, NSECS_PER_USEC, calc_alloc,
                                 e_nmb, start_nmb, end_nmb))) {
        LOG_WARN("calc_time_type faied", K(ret));
      }
    } else if (ObIntervalYMType == target_type) {
      if (OB_FAIL(construct_param_nmb(e->get_interval_ym(),
                                      start->get_interval_ym(),
                                      end->get_interval_ym(),
                                      calc_alloc, e_nmb, start_nmb, end_nmb))) {
        LOG_WARN("construct param nmb failed", K(ret));
      }
    } else if (ObIntervalDSType == target_type) {
      ObIntervalDSValue ds_value1 = e->get_interval_ds();
      ObIntervalDSValue ds_value2 = start->get_interval_ds();
      ObIntervalDSValue ds_value3 = end->get_interval_ds();
      if (OB_FAIL(calc_time_type(ds_value1.nsecond_, ds_value1.fractional_second_,
                                  ds_value2.nsecond_, ds_value2.fractional_second_,
                                  ds_value3.nsecond_, ds_value3.fractional_second_,
                                  NSECS_PER_SEC, calc_alloc,
                                  e_nmb, start_nmb, end_nmb))) {
        LOG_WARN("calc_time_type faied", K(ret));
      }
    } else {

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

int ObExprWidthBucket::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_width_bucket_expr;
  return ret;
}
} // namespace sql
} // namespace oceanbase
