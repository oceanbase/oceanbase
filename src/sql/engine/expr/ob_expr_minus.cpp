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

#include "sql/engine/expr/ob_expr_minus.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_macro_utils.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/ob_sql_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/engine/expr/ob_batch_eval_util.h"
#include "sql/engine/expr/ob_rt_datum_arith.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/engine/expr/ob_expr_util.h"

namespace oceanbase
{
using namespace common;
using namespace common::number;
using namespace oceanbase::lib;

namespace sql
{

ObExprMinus::ObExprMinus(ObIAllocator &alloc, ObExprOperatorType type)
  : ObArithExprOperator(alloc,
                        type,
                        N_MINUS,
                        2,
                        NOT_ROW_DIMENSION,
                        ObExprResultTypeUtil::get_minus_result_type,
                        ObExprResultTypeUtil::get_minus_calc_type,
                        minus_funcs_)
{
  param_lazy_eval_ = lib::is_oracle_mode();
}

int ObExprMinus::calc_result_type2(ObExprResType &type,
                                   ObExprResType &type1,
                                   ObExprResType &type2,
                                   ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  static const int64_t CARRY_OFFSET = 1;
  ObScale scale = SCALE_UNKNOWN_YET;
  ObPrecision precision = PRECISION_UNKNOWN_YET;
  const ObSQLSessionInfo *session = nullptr;
  bool is_oracle = lib::is_oracle_mode();
  const bool is_all_decint_args =
    ob_is_decimal_int(type1.get_type()) && ob_is_decimal_int(type2.get_type());
  if (OB_ISNULL(session = type_ctx.get_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get mysession", K(ret));
  } else if (OB_FAIL(ObArithExprOperator::calc_result_type2(type, type1, type2, type_ctx))) {
    LOG_WARN("fail to calc result type", K(ret), K(type), K(type1), K(type2));
  } else if (type.is_decimal_int() && (type1.is_null() || type2.is_null())) {
    type.set_precision(MAX(type1.get_precision(), type2.get_precision()));
    type.set_scale(MAX(type1.get_scale(), type2.get_scale()));
  } else if (OB_UNLIKELY(SCALE_UNKNOWN_YET == type1.get_scale() ||
                         SCALE_UNKNOWN_YET == type2.get_scale())) {
    type.set_scale(NUMBER_SCALE_UNKNOWN_YET);
    type.set_precision(PRECISION_UNKNOWN_YET);
  } else if (type1.get_type_class() == ObIntervalTC
             || type2.get_type_class() == ObIntervalTC) {
    type.set_scale(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][type.get_type()].get_scale());
    type.set_precision(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][type.get_type()].get_precision());
  } else if (ob_is_oracle_datetime_tc(type1.get_type())
             && ob_is_oracle_datetime_tc(type2.get_type())
             && type.get_type_class() == ObIntervalTC) {
    type.set_scale(ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(
                    MAX_SCALE_FOR_ORACLE_TEMPORAL, std::max(type1.get_scale(), type2.get_scale())));
    type.set_precision(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][type.get_type()].get_precision());
  } else {
    if (OB_UNLIKELY(is_oracle && type.is_datetime())) {
      scale = OB_MAX_DATE_PRECISION;
    } else {
      ObScale scale1 = static_cast<ObScale>(MAX(type1.get_scale(), 0));
      ObScale scale2 = static_cast<ObScale>(MAX(type2.get_scale(), 0));
      scale = MAX(scale1, scale2);
      if (lib::is_mysql_mode() && type.is_double()) {
        precision = ObMySQLUtil::float_length(scale);
      } else if (type.has_result_flag(DECIMAL_INT_ADJUST_FLAG)) {
        precision = MAX(type1.get_precision(), type2.get_precision());
      } else {
        int64_t inter_part_length1 = type1.get_precision() - type1.get_scale();
        int64_t inter_part_length2 = type2.get_precision() - type2.get_scale();
        precision = static_cast<ObPrecision>(MAX(inter_part_length1, inter_part_length2)
                                            + CARRY_OFFSET + scale);
      }
    }
    type.set_scale(scale);

    if (OB_UNLIKELY(PRECISION_UNKNOWN_YET == type1.get_precision()) ||
        OB_UNLIKELY(PRECISION_UNKNOWN_YET == type2.get_precision())) {
      type.set_precision(PRECISION_UNKNOWN_YET);
    } else {
      type.set_precision(precision);
    }
    if (lib::is_mysql_mode() && is_no_unsigned_subtraction(session->get_sql_mode())) {
      ObObjType convert_type = type.get_type();
      convert_unsigned_type_to_signed(convert_type);
      type.set_type(convert_type);
    }
    if (is_all_decint_args || type.is_decimal_int()) {
      if (OB_UNLIKELY(PRECISION_UNKNOWN_YET == type.get_precision() ||
                      SCALE_UNKNOWN_YET == type.get_scale())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected decimal int precision and scale", K(ret), K(type));
      } else if (is_oracle && type.get_precision() > OB_MAX_NUMBER_PRECISION) {
        type1.set_calc_type(ObNumberType);
        type2.set_calc_type(ObNumberType);
        type.set_number();
      } else {
        if (ObRawExprUtils::decimal_int_need_cast(type1.get_accuracy(), type.get_accuracy()) ||
              ObRawExprUtils::decimal_int_need_cast(type2.get_accuracy(), type.get_accuracy())) {
          type.set_result_flag(DECIMAL_INT_ADJUST_FLAG);
        }
        type1.set_calc_accuracy(type.get_accuracy());
        type2.set_calc_accuracy(type.get_accuracy());
      }
      LOG_DEBUG("calc_result_type2", K(type.get_accuracy()), K(type1.get_accuracy()),
                                     K(type2.get_accuracy()));
    }
    // reset PS to unknown for oracle number type
    if (OB_SUCC(ret) && is_oracle && type.is_oracle_decimal() && !type.is_decimal_int()) {
      type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
      type.set_precision(PRECISION_UNKNOWN_YET);
    }
    if ((ob_is_double_tc(type.get_type()) || ob_is_float_tc(type.get_type())) && type.get_scale() > 0) {
      // if result is fixed double/float, calc type's of params should also be fixed double/float
      if (ob_is_double_tc(type1.get_calc_type()) || ob_is_float_tc(type1.get_calc_type())) {
        type1.set_calc_scale(type.get_scale());
      }
      if (ob_is_double_tc(type2.get_calc_type()) || ob_is_float_tc(type2.get_calc_type())) {
        type2.set_calc_scale(type.get_scale());
      }
    }
    LOG_DEBUG("calc_result_type2", K(scale), K(type1), K(type2), K(type), K(precision));
  }
  return ret;
}

int ObExprMinus::calc_datetime_minus(common::ObObj &result,
                                     const common::ObObj &left,
                                     const common::ObObj &right,
                                     common::ObExprCtx &expr_ctx,
                                     common::ObScale calc_scale)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("calc_buf_ should not be null", K(ret), K(expr_ctx.calc_buf_));
  } else {
    const ObObj *res_left = &left;
    const ObObj *res_right = &right;
    ObObj tmp_left_obj;
    ObObj tmp_right_obj;
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NO_CAST_INT_UINT);
    if (OB_FAIL(ObObjCaster::to_type(ObDateTimeType,
                                     cast_ctx,
                                     left,
                                     tmp_left_obj,
                                     res_left))) {
      LOG_WARN("cast failed.", K(ret), K(left));
    } else if (OB_FAIL(ObObjCaster::to_type(ObDateTimeType,
                                            cast_ctx,
                                            right,
                                            tmp_right_obj,
                                            res_right))) {
      LOG_WARN("cast failed.", K(ret), K(right));
    } else {
      const int64_t LOCAL_BUF_SIZE = ObNumber::MAX_CALC_BYTE_LEN * 5;
      char local_buf[LOCAL_BUF_SIZE];
      ObDataBuffer local_alloc(local_buf, LOCAL_BUF_SIZE);
      ObNumber left_datetime;
      ObNumber right_datetime;
      ObNumber usecs_per_day;
      ObNumber left_date;
      ObNumber right_date;
      if (OB_FAIL(left_datetime.from(res_left->get_datetime(), local_alloc))) {
        LOG_WARN("convert int64 to number failed", K(ret), K(res_left));
      } else if (OB_FAIL(right_datetime.from(res_right->get_datetime(), local_alloc))) {
        LOG_WARN("convert int64 to number failed", K(ret), K(res_right));
      } else if (OB_FAIL(usecs_per_day.from(USECS_PER_DAY, local_alloc))) {
        LOG_WARN("convert int64 to number failed", K(ret));
      } else if (OB_FAIL(left_datetime.div_v3(usecs_per_day, left_date, local_alloc,
                                              ObNumber::OB_MAX_DECIMAL_DIGIT, false))) {
        LOG_WARN("calc left date number failed", K(ret));
      } else if (OB_FAIL(right_datetime.div_v3(usecs_per_day, right_date, local_alloc,
                                              ObNumber::OB_MAX_DECIMAL_DIGIT, false))) {
        LOG_WARN("calc left date number failed", K(ret));
      } else if (FALSE_IT(tmp_left_obj.set_number(left_date))) {
      } else if (FALSE_IT(tmp_right_obj.set_number(right_date))) {
      } else if (OB_FAIL(minus_number(result, tmp_left_obj, tmp_right_obj, expr_ctx.calc_buf_, calc_scale))) {
        LOG_WARN("minus_number failed.", K(ret), K(tmp_left_obj), K(tmp_right_obj));
      } else {
        LOG_DEBUG("succ to calc_datetime_minus", K(ret), K(result), K(left), K(right),
                  K(left_datetime), K(right_datetime), "left_double", res_left->get_datetime(),
                  K(left_date), K(right_date), K(tmp_left_obj), K(tmp_right_obj));
      }
    }
  }
  return ret;
}

int ObExprMinus::calc_timestamp_minus(common::ObObj &result,
                                      const common::ObObj &left,
                                      const common::ObObj &right,
                                      const ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;
  ObIntervalDSValue res_interval;
  ObOTimestampData left_v;
  ObOTimestampData right_v;

  if (OB_UNLIKELY(!ob_is_oracle_datetime_tc(left.get_type())
                  || !ob_is_oracle_datetime_tc(right.get_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (left.is_datetime()) {
      ret = ObTimeConverter::odate_to_otimestamp(left.get_datetime(), tz_info, ObTimestampTZType, left_v);
    } else if (left.is_timestamp_nano()) {
      ret = ObTimeConverter::odate_to_otimestamp(left.get_otimestamp_value().time_us_, tz_info, ObTimestampTZType, left_v);
      left_v.time_ctx_.tail_nsec_ = left.get_otimestamp_value().time_ctx_.tail_nsec_;
    } else {
      ret = ObTimeConverter::otimestamp_to_otimestamp(left.get_type(), left.get_otimestamp_value(), tz_info, ObTimestampTZType, left_v);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to convert left to timestamp tz", K(ret), K(left));
    }
  }

  if (OB_SUCC(ret)) {
    if (right.is_datetime()) {
      ret = ObTimeConverter::odate_to_otimestamp(right.get_datetime(), tz_info, ObTimestampTZType, right_v);
    } else if (right.is_timestamp_nano()) {
      ret = ObTimeConverter::odate_to_otimestamp(right.get_otimestamp_value().time_us_, tz_info, ObTimestampTZType, right_v);
      right_v.time_ctx_.tail_nsec_ = right.get_otimestamp_value().time_ctx_.tail_nsec_;
    } else {
      ret = ObTimeConverter::otimestamp_to_otimestamp(right.get_type(), right.get_otimestamp_value(), tz_info, ObTimestampTZType, right_v);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to convert right to timestamp tz", K(ret), K(right));
    }
  }

  if (OB_SUCC(ret)) {
    ObTimeConverter::calc_oracle_temporal_minus(left_v, right_v, res_interval);
    ObScale res_scale = ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(
          MAX_SCALE_FOR_ORACLE_TEMPORAL,
          std::max(left.get_scale(), right.get_scale())
          );
    result.set_interval_ds(res_interval);
    result.set_scale(res_scale);
  }

  LOG_DEBUG("succ to calc_timestamp_minus", K(ret), K(result),
            K(left_v), K(right_v), K(left), K(right), K(res_interval));

  return ret;
}

int ObExprMinus::calc(ObDatum &res, const ObDatum &left, const ObDatum &right,
                      const ObDatumMeta &left_meta, const ObDatumMeta &right_meta,
                      ObIAllocator *allocator, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null allocator", K(ret));
  } else {
    SMART_VARS_2((ObSQLSessionInfo, default_session), (ObExecContext, exec_ctx, *allocator)) {
      const ObTenantSchema *tenant_schema = NULL;
      ObSchemaGetterGuard schema_guard;

      if (OB_FAIL(default_session.init(0, 0, allocator))) {
        LOG_WARN("init empty session failed", K(ret));
      } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("get schema guard failed", K(ret));
      } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
        LOG_WARN("get tenant_schema failed", K(ret));
      } else if (OB_FAIL(default_session.init_tenant(tenant_schema->get_tenant_name_str(), tenant_id))) {
        LOG_WARN("init tenant failed", K(ret));
      } else if (OB_FAIL(default_session.load_all_sys_vars(schema_guard))) {
        LOG_WARN("session load system variable failed", K(ret));
      } else if (OB_FAIL(default_session.load_default_configs_in_pc())) {
        LOG_WARN("session load default configs failed", K(ret));
      } else {
        // ObExecContext exec_ctx(*allocator);
        ObPhysicalPlanCtx plan_ctx(*allocator);
        ObRTDatumArith *arith = NULL;
        OZ (plan_ctx.init_datum_param_store());
        OX (exec_ctx.set_physical_plan_ctx(&plan_ctx));
        OX (exec_ctx.set_my_session(&default_session));
        OX (exec_ctx.set_mem_attr(ObMemAttr(tenant_id,
                                            ObModIds::OB_SQL_EXEC_CONTEXT,
                                            ObCtxIds::EXECUTE_CTX_ID)));
        OX (arith = OB_NEWx(ObRTDatumArith, allocator, exec_ctx, default_session));
        if (OB_FAIL(ret)) {
          if (NULL != arith) {
            arith->~ObRTDatumArith();
            arith = NULL;
          }
        } else if (NULL == arith) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else if (OB_FAIL(arith->setup_datum_metas(left_meta, right_meta))) {
          LOG_WARN("setup datum metas failed", K(ret));
        } else {
          auto left_item = arith->ref(0);
          auto right_item = arith->ref(1);
          if (OB_FAIL(arith->generate(left_item - right_item))) {
            LOG_WARN("generate arithmetic expression failed", K(ret));
          } else if (NULL == arith->get_expr()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("no runtime expr generated", K(ret));
          }
          if (OB_SUCC(ret)) {
            ObDatum *eval_res = NULL;
            if (OB_FAIL(arith->eval(eval_res, left, right))) {
              LOG_WARN("runtime datum arithmetic evaluate failed", K(ret));
            } else if (OB_ISNULL(eval_res)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("eval result is NULL", K(ret));
            } else if (OB_FAIL(res.deep_copy(*eval_res, *allocator))){
              LOG_WARN("failed to deep copy", K(ret));
            }
          }
        }
        if (NULL != arith) {
          arith->~ObRTDatumArith();
        }
      }
    }
  }
  return ret;
}

ObArithFunc ObExprMinus::minus_funcs_[ObMaxTC] =
{
  NULL,
  ObExprMinus::minus_int,
  ObExprMinus::minus_uint,
  ObExprMinus::minus_float,
  ObExprMinus::minus_double,
  ObExprMinus::minus_number,
  ObExprMinus::minus_datetime,//datetime
  NULL,//date
  NULL,//time
  NULL,//year
  NULL,//string
  NULL,//extend
  NULL,//unknown
  NULL,//text
  NULL,//bit
  NULL,//enumset
  NULL,//enumsetInner
};

ObArithFunc ObExprMinus::agg_minus_funcs_[ObMaxTC] =
{
  NULL,
  ObExprMinus::minus_int,
  ObExprMinus::minus_uint,
  ObExprMinus::minus_float,
  ObExprMinus::minus_double_no_overflow,
  ObExprMinus::minus_number,
  ObExprMinus::minus_datetime,//datetime
  NULL,//date
  NULL,//time
  NULL,//year
  NULL,//string
  NULL,//extend
  NULL,//unknown
  NULL,//text
  NULL,//bit
  NULL,//enumset
  NULL,//enumsetInner
};

int ObExprMinus::minus_int(ObObj &res,
                           const ObObj &left,
                           const ObObj &right,
                           ObIAllocator *allocator,
                           ObScale scale)
{
  int ret = OB_SUCCESS;
  int64_t left_i = left.get_int();
  int64_t right_i = right.get_int();
  char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
  int64_t pos = 0;
  if (left.get_type_class() == right.get_type_class()) {
    res.set_int(left_i - right_i);
    if (OB_UNLIKELY(is_int_int_out_of_range(left_i, right_i, res.get_int()))) {
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%ld - %ld)'",
                      left_i,
                      right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT", expr_str);
    }
  } else if (OB_UNLIKELY(ObUIntTC != right.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    res.set_uint64(left_i - right_i);
    if (OB_UNLIKELY(is_int_uint_out_of_range(left_i, right_i, res.get_uint64()))) {
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%ld - %lu)'",
                      left_i,
                      right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprMinus::minus_uint(ObObj &res,
                            const ObObj &left,
                            const ObObj &right,
                            ObIAllocator *allocator,
                            ObScale scale)
{
  int ret = OB_SUCCESS;
  uint64_t left_i = left.get_uint64();
  uint64_t right_i = right.get_uint64();
  res.set_uint64(left_i - right_i);
  char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
  int64_t pos = 0;
  if (left.get_type_class() == right.get_type_class()) {
    if (OB_UNLIKELY(is_uint_uint_out_of_range(left_i, right_i, res.get_uint64()))) {
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%lu - %lu)'",
                      left_i,
                      right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
  } else if (OB_UNLIKELY(ObIntTC != right.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    if (OB_UNLIKELY(is_uint_int_out_of_range(right_i, left_i, res.get_uint64()))) {
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%lu - %ld)'",
                      left_i,
                      right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprMinus::minus_float(ObObj &res,
                             const ObObj &left,
                             const ObObj &right,
                             ObIAllocator *allocator,
                             ObScale scale)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lib::is_oracle_mode())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only oracle mode arrive here", K(ret), K(left), K(right));
  } else if (OB_UNLIKELY(left.get_type_class() != right.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    float left_f = left.get_float();
    float right_f = right.get_float();
    res.set_float(left_f - right_f);
    if (OB_UNLIKELY(is_float_out_of_range(res.get_float()))
        && !lib::is_oracle_mode()) {
      ret = OB_OPERATE_OVERFLOW;
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%e - %e)'",
                      left_f,
                      right_f);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BINARY_FLOAT", expr_str);
      LOG_WARN("float out of range", K(res), K(left), K(right), K(res));
    }
  }
  LOG_DEBUG("succ to minus float", K(res), K(left), K(right));
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprMinus::minus_double(ObObj &res,
                              const ObObj &left,
                              const ObObj &right,
                              ObIAllocator *allocator,
                              ObScale scale)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(left.get_type_class() != right.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    double left_d = left.get_double();
    double right_d = right.get_double();
    res.set_double(left_d - right_d);
    if (OB_UNLIKELY(is_double_out_of_range(res.get_double()))
        && !lib::is_oracle_mode()) {
      ret = OB_OPERATE_OVERFLOW;
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%e - %e)'",
                      left_d,
                      right_d);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DOUBLE", expr_str);
      LOG_WARN("double out of range", K(res), K(left), K(right), K(res));
      res.set_null();
    }
    LOG_DEBUG("succ to minus double", K(res), K(left), K(right));
  }
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprMinus::minus_double_no_overflow(ObObj &res,
                                          const ObObj &left,
                                          const ObObj &right,
                                          ObIAllocator *,
                                          ObScale)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(left.get_type_class() != right.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid types", K(ret), K(left), K(right));
  } else {
    double left_d = left.get_double();
    double right_d = right.get_double();
    res.set_double(left_d - right_d);
  }
  return ret;
}

int ObExprMinus::minus_number(ObObj &res,
                              const ObObj &left,
                              const ObObj &right,
                              ObIAllocator *allocator,
                              ObScale scale)
{
  int ret = OB_SUCCESS;
  number::ObNumber res_nmb;
  if (OB_UNLIKELY(NULL == allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_FAIL(left.get_number().sub_v3(right.get_number(), res_nmb, *allocator))) {
    LOG_WARN("failed to sub numbers", K(ret), K(left), K(right));
  } else {
    if (ObUNumberType == res.get_type()) {
      res.set_unumber(res_nmb);
    } else {
      res.set_number(res_nmb);
    }
  }
  UNUSED(scale);
  return ret;
}

int ObExprMinus::minus_datetime(ObObj &res, const ObObj &left, const ObObj &right,
    ObIAllocator *allocator, ObScale scale)
{
  int ret = OB_SUCCESS;
  const int64_t left_i = left.get_datetime();
  const int64_t right_i = right.get_datetime();
  ObTime ob_time;
  if (OB_LIKELY(left.get_type_class() == right.get_type_class())) {
    int64_t round_value = left_i - right_i;
    ObTimeConverter::round_datetime(OB_MAX_DATE_PRECISION, round_value);
    res.set_datetime(round_value);
    res.set_scale(OB_MAX_DATE_PRECISION);
    if (OB_UNLIKELY(res.get_datetime() > DATETIME_MAX_VAL || res.get_datetime() < DATETIME_MIN_VAL)
        || (OB_FAIL(ObTimeConverter::datetime_to_ob_time(res.get_datetime(), NULL, ob_time)))
        || (OB_FAIL(ObTimeConverter::validate_oracle_date(ob_time)))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%ld - %ld)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DATE", expr_str);
    }
  }
  LOG_DEBUG("minus datetime", K(left), K(right), K(ob_time), K(res));
  UNUSED(allocator);
  UNUSED(scale);
  return ret;
}

int ObExprMinus::cg_expr(ObExprCGCtx &op_cg_ctx,
                         const ObRawExpr &raw_expr,
                         ObExpr &rt_expr) const
{
#define SET_MINUS_FUNC_PTR(v) \
  rt_expr.eval_func_ = ObExprMinus::v; \
  rt_expr.eval_batch_func_ = ObExprMinus::v##_batch;

  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  if (rt_expr.arg_cnt_ != 2 || OB_ISNULL(rt_expr.args_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count of children is not 2 or children is null", K(ret), K(rt_expr.arg_cnt_),
                                                            K(rt_expr.args_));
  } else if (OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret), K(rt_expr.args_[0]), K(rt_expr.args_[1]));
  } else {
    rt_expr.eval_func_ = NULL;
    rt_expr.may_not_need_raw_check_ = false;
    const ObObjType left_type = rt_expr.args_[0]->datum_meta_.type_;
    const ObObjType right_type = rt_expr.args_[1]->datum_meta_.type_;
    const ObObjType result_type = rt_expr.datum_meta_.type_;
    const ObObjTypeClass left_tc = ob_obj_type_class(left_type);
    const ObObjTypeClass right_tc = ob_obj_type_class(right_type);

    switch (result_type) {
      case ObIntType:
        rt_expr.may_not_need_raw_check_ = true;
        SET_MINUS_FUNC_PTR(minus_int_int);
        rt_expr.eval_vector_func_ = minus_int_int_vector;
        break;
      case ObUInt64Type:
        if (ObIntTC == left_tc && ObUIntTC == right_tc) {
          SET_MINUS_FUNC_PTR(minus_int_uint);
          rt_expr.eval_vector_func_ = minus_int_uint_vector;
        } else if (ObUIntTC == left_tc && ObIntTC == right_tc) {
          SET_MINUS_FUNC_PTR(minus_uint_int);
          rt_expr.eval_vector_func_ = minus_uint_int_vector;
        } else if (ObUIntTC == left_tc && ObUIntTC == right_tc) {
          SET_MINUS_FUNC_PTR(minus_uint_uint);
          rt_expr.eval_vector_func_ = minus_uint_uint_vector;
        }
        break;
      case ObIntervalYMType:
        SET_MINUS_FUNC_PTR(minus_intervalym_intervalym);
        break;
      case ObIntervalDSType:
        if (ObIntervalDSType == left_type) {
          SET_MINUS_FUNC_PTR(minus_intervalds_intervalds);
        } else {
          SET_MINUS_FUNC_PTR(minus_timestamp_timestamp);
        }
        break;
      case ObDateTimeType:
        if (ObIntervalYMType == right_type) {
          SET_MINUS_FUNC_PTR(minus_datetime_intervalym);
        } else if (ObIntervalDSType == right_type) {
          SET_MINUS_FUNC_PTR(minus_datetime_intervalds);
        } else if (ObDateTimeType == left_type && ObNumberType == right_type) {
          SET_MINUS_FUNC_PTR(minus_datetime_number);
        } else {
          SET_MINUS_FUNC_PTR(minus_datetime_datetime);
        }
        break;
      case ObTimestampTZType:
        if (ObIntervalYMType == right_type) {
          SET_MINUS_FUNC_PTR(minus_timestamptz_intervalym);
        } else if (ObIntervalDSType == right_type) {
          SET_MINUS_FUNC_PTR(minus_timestamptz_intervalds);
        }
        break;
      case ObTimestampLTZType:
        if (ObIntervalYMType == right_type) {
          SET_MINUS_FUNC_PTR(minus_timestampltz_intervalym);
        } else if (ObIntervalDSType == right_type) {
          SET_MINUS_FUNC_PTR(minus_timestamp_tiny_intervalds);
        }
        break;
      case ObTimestampNanoType:
        if (ObIntervalYMType == right_type) {
          SET_MINUS_FUNC_PTR(minus_timestampnano_intervalym);
        } else if (ObIntervalDSType == right_type) {
          SET_MINUS_FUNC_PTR(minus_timestamp_tiny_intervalds);
        }
        break;
      case ObFloatType:
        SET_MINUS_FUNC_PTR(minus_float_float);
        rt_expr.eval_vector_func_ = minus_float_float_vector;
        break;
      case ObDoubleType:
        SET_MINUS_FUNC_PTR(minus_double_double);
        rt_expr.eval_vector_func_ = minus_double_double_vector;
        break;
      case ObUNumberType:
      case ObNumberType:
        if (ObDateTimeType == left_type) {
          SET_MINUS_FUNC_PTR(minus_datetime_datetime_oracle);
        } else if (ob_is_decimal_int(left_type) && ob_is_decimal_int(right_type)) {
          switch (get_decimalint_type(rt_expr.args_[0]->datum_meta_.precision_)) {
            case DECIMAL_INT_32:
              SET_MINUS_FUNC_PTR(minus_decimalint32_oracle);
              rt_expr.eval_vector_func_ = minus_decimalint32_oracle_vector;
              break;
            case DECIMAL_INT_64:
              SET_MINUS_FUNC_PTR(minus_decimalint64_oracle);
              rt_expr.eval_vector_func_ = minus_decimalint64_oracle_vector;
              break;
            case DECIMAL_INT_128:
              SET_MINUS_FUNC_PTR(minus_decimalint128_oracle);
              rt_expr.eval_vector_func_ = minus_decimalint128_oracle_vector;
              break;
            default:
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected precision", K(ret), K(rt_expr.datum_meta_));
              break;
          }
        } else {
          SET_MINUS_FUNC_PTR(minus_number_number);
          rt_expr.eval_vector_func_ = minus_number_number_vector;
        }
        break;
      case ObDecimalIntType:
        switch (get_decimalint_type(rt_expr.datum_meta_.precision_)) {
          case DECIMAL_INT_32:
            SET_MINUS_FUNC_PTR(minus_decimalint32);
            rt_expr.eval_vector_func_ = minus_decimalint32_vector;
            break;
          case DECIMAL_INT_64:
            SET_MINUS_FUNC_PTR(minus_decimalint64);
            rt_expr.eval_vector_func_ = minus_decimalint64_vector;
            break;
          case DECIMAL_INT_128:
            SET_MINUS_FUNC_PTR(minus_decimalint128);
            rt_expr.eval_vector_func_ = minus_decimalint128_vector;
            break;
          case DECIMAL_INT_256:
            SET_MINUS_FUNC_PTR(minus_decimalint256);
            rt_expr.eval_vector_func_ = minus_decimalint256_vector;
            break;
          case DECIMAL_INT_512:
            if (rt_expr.datum_meta_.precision_ < OB_MAX_DECIMAL_POSSIBLE_PRECISION) {
              SET_MINUS_FUNC_PTR(minus_decimalint512);
              rt_expr.eval_vector_func_ = minus_decimalint512_vector;
            } else {
              SET_MINUS_FUNC_PTR(minus_decimalint512_with_check);
              rt_expr.eval_vector_func_ = minus_decimalint512_with_check_vector;
            }
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected precision", K(ret), K(rt_expr.datum_meta_));
            break;
        }
        break;
      default:
        break;
    }
    if (OB_ISNULL(rt_expr.eval_func_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected params type.", K(ret), K(left_type), K(right_type), K(result_type));
    }
  }
  return ret;
#undef SET_MINUS_FUNC_PTR
}

struct ObIntIntBatchMinusRaw : public ObArithOpRawType<int64_t, int64_t, int64_t>
{
  static void raw_op(int64_t &res, const int64_t l, const int64_t r)
  {
    res = l - r;
  }

  static int raw_check(const int64_t res, const int64_t l, const int64_t r)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(ObExprMinus::is_int_int_out_of_range(l, r, res))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%ld - %ld)'", l, r);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT", expr_str);
    }
    return ret;
  }
};

//calc_type is IntTC  left and right has same TC
int ObExprMinus::minus_int_int(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObArithOpWrap<ObIntIntBatchMinusRaw>>(EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_int_int_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op<ObArithOpWrap<ObIntIntBatchMinusRaw>>(BATCH_EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_int_int_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  return def_fixed_len_vector_arith_op<ObVectorArithOpWrap<ObIntIntBatchMinusRaw>>(VECTOR_EVAL_FUNC_ARG_LIST);
}


struct ObIntUIntBatchMinusRaw : public ObArithOpRawType<uint64_t, int64_t, uint64_t>
{
  static void raw_op(uint64_t &res, const int64_t l, const uint64_t r)
  {
    res = l - r;
  }

  static int raw_check(const uint64_t res, const int64_t l, const uint64_t r)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(ObExprMinus::is_int_uint_out_of_range(l, r, res))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%ld - %lu)'", l, r);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
    return ret;
  }
};

// calc_type/left_type is IntTC, right is ObUIntTC, only mysql mode
int ObExprMinus::minus_int_uint(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObArithOpWrap<ObIntUIntBatchMinusRaw>>(EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_int_uint_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op<ObArithOpWrap<ObIntUIntBatchMinusRaw>>(BATCH_EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_int_uint_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  return def_fixed_len_vector_arith_op<ObVectorArithOpWrap<ObIntUIntBatchMinusRaw>>(
    VECTOR_EVAL_FUNC_ARG_LIST);
}

struct ObUIntUIntBatchMinusRaw : public ObArithOpRawType<uint64_t, uint64_t, uint64_t>
{
  static void raw_op(uint64_t &res, const uint64_t l, const uint64_t r)
  {
    res = l - r;
  }

  static int raw_check(const uint64_t res, const uint64_t l, const uint64_t r)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(ObExprMinus::is_uint_uint_out_of_range(l, r, res))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%lu - %lu)'", l, r);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
    return ret;
  }
};

//calc_type is UIntTC  left and right has same TC
int ObExprMinus::minus_uint_uint(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObArithOpWrap<ObUIntUIntBatchMinusRaw>>(EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_uint_uint_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op<ObArithOpWrap<ObUIntUIntBatchMinusRaw>>(BATCH_EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_uint_uint_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  return def_fixed_len_vector_arith_op<ObVectorArithOpWrap<ObUIntUIntBatchMinusRaw>>(
    VECTOR_EVAL_FUNC_ARG_LIST);
}

struct ObUIntIntBatchMinusRaw : public ObArithOpRawType<uint64_t, uint64_t, int64_t>
{
  static void raw_op(uint64_t &res, const uint64_t l, const int64_t r)
  {
    res = l - r;
  }

  static int raw_check(const uint64_t res, const uint64_t l, const int64_t r)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(ObExprMinus::is_uint_int_out_of_range(r, l, res))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%lu - %ld)'", l, r);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BIGINT UNSIGNED", expr_str);
    }
    return ret;
  }
};

// calc_type/left_tpee is UIntTC , right is intTC. only mysql mode
int ObExprMinus::minus_uint_int(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObArithOpWrap<ObUIntIntBatchMinusRaw>>(EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_uint_int_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op<ObArithOpWrap<ObUIntIntBatchMinusRaw>>(BATCH_EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_uint_int_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  return def_fixed_len_vector_arith_op<ObVectorArithOpWrap<ObUIntIntBatchMinusRaw>>(
    VECTOR_EVAL_FUNC_ARG_LIST);
}

struct ObFloatBatchMinusRawNoCheck : public ObArithOpRawType<float, float, float>
{
  static void raw_op(float &res, const float l, const float r)
  {
    res = l - r;
  }

  static int raw_check(const float, const float, const float)
  {
    return OB_SUCCESS;
  }
};

struct ObFloatBatchMinusRawWithCheck: public ObFloatBatchMinusRawNoCheck
{
  static int raw_check(const float res, const float l, const float r)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(ObExprMinus::is_float_out_of_range(res))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%e - %e)'", l, r);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "FLOAT", expr_str);
      LOG_WARN("float out of range", K(l), K(r), K(res));
    }
    return ret;
  }
};

//calc type is floatTC, left and right has same TC, only oracle mode
int ObExprMinus::minus_float_float(EVAL_FUNC_ARG_DECL)
{
  return lib::is_oracle_mode()
      ? def_arith_eval_func<ObArithOpWrap<ObFloatBatchMinusRawNoCheck>>(EVAL_FUNC_ARG_LIST)
      : def_arith_eval_func<ObArithOpWrap<ObFloatBatchMinusRawWithCheck>>(EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_float_float_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return lib::is_oracle_mode()
      ? def_batch_arith_op<ObArithOpWrap<ObFloatBatchMinusRawNoCheck>>(BATCH_EVAL_FUNC_ARG_LIST)
      : def_batch_arith_op<ObArithOpWrap<ObFloatBatchMinusRawWithCheck>>(BATCH_EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_float_float_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  return lib::is_oracle_mode() ?
           def_fixed_len_vector_arith_op<ObVectorArithOpWrap<ObFloatBatchMinusRawNoCheck>>(
             VECTOR_EVAL_FUNC_ARG_LIST) :
           def_fixed_len_vector_arith_op<ObVectorArithOpWrap<ObFloatBatchMinusRawWithCheck>>(
             VECTOR_EVAL_FUNC_ARG_LIST);
}

struct ObDoubleBatchMinusRawNoCheck : public ObArithOpRawType<double, double, double>
{
  static void raw_op(double &res, const double l, const double r)
  {
    res = l - r;
  }

  static int raw_check(const double , const double , const double)
  {
    return OB_SUCCESS;
  }
};

struct ObDoubleBatchMinusRawWithCheck: public ObDoubleBatchMinusRawNoCheck
{
  static int raw_check(const double res, const double l, const double r)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(ObExprMinus::is_double_out_of_range(res))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%e - %e)'", l, r);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DOUBLE", expr_str);
      LOG_WARN("double out of range", K(l), K(r), K(res));
    }
    return ret;
  }
};

//calc type is doubleTC, left and right has same TC
int ObExprMinus::minus_double_double(EVAL_FUNC_ARG_DECL)
{
  return lib::is_oracle_mode() || T_OP_AGG_MINUS == expr.type_
      ? def_arith_eval_func<ObArithOpWrap<ObDoubleBatchMinusRawNoCheck>>(EVAL_FUNC_ARG_LIST)
      : def_arith_eval_func<ObArithOpWrap<ObDoubleBatchMinusRawWithCheck>>(EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_double_double_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return lib::is_oracle_mode() || T_OP_AGG_MINUS == expr.type_
      ? def_batch_arith_op<ObArithOpWrap<ObDoubleBatchMinusRawNoCheck>>(BATCH_EVAL_FUNC_ARG_LIST)
      : def_batch_arith_op<ObArithOpWrap<ObDoubleBatchMinusRawWithCheck>>(BATCH_EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_double_double_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  return lib::is_oracle_mode() || T_OP_AGG_MINUS == expr.type_ ?
           def_fixed_len_vector_arith_op<ObVectorArithOpWrap<ObDoubleBatchMinusRawNoCheck>>(
             VECTOR_EVAL_FUNC_ARG_LIST) :
           def_fixed_len_vector_arith_op<ObVectorArithOpWrap<ObDoubleBatchMinusRawWithCheck>>(
             VECTOR_EVAL_FUNC_ARG_LIST);
}

struct ObNumberMinusFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    int ret = OB_SUCCESS;
    char local_buff[ObNumber::MAX_BYTE_LEN];
    ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN);
    number::ObNumber l_num(l.get_number());
    number::ObNumber r_num(r.get_number());
    number::ObNumber res_num;
    if (OB_FAIL(l_num.sub_v3(r_num, res_num, local_alloc))) {
      LOG_WARN("minus num failed", K(ret), K(l_num), K(r_num));
    } else {
      res.set_number(res_num);
    }
    return ret;
  }
};

//calc type TC is ObNumberTC
int ObExprMinus::minus_number_number(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObNumberMinusFunc>(EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_number_number_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  LOG_DEBUG("minus_number_number_batch begin");
  int ret = OB_SUCCESS;
  ObDatumVector l_datums;
  ObDatumVector r_datums;
  const ObExpr &left = *expr.args_[0];
  const ObExpr &right = *expr.args_[1];

  if (OB_FAIL(binary_operand_batch_eval(expr, ctx, skip, size,
                                        lib::is_oracle_mode()))) {
    LOG_WARN("number minus batch evaluation failure", K(ret));
  } else {
    l_datums = left.locate_expr_datumvector(ctx);
    r_datums = right.locate_expr_datumvector(ctx);
  }

  if (OB_SUCC(ret)) {
    char local_buff[ObNumber::MAX_BYTE_LEN];
    ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN);
    ObDatumVector results = expr.locate_expr_datumvector(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

    for (auto i = 0; OB_SUCC(ret) && i < size; i++) {
      if (eval_flags.at(i) || skip.at(i)) {
        continue;
      }
      if (l_datums.at(i)->is_null() || r_datums.at(i)->is_null()) {
        results.at(i)->set_null();
        eval_flags.set(i);
        continue;
      }
      ObNumber l_num(l_datums.at(i)->get_number());
      ObNumber r_num(r_datums.at(i)->get_number());
      uint32_t *res_digits = const_cast<uint32_t *> (results.at(i)->get_number_digits());
      ObNumber::Desc &desc_buf = const_cast<ObNumber::Desc &> (results.at(i)->get_number_desc());
      // Notice that, space of desc_buf is allocated in frame but without memset operation, which causes random memory content.
      // And the reserved in storage layer should be 0, thus you must replacement new here to avoid checksum error, etc.
      ObNumber::Desc *res_desc = new (&desc_buf) ObNumberDesc();
      // speedup detection
      if (ObNumber::try_fast_minus(l_num, r_num, res_digits, *res_desc)) {
        results.at(i)->set_pack(sizeof(number::ObCompactNumber) +
                                res_desc->len_ * sizeof(*res_digits));
        eval_flags.set(i);
        // LOG_INFO("mul speedup done", K(l_num.format()), K(r_num.format()));
      } else {
        // normal path: no speedup
        ObNumber res_num;
        if (OB_FAIL(l_num.sub_v3(r_num, res_num, local_alloc))) {
          LOG_WARN("mul num failed", K(ret), K(l_num), K(r_num));
        } else {
          results.at(i)->set_number(res_num);
          eval_flags.set(i);
        }
        local_alloc.free();
      }
    }
  }
  LOG_DEBUG("minus_number_number_batch done");
  return ret;
}

struct NmbTryFastMinusOp
{
  OB_INLINE bool operator()(ObNumber &l_num, ObNumber &r_num, uint32_t *res_digit,
                            ObNumberDesc &res_desc)
  {
    return ObNumber::try_fast_minus(l_num, r_num, res_digit, res_desc);
  }
  OB_INLINE int operator()(const ObNumber &left, const ObNumber &right, ObNumber &value,
                           ObIAllocator &allocator)
  {
    return ObNumber::sub_v3(left, right, value, allocator);
  }
};

int ObExprMinus::minus_number_number_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  NmbTryFastMinusOp op;
  return def_number_vector_arith_op(VECTOR_EVAL_FUNC_ARG_LIST, op);
}

struct ObIntervalYMIntervalYMMinusFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    int ret = OB_SUCCESS;
    ObIntervalYMValue value = l.get_interval_nmonth() - r.get_interval_nmonth();
    if (OB_FAIL(value.validate())) {
      LOG_WARN("value validate failed", K(ret), K(value));
    } else {
      res.set_interval_nmonth(value.get_nmonth());
    }
    return ret;
  }
};

//interval can calc with different types such as date, timestamp, interval.
//the params do not need to do cast

//left and right have the same type. both IntervalYM.
int ObExprMinus::minus_intervalym_intervalym(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObIntervalYMIntervalYMMinusFunc>(EVAL_FUNC_ARG_LIST);
}
int ObExprMinus::minus_intervalym_intervalym_batch(BATCH_EVAL_FUNC_ARG_DECL)

{
  return def_batch_arith_op_by_datum_func<ObIntervalYMIntervalYMMinusFunc>(
      BATCH_EVAL_FUNC_ARG_LIST);
}

struct ObIntervalDSIntervalDSMinusFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    int ret = OB_SUCCESS;
    ObIntervalDSValue value = l.get_interval_ds() - r.get_interval_ds();
    if (OB_FAIL(value.validate())) {
      LOG_WARN("value validate failed", K(ret), K(value));
    } else {
      res.set_interval_ds(value);
    }
    return ret;
  }
};

//left and right must have the same type. both IntervalDS
int ObExprMinus::minus_intervalds_intervalds(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObIntervalDSIntervalDSMinusFunc>(EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_intervalds_intervalds_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObIntervalDSIntervalDSMinusFunc>(
      BATCH_EVAL_FUNC_ARG_LIST);
}

struct ObDatetimeIntervalYMMinusFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    int ret = OB_SUCCESS;
    int64_t result_v = 0;
    ret = ObTimeConverter::date_add_nmonth(l.get_datetime(),
                                           -r.get_interval_nmonth(),
                                           result_v);
    if (OB_FAIL(ret)) {
      LOG_WARN("minus value failed", K(ret), K(l), K(r));
    } else {
      res.set_datetime(result_v);
    }
    return ret;
  }
};


//Left is datetime TC. Right is intervalYM type.
int ObExprMinus::minus_datetime_intervalym(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObDatetimeIntervalYMMinusFunc>(EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_datetime_intervalym_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObDatetimeIntervalYMMinusFunc>(
      BATCH_EVAL_FUNC_ARG_LIST);
}

struct ObDatetimeIntervalDSMinusFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    int ret = OB_SUCCESS;
    int64_t result_v = 0;
    ret = ObTimeConverter::date_add_nsecond(l.get_datetime(),
                                            - r.get_interval_ds().get_nsecond(),
                                            - r.get_interval_ds().get_fs(),
                                            result_v);
    if (OB_FAIL(ret)) {
      LOG_WARN("minus value failed", K(ret), K(l), K(r));
    } else {
      res.set_datetime(result_v);
    }
    return ret;
  }
};

//Left is datetime TC. Right is intervalDS type.
int ObExprMinus::minus_datetime_intervalds(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObDatetimeIntervalDSMinusFunc>(EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_datetime_intervalds_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObDatetimeIntervalDSMinusFunc>(
      BATCH_EVAL_FUNC_ARG_LIST);
}

struct ObTimestampTZIntervalYMMinusFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, ObEvalCtx &ctx) const
  {
    int ret = OB_SUCCESS;
    ObOTimestampData result_v;
    ret = ObTimeConverter::otimestamp_add_nmonth(
        ObTimestampTZType,
        l.get_otimestamp_tz(),
        get_timezone_info(ctx.exec_ctx_.get_my_session()),
        - r.get_interval_nmonth(),
        result_v);
    if (OB_FAIL(ret)) {
      LOG_WARN("minus value failed", K(ret), K(l), K(r));
    } else {
      res.set_otimestamp_tz(result_v);
    }
    return ret;
  }
};

//Left is timestampTZ type. Right is intervalYM type.
int ObExprMinus::minus_timestamptz_intervalym(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObTimestampTZIntervalYMMinusFunc>(EVAL_FUNC_ARG_LIST, ctx);
}

int ObExprMinus::minus_timestamptz_intervalym_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObTimestampTZIntervalYMMinusFunc>(
      BATCH_EVAL_FUNC_ARG_LIST, ctx);
}

struct ObTimestampLTZIntervalYMMinusFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, ObEvalCtx &ctx) const
  {
    int ret = OB_SUCCESS;
    ObOTimestampData result_v;
    ret = ObTimeConverter::otimestamp_add_nmonth(
        ObTimestampLTZType,
        l.get_otimestamp_tiny(),
        get_timezone_info(ctx.exec_ctx_.get_my_session()),
        - r.get_interval_nmonth(),
        result_v);
    if (OB_FAIL(ret)) {
      LOG_WARN("minus value failed", K(ret), K(l), K(r));
    } else {
      res.set_otimestamp_tiny(result_v);
    }
    return ret;
  }
};

//Left is timestampLTZ type. Right is intervalYM type.
int ObExprMinus::minus_timestampltz_intervalym(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObTimestampLTZIntervalYMMinusFunc>(EVAL_FUNC_ARG_LIST, ctx);
}

int ObExprMinus::minus_timestampltz_intervalym_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObTimestampLTZIntervalYMMinusFunc>(
      BATCH_EVAL_FUNC_ARG_LIST, ctx);
}

struct ObTimestampNanoIntervalYMMinusFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, ObEvalCtx &ctx) const
  {
    int ret = OB_SUCCESS;
    ObOTimestampData result_v;
    ret = ObTimeConverter::otimestamp_add_nmonth(
        ObTimestampNanoType, l.get_otimestamp_tiny(),
        get_timezone_info(ctx.exec_ctx_.get_my_session()),
        - r.get_interval_nmonth(), result_v);

    if (OB_FAIL(ret)) {
      LOG_WARN("calc with timestamp value failed", K(ret), K(l), K(r));
    } else {
      res.set_otimestamp_tiny(result_v);
    }
    return ret;
  }
};

//Left is timestampNano type. Right is intervalYM type.
int ObExprMinus::minus_timestampnano_intervalym(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObTimestampNanoIntervalYMMinusFunc>(EVAL_FUNC_ARG_LIST, ctx);
}

int ObExprMinus::minus_timestampnano_intervalym_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObTimestampNanoIntervalYMMinusFunc>(
      BATCH_EVAL_FUNC_ARG_LIST, ctx);
}

struct ObTimestampTZIntervalDSMinusFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    int ret = OB_SUCCESS;
    ObOTimestampData result_v;
    ret = ObTimeConverter::otimestamp_add_nsecond(l.get_otimestamp_tz(),
                                                  - r.get_interval_ds().get_nsecond(),
                                                  - r.get_interval_ds().get_fs(),
                                                  result_v);
    if (OB_FAIL(ret)) {
      LOG_WARN("calc with timestamp value failed", K(ret), K(l), K(r));
    } else {
      res.set_otimestamp_tz(result_v);
    }
    return ret;
  }
};
//Left is timestamp TZ. Right is intervalDS type.
int ObExprMinus::minus_timestamptz_intervalds(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObTimestampTZIntervalDSMinusFunc>(EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_timestamptz_intervalds_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObTimestampTZIntervalDSMinusFunc>(
      BATCH_EVAL_FUNC_ARG_LIST);
}

struct ObTimestampLTZIntervalDSMinusFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    int ret = OB_SUCCESS;
    ObOTimestampData result_v;
    ret = ObTimeConverter::otimestamp_add_nsecond(l.get_otimestamp_tiny(),
                                                  - r.get_interval_ds().get_nsecond(),
                                                  - r.get_interval_ds().get_fs(),
                                                  result_v);
    if (OB_FAIL(ret)) {
      LOG_WARN("calc with timestamp value failed", K(ret), K(l), K(r));
    } else {
      res.set_otimestamp_tiny(result_v);
    }
    return ret;
  }
};

//Left is timestamp LTZ or Nano. Right is intervalDS type.
int ObExprMinus::minus_timestamp_tiny_intervalds(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObTimestampLTZIntervalDSMinusFunc>(EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_timestamp_tiny_intervalds_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObTimestampLTZIntervalDSMinusFunc>(
      BATCH_EVAL_FUNC_ARG_LIST);
}

struct ObTimestampTimestampMinusFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, ObEvalCtx &ctx,
                 const ObObjType &left_type, const ObObjType &right_type) const
  {
    int ret = OB_SUCCESS;
    ObOTimestampData left_v, right_v;
    ObIntervalDSValue result_intervalds;
    const ObTimeZoneInfo *tz_info = get_timezone_info(ctx.exec_ctx_.get_my_session());
    if (ObDateTimeType == left_type) {
      ret = ObTimeConverter::odate_to_otimestamp(l.get_datetime(), tz_info,
                                                 ObTimestampTZType, left_v);
    } else if (ObTimestampNanoType == left_type) {
      ret = ObTimeConverter::odate_to_otimestamp(l.get_otimestamp_tiny().time_us_,
                                                 tz_info, ObTimestampTZType, left_v);
      left_v.time_ctx_.tail_nsec_ = l.get_otimestamp_tiny().time_ctx_.tail_nsec_;
    } else if (ObTimestampTZType == left_type) {
      ret = ObTimeConverter::otimestamp_to_otimestamp(ObTimestampTZType,
                                                      l.get_otimestamp_tz(),
                                                      tz_info, ObTimestampTZType, left_v);
    } else {
      ret = ObTimeConverter::otimestamp_to_otimestamp(ObTimestampLTZType,
                                                      l.get_otimestamp_tiny(),
                                                      tz_info, ObTimestampTZType, left_v);

    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to convert left to timestamp tz", K(ret), K(l));
    }

    if (OB_SUCC(ret)) {
      if (ObDateTimeType == right_type) {
        ret = ObTimeConverter::odate_to_otimestamp(r.get_datetime(), tz_info,
                                                   ObTimestampTZType, right_v);
      } else if (ObTimestampNanoType == right_type) {
        ret = ObTimeConverter::odate_to_otimestamp(r.get_otimestamp_tiny().time_us_,
                                                   tz_info, ObTimestampTZType, right_v);
        right_v.time_ctx_.tail_nsec_ = r.get_otimestamp_tiny().time_ctx_.tail_nsec_;
      } else if (ObTimestampTZType == right_type) {
        ret = ObTimeConverter::otimestamp_to_otimestamp(ObTimestampTZType,
                                                        r.get_otimestamp_tz(), tz_info,
                                                        ObTimestampTZType, right_v);
      } else {
        ret = ObTimeConverter::otimestamp_to_otimestamp(ObTimestampLTZType,
                                                        r.get_otimestamp_tiny(), tz_info,
                                                        ObTimestampTZType, right_v);
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to convert right to timestamp tz", K(ret), K(r));
      }
    }

    if (OB_SUCC(ret)) {
      ObTimeConverter::calc_oracle_temporal_minus(left_v, right_v, result_intervalds);
      res.set_interval_ds(result_intervalds);
    }
    return ret;
  }
};

// only oracle mode.
//both left and right are datetimeTC or otimestampTC.
//cast left and right to ObTimestampTZType first.
int ObExprMinus::minus_timestamp_timestamp(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObTimestampTimestampMinusFunc>(
      EVAL_FUNC_ARG_LIST, ctx,
      expr.args_[0]->datum_meta_.type_, expr.args_[1]->datum_meta_.type_);
}

int ObExprMinus::minus_timestamp_timestamp_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObTimestampTimestampMinusFunc>(
      BATCH_EVAL_FUNC_ARG_LIST, ctx,
      expr.args_[0]->datum_meta_.type_, expr.args_[1]->datum_meta_.type_);
}

struct ObDatetimeNumberMinusFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    int ret = OB_SUCCESS;
    number::ObNumber right_nmb(r.get_number());
    const int64_t left_i = l.get_datetime();
    int64_t int_part = 0;
    int64_t dec_part = 0;
    if (!right_nmb.is_int_parts_valid_int64(int_part,dec_part)) {
      ret = OB_INVALID_DATE_FORMAT;
      LOG_WARN("invalid date format", K(ret), K(right_nmb));
    } else {
      const int64_t right_i = static_cast<int64_t>(int_part * USECS_PER_DAY)
          + (right_nmb.is_negative() ? -1  : 1 )
          * static_cast<int64_t>(static_cast<double>(dec_part)
                                 / NSECS_PER_SEC * static_cast<double>(USECS_PER_DAY));
      int64_t round_value = left_i - right_i;
      ObTimeConverter::round_datetime(OB_MAX_DATE_PRECISION, round_value);
      res.set_datetime(round_value);
      ObTime ob_time;
      if (OB_UNLIKELY(res.get_datetime() > DATETIME_MAX_VAL
                      || res.get_datetime() < DATETIME_MIN_VAL)
          || (OB_FAIL(ObTimeConverter::datetime_to_ob_time(res.get_datetime(), NULL, ob_time)))
          || (OB_FAIL(ObTimeConverter::validate_oracle_date(ob_time)))) {
        char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
        int64_t pos = 0;
        ret = OB_OPERATE_OVERFLOW;
        pos = 0;
        databuff_printf(expr_str,
                        OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                        pos,
                        "'(%ld - %ld)'", left_i, right_i);
        LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DATE", expr_str);
      }
    }
    return ret;
  }
};

int ObExprMinus::minus_datetime_number(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObDatetimeNumberMinusFunc>(EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_datetime_number_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObDatetimeNumberMinusFunc>(
      BATCH_EVAL_FUNC_ARG_LIST);
}

struct ObDatetimeDatetimeOralceMinusFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    int ret = OB_SUCCESS;
    const int64_t LOCAL_BUF_SIZE = ObNumber::MAX_CALC_BYTE_LEN * 5;
    char local_buf[LOCAL_BUF_SIZE];
    ObDataBuffer local_alloc(local_buf, LOCAL_BUF_SIZE);
    ObNumber left_datetime;
    ObNumber right_datetime;
    ObNumber usecs_per_day;
    ObNumber sub_datetime;
    ObNumber sub_date;
    if (OB_FAIL(left_datetime.from(l.get_datetime(), local_alloc))) {
      LOG_WARN("convert int64 to number failed", K(ret), K(l.get_datetime()));
    } else if (OB_FAIL(right_datetime.from(r.get_datetime(), local_alloc))) {
      LOG_WARN("convert int64 to number failed", K(ret), K(r.get_datetime()));
    } else if (OB_FAIL(usecs_per_day.from(USECS_PER_DAY, local_alloc))) {
      LOG_WARN("convert int64 to number failed", K(ret));
    } else if (OB_FAIL(left_datetime.sub_v3(right_datetime, sub_datetime, local_alloc))) {
      LOG_WARN("sub failed", K(ret), K(left_datetime), K(right_datetime));
    } else if (OB_FAIL(sub_datetime.div_v3(usecs_per_day, sub_date, local_alloc))) {
      LOG_WARN("calc left date number failed", K(ret));
    } else {
      res.set_number(sub_date);
    }
    return ret;
  }
};

//left and right are both ObDateTimeTC. calc_type is ObNumberType. only oracle mode.
int ObExprMinus::minus_datetime_datetime_oracle(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObDatetimeDatetimeOralceMinusFunc>(EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_datetime_datetime_oracle_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObDatetimeDatetimeOralceMinusFunc>(
      BATCH_EVAL_FUNC_ARG_LIST);
}

struct ObDatetimeDatetimeMinusFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    int ret = OB_SUCCESS;
    const int64_t left_i = l.get_datetime();
    const int64_t right_i = r.get_datetime();
    ObTime ob_time;
    int64_t round_value = left_i - right_i;
    ObTimeConverter::round_datetime(OB_MAX_DATE_PRECISION, round_value);
    res.set_datetime(round_value);
    if (OB_UNLIKELY(res.get_datetime() > DATETIME_MAX_VAL
        || res.get_datetime() < DATETIME_MIN_VAL)
        || (OB_FAIL(ObTimeConverter::datetime_to_ob_time(res.get_datetime(), NULL, ob_time)))
        || (OB_FAIL(ObTimeConverter::validate_oracle_date(ob_time)))) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      ret = OB_OPERATE_OVERFLOW;
      pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%ld - %ld)'", left_i, right_i);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DATE", expr_str);
    }
    return ret;
  }
};

//calc type is datetimeTC. cast left and right to calc_type.
int ObExprMinus::minus_datetime_datetime(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObDatetimeDatetimeMinusFunc>(EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_datetime_datetime_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op_by_datum_func<ObDatetimeDatetimeMinusFunc>(
      BATCH_EVAL_FUNC_ARG_LIST);
}

template<typename T>
struct ObDecimalIntBatchMinusRaw : public ObArithOpRawType<T, T, T>
{
  static void raw_op(T &res, const T &l, const T &r)
  {
    res = l - r;
  }

  static int raw_check(const T &res, const T &l, const T &r)
  {
    return OB_SUCCESS;
  }
};

struct ObDecimalIntBatchMinusRawWithCheck : public ObDecimalIntBatchMinusRaw<int512_t>
{
  static int raw_check(const int512_t &res, const int512_t &l, const int512_t &r)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(res <= wide::ObDecimalIntConstValue::MYSQL_DEC_INT_MIN
                    || res >= wide::ObDecimalIntConstValue::MYSQL_DEC_INT_MAX)) {
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      ret = OB_OPERATE_OVERFLOW;
      int64_t pos = 0;
      databuff_printf(expr_str, OB_MAX_TWO_OPERATOR_EXPR_LENGTH, pos, "");
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DECIMAL", expr_str);
      LOG_WARN("decimal int out of range", K(ret));
    }
    return ret;
  }
};

#define DECINC_MINUS_EVAL_FUNC_DECL(TYPE) \
int ObExprMinus::minus_decimal##TYPE(EVAL_FUNC_ARG_DECL)      \
{                                            \
  return def_arith_eval_func<ObArithOpWrap<ObDecimalIntBatchMinusRaw<TYPE##_t>>>(EVAL_FUNC_ARG_LIST); \
}                                            \
int ObExprMinus::minus_decimal##TYPE##_batch(BATCH_EVAL_FUNC_ARG_DECL)      \
{                                            \
  return def_batch_arith_op<ObArithOpWrap<ObDecimalIntBatchMinusRaw<TYPE##_t>>>(BATCH_EVAL_FUNC_ARG_LIST); \
}                                            \
int ObExprMinus::minus_decimal##TYPE##_vector(VECTOR_EVAL_FUNC_ARG_DECL)      \
{                                            \
  return def_fixed_len_vector_arith_op<ObVectorArithOpWrap<ObDecimalIntBatchMinusRaw<TYPE##_t>>>(VECTOR_EVAL_FUNC_ARG_LIST); \
}


DECINC_MINUS_EVAL_FUNC_DECL(int32)
DECINC_MINUS_EVAL_FUNC_DECL(int64)
DECINC_MINUS_EVAL_FUNC_DECL(int128)
DECINC_MINUS_EVAL_FUNC_DECL(int256)
DECINC_MINUS_EVAL_FUNC_DECL(int512)

int ObExprMinus::minus_decimalint512_with_check(EVAL_FUNC_ARG_DECL)
{
  return def_arith_eval_func<ObArithOpWrap<ObDecimalIntBatchMinusRawWithCheck>>(EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_decimalint512_with_check_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return def_batch_arith_op<ObArithOpWrap<ObDecimalIntBatchMinusRawWithCheck>>(BATCH_EVAL_FUNC_ARG_LIST);
}

int ObExprMinus::minus_decimalint512_with_check_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  return def_fixed_len_vector_arith_op<ObVectorArithOpWrap<ObDecimalIntBatchMinusRawWithCheck>>(
    VECTOR_EVAL_FUNC_ARG_LIST);
}

#undef DECINC_MINUS_EVAL_FUNC_DECL

template<typename T>
struct ObDecimalOracleMinusFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, const int64_t scale,
                 ObNumStackOnceAlloc &alloc) const
  {
    int ret = OB_SUCCESS;
    const T res_int = *reinterpret_cast<const T *>(l.ptr_) - *reinterpret_cast<const T *>(r.ptr_);
    number::ObNumber res_num;
    if (OB_FAIL(wide::to_number(res_int, scale, alloc, res_num))) {
      LOG_WARN("fail to cast decima int to number", K(ret), K(scale));
    } else {
      res.set_number(res_num);
      alloc.free();  // for batch function reuse alloc
    }
    return ret;
  }
};

template<typename T>
struct ObDecimalOracleVectorMinusFunc
{
  template <typename ResVector, typename LeftVector, typename RightVector>
  int operator()(ResVector &res_vec, const LeftVector &l_vec, const RightVector &r_vec,
                 const int64_t idx, const int64_t scale, ObNumStackOnceAlloc &alloc) const
  {
    int ret = OB_SUCCESS;
    const T res_int = *reinterpret_cast<const T *>(l_vec.get_payload(idx))
                      - *reinterpret_cast<const T *>(r_vec.get_payload(idx));
    number::ObNumber res_num;
    if (OB_FAIL(wide::to_number(res_int, scale, alloc, res_num))) {
      LOG_WARN("fail to cast decima int to number", K(ret), K(scale));
    } else {
      res_vec.set_number(idx, res_num);
      alloc.free();  // for batch function reuse alloc
    }
    return ret;
  }
};

#define DECINC_MINUS_EVAL_FUNC_ORA_DECL(TYPE) \
int ObExprMinus::minus_decimal##TYPE##_oracle(EVAL_FUNC_ARG_DECL)      \
{                                            \
  ObNumStackOnceAlloc tmp_alloc;                                \
  const int64_t scale = expr.args_[0]->datum_meta_.scale_;      \
  return def_arith_eval_func<ObDecimalOracleMinusFunc<TYPE##_t>>(EVAL_FUNC_ARG_LIST, scale, tmp_alloc); \
}                                            \
int ObExprMinus::minus_decimal##TYPE##_oracle_batch(BATCH_EVAL_FUNC_ARG_DECL)      \
{                                            \
  ObNumStackOnceAlloc tmp_alloc;                                \
  const int64_t scale = expr.args_[0]->datum_meta_.scale_;      \
  return def_batch_arith_op_by_datum_func<ObDecimalOracleMinusFunc<TYPE##_t>>(BATCH_EVAL_FUNC_ARG_LIST, scale, tmp_alloc); \
}                                            \
int ObExprMinus::minus_decimal##TYPE##_oracle_vector(VECTOR_EVAL_FUNC_ARG_DECL)      \
{                                            \
  ObNumStackOnceAlloc tmp_alloc;                                \
  const int64_t scale = expr.args_[0]->datum_meta_.scale_;      \
  return def_fixed_len_vector_arith_op_func<ObDecimalOracleVectorMinusFunc<TYPE##_t>,\
                                            ObArithTypedBase<TYPE##_t, TYPE##_t, TYPE##_t>>(VECTOR_EVAL_FUNC_ARG_LIST, scale, tmp_alloc); \
}

DECINC_MINUS_EVAL_FUNC_ORA_DECL(int32)
DECINC_MINUS_EVAL_FUNC_ORA_DECL(int64)
DECINC_MINUS_EVAL_FUNC_ORA_DECL(int128)


#undef DECINC_MINUS_EVAL_FUNC_ORA_DECL

}
}
