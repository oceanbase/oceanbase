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

#define USING_LOG_PREFIX  SQL_ENG

#include "sql/engine/expr/ob_expr_unix_timestamp.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/allocator/ob_allocator.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprUnixTimestamp::ObExprUnixTimestamp(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_UNIX_TIMESTAMP, N_UNIX_TIMESTAMP, ZERO_OR_ONE, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprUnixTimestamp::~ObExprUnixTimestamp()
{
}

int ObExprUnixTimestamp::calc_result_typeN(ObExprResType &type,
                                           ObExprResType *type_array,
                                           int64_t param,
                                           ObExprTypeCtx &type_ctx) const
{
  /*  yeti:
   *  no argument - > int
   *  has argument & is literal :
   *  --> out_of_range date --> 0.000000
   *  --> (has usec) --> int
   *  --> (not has usec) --> number scale = -1
   *  has argument & is not literal :
   *  --> is string --> number scale = 6
   *  --> type.scale = 0 --> int
   *  --> type.scale != 0 --> number scale = type.scale
   */

  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (0 == param) {
    type.set_int();
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
    UNUSED(type_array);
  } else {
    ObExprResType &date_type = type_array[0];
    if(ObNullType == date_type.get_type()) {
      type.set_int();
    } else if (date_type.is_literal()) { // literal
      ret = calc_result_type_literal(type, date_type);
    } else { // column
      ret = calc_result_type_column(type, date_type);
    }
    date_type.set_calc_type(ObTimestampType);
    date_type.set_calc_scale(type.get_scale());
  }
  ret = OB_SUCCESS;//just let it go if error happened
  return ret;

}

int ObExprUnixTimestamp::calc_result_type_literal(ObExprResType &type,
                             ObExprResType &type1) const
{
  int ret = OB_SUCCESS;
  ObObj date_obj = type1.get_param();
  bool is_number_res_type = false;
  int64_t utz_value = 0;
  if (date_obj.is_string_type()) { //string
    int16_t scale = DEFAULT_SCALE_FOR_INTEGER;
    ObTimeConvertCtx cvrt_ctx(NULL, false);
    if (OB_FAIL(ObTimeConverter::str_to_datetime(
                date_obj.get_string(), cvrt_ctx, utz_value, &scale, 0))) {
      LOG_WARN("failed to cast str to datetime", K(ret));
      is_number_res_type = true;
      type.set_scale(MAX_SCALE_FOR_TEMPORAL);
    } else {
      type.set_scale(scale);
    }
  } else {
    ObArenaAllocator oballocator(ObModIds::BLOCK_ALLOC); // is this ok?
    ObCastCtx cast_ctx(&oballocator,
                       NULL,
                       0,
                       CM_NONE,
                       CS_TYPE_INVALID,
                       NULL);
    EXPR_GET_DATETIME_V2(date_obj, utz_value);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to cast date to datetime", K(ret), K(date_obj));
      utz_value = 0;
    }
  }
  // todo (yeti) out-of-range date --> 0.000000
  if (is_number_res_type || utz_value % 1000000) {
    type.set_number();
    if (!type1.is_string_type()) {
      type.set_scale(type1.get_scale());
    }
    type.set_precision(static_cast<ObPrecision>(TIMESTAMP_VALUE_LENGTH +
                                        easy_max(0, type.get_scale())));

  } else {
    type.set_int();
    type.set_precision(static_cast<ObPrecision>(TIMESTAMP_VALUE_LENGTH));
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  }
  return ret;
}

int ObExprUnixTimestamp::calc_result_type_column(ObExprResType &type,
                             ObExprResType &type1) const
{
  int ret = OB_SUCCESS;
  if (type1.is_string_type() || type1.is_double()) {
    type.set_number();
    type.set_scale(MAX_SCALE_FOR_TEMPORAL);
    type.set_precision(static_cast<ObPrecision>(TIMESTAMP_VALUE_LENGTH +
                                          easy_max(0, type.get_scale())));
  } else {
    if (DEFAULT_SCALE_FOR_INTEGER == type1.get_scale()) {
      type.set_int();
      type.set_precision(static_cast<ObPrecision>(TIMESTAMP_VALUE_LENGTH));
      type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
    } else {
      type.set_number();
      type.set_scale(type1.get_scale());
      type.set_precision(static_cast<ObPrecision>(TIMESTAMP_VALUE_LENGTH +
                                          easy_max(0, type.get_scale())));
    }
  }
  return ret;
}

int ObExprUnixTimestamp::eval_unix_timestamp(const ObExpr &expr, ObEvalCtx &ctx, 
                                       ObDatum &res)
{
  int ret = OB_SUCCESS;
  int64_t utz_value = 0;
  bool is_null = false;
  if (0 == expr.arg_cnt_) {
    CK(OB_NOT_NULL(ctx.exec_ctx_.get_physical_plan_ctx()));
    if (OB_SUCC(ret)) {
      const ObObj &date = ctx.exec_ctx_.get_physical_plan_ctx()->get_cur_time();
      utz_value = date.get_timestamp();
    }
  } else if (1 == expr.arg_cnt_) {
    ObDatum *arg = NULL;
    if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
      LOG_WARN("eval arg failed", K(ret));
    } else if (arg->is_null()) {
      is_null = true;
      res.set_null();
    } else {
      utz_value = arg->get_timestamp();
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg cnt", K(ret), K(expr.arg_cnt_));
  }

  if (OB_SUCC(ret) && !is_null) {
    ObObjType res_type = expr.datum_meta_.type_;
    utz_value = utz_value < 0 ? 0 : utz_value;
    if (ObIntType == res_type) {
      res.set_int(utz_value/ 1000000);
    } else if (ObNumberType == res_type) {
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &calc_alloc = alloc_guard.get_allocator();
      number::ObNumber tmp_nmb;
      number::ObNumber res_nmb;
      number::ObNumber usec_nmb;
      int64_t usec = 1000000;
      OZ(usec_nmb.from(usec, calc_alloc));
      OZ(tmp_nmb.from(utz_value, calc_alloc));
      OZ(tmp_nmb.div(usec_nmb, res_nmb, calc_alloc));
      OX(res.set_number(res_nmb));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected res type", K(ret), K(res_type));
    }
  }
  return ret;
}

int ObExprUnixTimestamp::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_unix_timestamp;
  return ret;
}
} //namespace sql
} //namespace oceanbase
