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

#include "sql/engine/expr/ob_expr_timestamp_to_scn.h"
#include "lib/ob_name_def.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/ob_exec_context.h"


namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprTimestampToScn::ObExprTimestampToScn(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TIMESTAMP_TO_SCN, N_TIMESTAMP_TO_SCN, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprTimestampToScn::~ObExprTimestampToScn()
{
}

int ObExprTimestampToScn::calc_result_type1(ObExprResType &type,
                                            ObExprResType &time,
                                            ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    type.set_number();
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
    //set calc type
    time.set_calc_type(ObTimestampNanoType);
  } else {
    type.set_uint64();
    type.set_precision(ObAccuracy::MAX_ACCURACY[ObUInt64Type].precision_);
    type.set_scale(ObAccuracy::MAX_ACCURACY[ObUInt64Type].scale_);
    type_ctx.set_cast_mode(type_ctx.get_cast_mode() & (~(CM_WARN_ON_FAIL)));
    //set calc type
    time.set_calc_type(ObDateTimeType);
  }
  return ret;
}

int calc_timestamp_to_scn_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *in_datum = NULL;
  ObString sys_time_zone;
  ObTimeZoneInfoWrap tz_info_wrap;
  const ObBasicSessionInfo *session = ctx.exec_ctx_.get_my_session();
  const ObTimeZoneInfo *cur_tz_info = NULL;
  if (OB_ISNULL(session) || OB_ISNULL(cur_tz_info = session->get_timezone_info())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(" session or tz_info is null", KP(session), KP(cur_tz_info), K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, in_datum))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (OB_ISNULL(in_datum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in_datum is NULL", K(expr), K(ret));
  } else if (in_datum->is_null()) {
     ret = lib::is_oracle_mode() ? OB_ERR_FETCH_COLUMN_NULL : OB_INVALID_ARGUMENT_FOR_TIMESTAMP_TO_SCN;
  } else if (OB_FAIL(session->get_sys_variable(share::SYS_VAR_SYSTEM_TIME_ZONE, sys_time_zone))) {
    LOG_WARN("Get sys variable error", K(ret));
  } else if (OB_FAIL(tz_info_wrap.init_time_zone(sys_time_zone,
                                                 OB_INVALID_VERSION,
                                                 *(const_cast<ObTZInfoMap *>(cur_tz_info->get_tz_info_map()))))) {
    LOG_WARN("tz_info_wrap init_time_zone fail", KR(ret), K(sys_time_zone));
  } else {
    uint64_t scn_value = palf::LOG_INVALID_LSN_VAL;
    const ObTimeZoneInfo *sys_tz_info = tz_info_wrap.get_time_zone_info();
    if (lib::is_oracle_mode()) {
      ObNumStackOnceAlloc tmp_alloc;
      number::ObNumber number;
      ObOTimestampData timestamp_value = in_datum->get_otimestamp_tiny();
      if (OB_FAIL(ObTimeConverter::otimestamp_to_scn_value(timestamp_value, sys_tz_info, scn_value))) {
        LOG_WARN("failed to convert otimestamp to scn_val", K(ret), K(timestamp_value));
      } else if (OB_FAIL(number.from(scn_value, tmp_alloc))) {
        LOG_WARN("number.from failed", K(ret), K(scn_value));
      } else {
        res_datum.set_number(number);
      }
    } else {
      const int64_t datetime_value = in_datum->get_datetime();
      if (OB_FAIL(ObTimeConverter::datetime_to_scn_value(datetime_value, sys_tz_info, scn_value))) {
        LOG_WARN("failed to convert datetime to scn_val", K(ret), K(datetime_value));
      } else {
        res_datum.set_uint(scn_value);
      }
    }
  }
  return ret;
}

int ObExprTimestampToScn::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                  ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_timestamp_to_scn_expr;
  return ret;
}
}//end of namespace sql
}//end of namespace oceanbase
