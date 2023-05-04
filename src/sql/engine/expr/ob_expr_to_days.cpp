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

#define USING_LOG_PREFIX SQL_EXE
#include "ob_expr_to_days.h"
#include "lib/timezone/ob_time_convert.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_util.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprToDays::ObExprToDays(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TO_DAYS, N_TO_DAYS, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION) {};

ObExprToDays::~ObExprToDays()
{
}

int ObExprToDays::calc_result_type1(ObExprResType &type,
                                           ObExprResType &date,
                                           common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_int();
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  if (ob_is_enumset_tc(date.get_type())) {
    date.set_calc_type(ObVarcharType);
  } else {
    date.set_calc_type(ObDateType);
  }
  return ret;
}

int calc_todays_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *day_datum = NULL;
  // 这里没有像老框架一样，判断cast的返回值是否是OB_INVLIAD_DATE_VALUE
  // 如果转换失败，结果会被置为ZERO_DATE（cast mode默认是ZERO_ON_WARN）
  // MySQL的行为是，结果是zero date时，返回NULL
  if (OB_FAIL(expr.args_[0]->eval(ctx, day_datum))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (day_datum->is_null()) {
    res_datum.set_null();
  } else {
    int64_t day_int = day_datum->get_date() + DAYS_FROM_ZERO_TO_BASE;
    if (day_int < 0 || ObTimeConverter::ZERO_DATE == day_int) {
      res_datum.set_null();
    } else {
      res_datum.set_int(day_int);
    }
  }
  return ret;
}

int ObExprToDays::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  if (OB_UNLIKELY(1 != raw_expr.get_param_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("raw_expr should got one child", K(ret), K(raw_expr));
  } else if (ObDateType != rt_expr.args_[0]->datum_meta_.type_) {
    // 类型推导部分有针对enum/set设置的calc type，但是新框架cast目前对enum/set支持不完整
    // 这里先报错，后续补上对enum/set的处理
    // enum/set->varchar->date
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param type should be date", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = calc_todays_expr;
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
