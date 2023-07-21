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

// (C) Copyright 2011-2023 Alibaba Inc. All Rights Reserved.
//  Authors:
//    jim.wjh <>
//
#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_temp_table_ssid.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprTempTableSSID::ObExprTempTableSSID(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_GET_TEMP_TABLE_SESSID, N_TEMP_TABLE_SSID, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprTempTableSSID::~ObExprTempTableSSID()
{
}

int ObExprTempTableSSID::calc_result_type1(ObExprResType &type, ObExprResType &type1, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type1);
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  type.set_int();
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  type1.set_calc_type(ObIntType);
  return ret;
}

int ObExprTempTableSSID::calc_temp_table_ssid(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  if (OB_ISNULL(ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNDEFINED;
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param))) {
    LOG_WARN("fail to eval param", K(ret));
  } else if (expr.args_[0]->datum_meta_.type_ != ObIntType) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid object", K(ret));
  } else {
    switch (param->get_int()) {
      case GTT_SESSION_SCOPE:
        res.set_int(ctx.exec_ctx_.get_my_session()->get_gtt_session_scope_unique_id());
        break;
      case GTT_TRANS_SCOPE:
        res.set_int(ctx.exec_ctx_.get_my_session()->get_gtt_trans_scope_unique_id());
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid object", K(ret), KPC(param));
    }
    LOG_DEBUG("get result", K(res.get_int()));
  }
  return ret;
}

int ObExprTempTableSSID::cg_expr(
    ObExprCGCtx &op_cg_ctx,
    const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = calc_temp_table_ssid;
  return ret;
}

}
}
