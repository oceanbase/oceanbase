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
#include "sql/engine/expr/ob_expr_sys_view_bigint_param.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace sql {

ObExprSysViewBigintParam::ObExprSysViewBigintParam(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_SYS_VIEW_BIGINT_PARAM, N_SYS_VIEW_BIGINT_PARAM, 1, NOT_ROW_DIMENSION)
{}

ObExprSysViewBigintParam::~ObExprSysViewBigintParam()
{}

int ObExprSysViewBigintParam::calc_result_type1(
    ObExprResType& type, ObExprResType& type1, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(type1);
  int ret = OB_SUCCESS;
  if (NOT_ROW_DIMENSION != row_dimension_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row dimension must be NOT_ROW_DIMENSION", K(ret), K(row_dimension_));
  } else {
    // keep enumset as origin
    type.set_int();
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  }
  return ret;
}

int ObExprSysViewBigintParam::calc_result1(
    common::ObObj& result, const common::ObObj& obj, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = expr_ctx.phy_plan_ctx_;
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("physical plan context is NULL", K(ret));
  } else if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("calc buf is NULL", K(ret));
  } else {
    int64_t idx = 0;
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    EXPR_GET_INT64_V2(obj, idx);
    if (OB_FAIL(ret)) {
      LOG_ERROR("fail to get idx", K(ret), K(obj));
    } else {
      ObArray<ObObj> sys_view_bigint_params;  // useless
      if (OB_UNLIKELY(idx < 0 || idx >= sys_view_bigint_params.count())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("idx is out of range", K(ret), K(idx), K(sys_view_bigint_params.count()));
      } else {
        const ObObj& sys_view_bigint_param = sys_view_bigint_params.at(idx);
        if (OB_FAIL(ObObjCaster::to_type(ObIntType, cast_ctx, sys_view_bigint_param, result))) {
          LOG_WARN("failed to cast object", K(ret), K(sys_view_bigint_param));
        }
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
