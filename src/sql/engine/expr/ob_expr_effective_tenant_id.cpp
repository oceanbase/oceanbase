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
#include "sql/engine/expr/ob_expr_effective_tenant_id.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprEffectiveTenantId::ObExprEffectiveTenantId(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_EFFECTIVE_TENANT_ID,
                         N_EFFECTIVE_TENANT_ID, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
  {
  }
ObExprEffectiveTenantId::~ObExprEffectiveTenantId()
{
}

int ObExprEffectiveTenantId::calc_result_type0(ObExprResType &type,
                                               ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  const bool is_oracle = lib::is_oracle_mode();
  if (is_oracle) {
    type.set_type(ObNumberType);
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY2[is_oracle][ObNumberType].precision_);
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY2[is_oracle][ObNumberType].scale_);
  } else {
    type.set_type(ObUInt64Type);
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].precision_);
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].scale_);
  }
  return OB_SUCCESS;
}

int ObExprEffectiveTenantId::eval_effective_tenant_id(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  const ObBasicSessionInfo *session_info = NULL;
  if (OB_ISNULL(session_info = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else {
    const uint64_t effective_tenant_id = session_info->get_effective_tenant_id();
    if (OB_UNLIKELY(effective_tenant_id == OB_INVALID_ID)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("effective tanent id is invalid", K(ret));
    } else {
      if (lib::is_oracle_mode()) {
        char local_buff[number::ObNumber::MAX_BYTE_LEN];
        ObDataBuffer local_alloc(local_buff, number::ObNumber::MAX_BYTE_LEN);
        number::ObNumber num;
        if (OB_FAIL(num.from(effective_tenant_id, local_alloc))) {
          LOG_WARN("failed to convert int to number", K(ret));
        } else {
          expr_datum.set_number(num);
        }
      } else {
        expr_datum.set_uint(effective_tenant_id);
      }
    }
  }
  return ret;
}

int ObExprEffectiveTenantId::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprEffectiveTenantId::eval_effective_tenant_id;
  return OB_SUCCESS;
}


}/* ns sql*/
}/* ns oceanbase */
