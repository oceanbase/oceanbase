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
#include "sql/engine/expr/ob_expr_check_location_access.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{


ObExprCheckLocationAccess::ObExprCheckLocationAccess(ObIAllocator &alloc)
    :ObFuncExprOperator(alloc, T_FUN_SYS_CHECK_LOCATION_ACCESS, N_CHECK_LOCATION_ACCESS, 1,
                        NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprCheckLocationAccess::~ObExprCheckLocationAccess()
{
}

int ObExprCheckLocationAccess::calc_result_type1(ObExprResType &type,
                                                ObExprResType &type1,
                                                ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_tinyint();
  type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
  type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  type1.set_calc_type(ObVarcharType);
  type1.set_calc_collation_type(ObCharset::get_system_collation());
  return ret;
}

int ObExprCheckLocationAccess::eval_check_location_access(const ObExpr &expr,
                                                        ObEvalCtx &ctx,
                                                        ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *location = NULL;
  bool allow_show = true;
  ObSchemaGetterGuard schema_guard;
  const ObSQLSessionInfo *session_info = NULL;
  share::schema::ObSessionPrivInfo session_priv;
  uint64_t tenant_id = OB_INVALID_ID;
  if (OB_ISNULL(session_info = ctx.exec_ctx_.get_my_session()) || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FALSE_IT(tenant_id = session_info->get_effective_tenant_id())) {
  } else if (OB_FAIL(expr.eval_param_value(ctx, location))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret));
  } else if (OB_FAIL(session_info->get_session_priv_info(session_priv))) {
    LOG_WARN("failed to get session priv info", K(ret));
  } else if (OB_FAIL(schema_guard.check_location_show(session_priv,
                                                    session_info->get_enable_role_array(),
                                                    location->is_null() ? ObString() : location->get_string(),
                                                    allow_show))) {
    LOG_WARN("failed to check location show", K(ret));
  } else {
    expr_datum.set_bool(allow_show);
  }
  return ret;
}

int ObExprCheckLocationAccess::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprCheckLocationAccess::eval_check_location_access;
  return OB_SUCCESS;
}

}/* ns sql*/
}/* ns oceanbase */
