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
#include "sql/engine/expr/ob_expr_current_catalog.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{


ObExprCurrentCatalog::ObExprCurrentCatalog(ObIAllocator &alloc)
    :ObFuncExprOperator(alloc, T_FUN_SYS_CURRENT_CATALOG, N_CURRENT_CATALOG, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprCurrentCatalog::~ObExprCurrentCatalog()
{
}

int ObExprCurrentCatalog::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_varchar();
  type.set_default_collation_type();
  type.set_collation_level(CS_LEVEL_SYSCONST);
  type.set_length(static_cast<ObLength>((OB_MAX_CATALOG_NAME_LENGTH)));
  return ret;
}

int ObExprCurrentCatalog::eval_current_catalog(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  const ObSQLSessionInfo *session_info = NULL;
  ObSchemaGetterGuard schema_guard;
  uint64_t tenant_id = OB_INVALID_ID;
  uint64_t catalog_id = OB_INVALID_ID;
  const ObCatalogSchema *catalog_schema = NULL;
  if (OB_ISNULL(session_info = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else {
    tenant_id = session_info->get_effective_tenant_id();
    catalog_id = session_info->get_current_default_catalog();
  }
  if (OB_SUCC(ret)) {
    if (OB_INVALID_ID == catalog_id) {
      expr_datum.set_null();
    } else if (OB_INTERNAL_CATALOG_ID == catalog_id) {
      expr_datum.set_string(is_mysql_mode() ? OB_INTERNAL_CATALOG_NAME : OB_INTERNAL_CATALOG_NAME_UPPER);
    } else {
      if (OB_ISNULL(GCTX.schema_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL GCTX.schema_service_", K(ret));
      } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("failed to get tenant schema guard", K(ret));
      } else if (OB_FAIL(schema_guard.get_catalog_schema_by_id(tenant_id, catalog_id, catalog_schema))) {
        LOG_WARN("failed to get user info", K(ret), K(tenant_id), K(catalog_id));
      } else if (OB_ISNULL(catalog_schema)) {
        expr_datum.set_null();
      } else {
        expr_datum.set_string(catalog_schema->get_catalog_name_str());
      }
    }
  }
  return ret;
}

int ObExprCurrentCatalog::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprCurrentCatalog::eval_current_catalog;
  return OB_SUCCESS;
}

}/* ns sql*/
}/* ns oceanbase */
