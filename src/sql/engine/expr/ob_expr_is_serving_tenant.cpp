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
#include "sql/engine/expr/ob_expr_is_serving_tenant.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/ob_unit_getter.h"
#include "share/ob_i_sql_expression.h"
#include "share/config/ob_server_config.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase
{
namespace sql
{


ObExprIsServingTenant::ObExprIsServingTenant(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_IS_SERVING_TENANT, N_IS_SERVING_TENANT, 3, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprIsServingTenant::~ObExprIsServingTenant()
{
}

int ObExprIsServingTenant::calc_result_type3(ObExprResType &type,
                                             ObExprResType &type1,
                                             ObExprResType &type2,
                                             ObExprResType &type3,
                                             ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  type1.set_calc_type(ObVarcharType);
  type1.set_calc_collation_type(type_ctx.get_coll_type());
  type2.set_calc_type(ObInt32Type);
  type3.set_calc_type(ObUInt64Type);
  type.set_int();
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
  return ret;
}

int ObExprIsServingTenant::check_serving_tenant(
    bool &serving, ObExecContext &exec_ctx, const ObString &ip,
    const int64_t port, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_SYS_TENANT_ID == tenant_id) {
    serving = true;
  } else {
    ObAddr svr;
    ObUnitInfoGetter ui_getter;
    ObArray<ObAddr> servers;
    if (OB_ISNULL(exec_ctx.get_sql_proxy())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("sql proxy from exec_ctx is NULL", K(ret));
    } else if (OB_FAIL(ui_getter.init(*exec_ctx.get_sql_proxy(), &GCONF))) {
      LOG_WARN("fail to init ObUnitInfoGetter", K(ret));
    } else if (OB_FAIL(ui_getter.get_tenant_servers(tenant_id, servers))) {
      LOG_WARN("fail to get servers of a tenant", K(ret));
    } else if (false == svr.set_ip_addr(ip, static_cast<int32_t>(port))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to set ip addr", K(ret), K(ip), K(port));
    } else {
      bool found_server = false;
      for (int64_t i = 0; OB_SUCC(ret) && false == found_server && i < servers.count(); ++i) {
        if (svr == servers.at(i)) {
          found_server = true;
        }
      }
      if (OB_SUCC(ret)) {
        serving = found_server;
      }
    }
  }
  return ret;
}

int ObExprIsServingTenant::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &expr) const
{
  int ret = OB_SUCCESS;
  CK(3 == expr.arg_cnt_);
  expr.eval_func_ = eval_is_serving_tenant;
  return ret;
}


int ObExprIsServingTenant::eval_is_serving_tenant(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *ip = NULL;
  ObDatum *port = NULL;
  ObDatum *tenant = NULL;
  bool serving = false;
  if (OB_FAIL(expr.eval_param_value(ctx, ip, port, tenant))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (ip->is_null() || port->is_null() || tenant->is_null()) {
    expr_datum.set_null();
  } else if (OB_FAIL(check_serving_tenant(
              serving, ctx.exec_ctx_, ip->get_string(), port->get_int(), tenant->get_uint()))) {
    LOG_WARN("check serving tenant failed", K(ret));
  } else {
    expr_datum.set_int(serving);
  }
  return ret;
}

}/* ns sql*/
}/* ns oceanbase */
