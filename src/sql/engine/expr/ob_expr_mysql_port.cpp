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

#include "sql/engine/expr/ob_expr_mysql_port.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{


ObExprMySQLPort::ObExprMySQLPort(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_MYSQL_PORT, N_MYSQL_PORT, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprMySQLPort::~ObExprMySQLPort()
{
}

int ObExprMySQLPort::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_int();
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  return ret;
}

int ObExprMySQLPort::eval_mysql_port(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  UNUSED(ctx);
  common::ObServerConfig *config = GCTX.config_;
  if (OB_ISNULL(config)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "server config is null, get mysql port failed", K(ret));
  } else {
    expr_datum.set_int(config->mysql_port);
  }
  return ret;
}

int ObExprMySQLPort::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprMySQLPort::eval_mysql_port;
  return OB_SUCCESS;
}
} // namespace sql
} // namespace oceanbase
