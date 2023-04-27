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
#include "sql/engine/expr/ob_expr_rpc_port.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{


ObExprRpcPort::ObExprRpcPort(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_RPC_PORT, N_RPC_PORT, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprRpcPort::~ObExprRpcPort()
{
}

int ObExprRpcPort::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_int32();
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObInt32Type].scale_);
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObInt32Type].precision_);
  return ret;
}

int ObExprRpcPort::eval_rpc_port(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  UNUSED(ctx);
  //see
  ObAddr addr = ObCurTraceId::get_addr();
  expr_datum.set_int32(addr.get_port());
  return ret;
}

int ObExprRpcPort::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprRpcPort::eval_rpc_port;
  return OB_SUCCESS;
}
} // namespace sql
} // namespace oceanbase
