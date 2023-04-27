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
#include "sql/engine/expr/ob_expr_host_ip.h"
#include "observer/ob_server_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{


ObExprHostIP::ObExprHostIP(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_HOST_IP, N_HOST_IP, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprHostIP::~ObExprHostIP()
{
}

int ObExprHostIP::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  type.set_varchar();
  type.set_length(MAX_IP_ADDR_LENGTH);
  if (is_oracle_mode()) {
    const ObSQLSessionInfo *session = type_ctx.get_session();
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "session is null", K(ret));
    } else {
      type.set_collation_type(session->get_nls_collation());
      type.set_length_semantics(session->get_actual_nls_length_semantics());
      type.set_collation_level(CS_LEVEL_SYSCONST);
    }
  } else {
    const ObLengthSemantics default_length_semantics =
      (OB_NOT_NULL(type_ctx.get_session()) ?
       type_ctx.get_session()->get_actual_nls_length_semantics() : LS_BYTE);
    type.set_length_semantics(default_length_semantics);
    type.set_default_collation_type();
    type.set_collation_level(CS_LEVEL_SYSCONST);
  }
  return ret;
}

int ObExprHostIP::eval_host_ip(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  char *buf = expr.get_str_res_mem(ctx, OB_IP_STR_BUFF);
  if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "buff is null", K(ret));
  } else {
    //see
    ObAddr addr = ObCurTraceId::get_addr();
    MEMSET(buf, 0, OB_IP_STR_BUFF);
    if (!addr.ip_to_string(buf, OB_IP_STR_BUFF)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "ip to string failed", K(ret));
    } else {
      expr_datum.set_string(buf, strlen(buf));
    }
  }
  return ret;
}

int ObExprHostIP::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprHostIP::eval_host_ip;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
