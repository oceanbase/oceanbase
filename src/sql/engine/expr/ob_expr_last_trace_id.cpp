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

#include "sql/engine/expr/ob_expr_last_trace_id.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprLastTraceId::ObExprLastTraceId(ObIAllocator &alloc)
  : ObStringExprOperator(alloc, T_FUN_GET_LAST_TRACE_ID, N_LAST_TRACE_ID, 0, NOT_VALID_FOR_GENERATED_COL)
{
}

ObExprLastTraceId::~ObExprLastTraceId()
{
}

int ObExprLastTraceId::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_varchar();
  type.set_default_collation_type();
  type.set_collation_level(CS_LEVEL_SYSCONST);
  type.set_length(OB_MAX_DATABASE_NAME_LENGTH);
  return OB_SUCCESS;
}

int ObExprLastTraceId::eval_last_trace_id(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  const ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  if (OB_ISNULL(phy_plan_ctx = ctx.exec_ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "session info is null", K(ret));
  } else {
    const ObCurTraceId::TraceId &trace_id = phy_plan_ctx->get_last_trace_id();
    if (trace_id.is_invalid()) {
      expr_datum.set_null();
    } else {
      const int64_t MAX_BUF_LEN = 128;
      char *buf = expr.get_str_res_mem(ctx, MAX_BUF_LEN);
      int64_t pos = 0;
      if (OB_ISNULL(buf)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "buff is null", K(ret));
      } else {
        if (OB_FAIL(databuff_printf(buf, MAX_BUF_LEN, pos, "%s",
                                    to_cstring(trace_id)))) {
          SQL_ENG_LOG(WARN, "fail to databuff_printf", K(ret));
        } else {
          expr_datum.set_string(buf, pos);
        }
      }
    }
  }
  return ret;
}

int ObExprLastTraceId::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprLastTraceId::eval_last_trace_id;
  return OB_SUCCESS;
}
}
}
