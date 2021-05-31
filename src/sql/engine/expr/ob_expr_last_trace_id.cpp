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

namespace oceanbase {
namespace sql {

ObExprLastTraceId::ObExprLastTraceId(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_GET_LAST_TRACE_ID, N_LAST_TRACE_ID, 0)
{}

ObExprLastTraceId::~ObExprLastTraceId()
{}

int ObExprLastTraceId::calc_result_type0(ObExprResType& type, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  type.set_varchar();
  type.set_default_collation_type();
  type.set_collation_level(CS_LEVEL_SYSCONST);
  type.set_length(OB_MAX_DATABASE_NAME_LENGTH);
  return OB_SUCCESS;
}

int ObExprLastTraceId::calc_result0(ObObj& result, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo* session_info = NULL;
  if (OB_ISNULL(session_info = expr_ctx.my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "session info is null");
  } else {
    common::ObCurTraceId::TraceId& trace_id =
        const_cast<common::ObCurTraceId::TraceId&>(session_info->get_last_trace_id());
    if (trace_id.is_invalid()) {
      result.set_null();
    } else {
      const int64_t buf_len = 128;
      char* buf = static_cast<char*>(expr_ctx.calc_buf_->alloc(buf_len));
      int64_t pos = 0;
      ObAddr server;
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(ERROR, "fail to alloc memory", K(ret), K(buf));
      } else if (OB_FAIL(trace_id.get_server_addr(server))) {
        SQL_ENG_LOG(WARN, "fail to get server addr", K(ret));
      } else if (OB_FAIL(server.ip_port_to_string(buf, buf_len))) {
        SQL_ENG_LOG(WARN, "fail to print server ip", K(ret));
      } else {
        pos += strlen(buf);
        BUF_PRINTF(", TraceId: " TRACE_ID_FORMAT, trace_id.get()[0], trace_id.get()[1]);
        result.set_varchar(ObString(strlen(buf), buf));
        result.set_collation(result_type_);
      }
    }
  }
  return ret;
}

int ObExprLastTraceId::eval_last_trace_id(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  const ObSQLSessionInfo* session_info = NULL;
  if (OB_ISNULL(session_info = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "session info is null", K(ret));
  } else {
    const ObCurTraceId::TraceId& trace_id = session_info->get_last_trace_id();
    if (trace_id.is_invalid()) {
      expr_datum.set_null();
    } else {
      const int64_t MAX_BUF_LEN = 128;
      char* buf = expr.get_str_res_mem(ctx, MAX_BUF_LEN);
      int64_t pos = 0;
      if (OB_ISNULL(buf)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "buff is null", K(ret));
      } else {
        ObAddr server;
        if (OB_FAIL(trace_id.get_server_addr(server))) {
          SQL_ENG_LOG(WARN, "fail to get server addr", K(ret));
        } else if (OB_FAIL(server.ip_port_to_string(buf, MAX_BUF_LEN))) {
          SQL_ENG_LOG(WARN, "fail to print server ip", K(ret));
        } else {
          pos += strlen(buf);
          if (OB_FAIL(databuff_printf(
                  buf, MAX_BUF_LEN, pos, ", TraceId: " TRACE_ID_FORMAT, trace_id.get()[0], trace_id.get()[1]))) {
            SQL_ENG_LOG(WARN, "fail to databuff_printf", K(ret));
          } else {
            expr_datum.set_string(buf, pos);
          }
        }
      }
    }
  }
  return ret;
}

int ObExprLastTraceId::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprLastTraceId::eval_last_trace_id;
  return OB_SUCCESS;
}
}  // namespace sql
}  // namespace oceanbase
