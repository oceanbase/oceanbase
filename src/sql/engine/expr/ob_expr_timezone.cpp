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
#include "sql/engine/expr/ob_expr_timezone.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/system_variable/ob_sys_var_class_type.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{
const int64_t ORACLE_SESSIONTIMEZONE_MAX_LENGTH = 75;
const int64_t ORACLE_DBTIMEZONE_MAX_LENGTH = 6;//[+|-]TZH:TZM


ObExprBaseTimezone::ObExprBaseTimezone(ObIAllocator &alloc, ObExprOperatorType type,
    const char *name, const bool is_sessiontimezone)
  : ObFuncExprOperator(alloc, type, name, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION), is_sessiontimezone_(is_sessiontimezone)
{
}

int ObExprBaseTimezone::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  type.set_varchar();
  type.set_collation_type(type_ctx.get_coll_type());
  type.set_collation_level(CS_LEVEL_IMPLICIT);
  type.set_length(is_sessiontimezone_ ? ORACLE_SESSIONTIMEZONE_MAX_LENGTH : ORACLE_DBTIMEZONE_MAX_LENGTH);
  type.set_length_semantics(OB_NOT_NULL(type_ctx.get_session()) ? type_ctx.get_session()->get_actual_nls_length_semantics(): LS_BYTE);
  return OB_SUCCESS;
}


ObExprSessiontimezone::ObExprSessiontimezone(ObIAllocator &alloc)
  : ObExprBaseTimezone(alloc, T_FUN_SYS_SESSIONTIMEZONE, N_SESSIONTIMEZONE, true)
{
}

int ObExprSessiontimezone::eval_session_timezone(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  const ObBasicSessionInfo *session_info = NULL;
  if (OB_ISNULL(session_info = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else if (OB_ISNULL(session_info->get_tz_info_wrap().get_time_zone_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("time_zone_info is NULL", KPC(session_info), K(ret));
  } else {
    char tmp_buf[ORACLE_SESSIONTIMEZONE_MAX_LENGTH] = {0};
    int64_t pos = 0;
    if (OB_FAIL(session_info->get_tz_info_wrap().get_time_zone_info()->timezone_to_str(
        tmp_buf, ORACLE_SESSIONTIMEZONE_MAX_LENGTH, pos))) {
      LOG_WARN("failed to timezone_to_str",
               "time_zone_info", *(session_info->get_tz_info_wrap().get_time_zone_info()), K(ret));
    } else {
      char *buf = expr.get_str_res_mem(ctx, pos);
      if (OB_ISNULL(buf)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "buff is null", K(ret));
      } else {
        MEMCPY(buf, tmp_buf, pos);
        expr_datum.set_string(buf, pos);
      }
    }
  }
  return ret;
}

int ObExprSessiontimezone::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprSessiontimezone::eval_session_timezone;
  return OB_SUCCESS;
}


ObExprDbtimezone::ObExprDbtimezone(ObIAllocator &alloc)
  : ObExprBaseTimezone(alloc, T_FUN_SYS_DBTIMEZONE, N_DBTIMEZONE, false)
{
}

int ObExprDbtimezone::eval_db_timezone(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  const char const_dbtimezone_str[] = "+00:00";
  const size_t const_size = strlen(const_dbtimezone_str);
  char *buf = expr.get_str_res_mem(ctx, const_size);
  if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "buff is null", K(ret));
  } else {
    MEMCPY(buf, const_dbtimezone_str, const_size);
    expr_datum.set_string(buf, const_size);
  }
  return ret;
}

int ObExprDbtimezone::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprDbtimezone::eval_db_timezone;
  return OB_SUCCESS;
}

}/* ns sql*/
}/* ns oceanbase */
