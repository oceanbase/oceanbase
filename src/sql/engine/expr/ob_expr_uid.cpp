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
#include "sql/engine/expr/ob_expr_uid.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{


ObExprUid::ObExprUid(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_UID, N_UID, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprUid::~ObExprUid()
{
}

int ObExprUid::calc_result_type0(ObExprResType &type,
                                  ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  if (lib::is_oracle_mode()) {
    type.set_number();
    type.set_precision(PRECISION_UNKNOWN_YET);
    type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
  } else {
    type.set_uint64();
  }
  return OB_SUCCESS;
}

int ObExprUid::eval_uid(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  const ObBasicSessionInfo *session_info = NULL;
  if (OB_ISNULL(session_info = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else {
    char local_buff[number::ObNumber::MAX_BYTE_LEN];
    ObDataBuffer local_alloc(local_buff, number::ObNumber::MAX_BYTE_LEN);
    number::ObNumber num;
    if (OB_FAIL(num.from(session_info->get_user_id(), local_alloc))) {
      LOG_WARN("failed to convert int to number", K(ret));
    } else {
      expr_datum.set_number(num);
    }
  }
  return ret;
}

int ObExprUid::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprUid::eval_uid;
  return OB_SUCCESS;
}

}/* ns sql*/
}/* ns oceanbase */
