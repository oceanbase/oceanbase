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

#include "sql/engine/expr/ob_expr_database.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprDatabase::ObExprDatabase(ObIAllocator &alloc)
  : ObStringExprOperator(alloc, T_FUN_SYS_DATABASE, N_DATABASE, 0, NOT_VALID_FOR_GENERATED_COL)
{
}

ObExprDatabase::~ObExprDatabase()
{
}

int ObExprDatabase::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_varchar();
  type.set_default_collation_type();
  type.set_collation_level(CS_LEVEL_SYSCONST);
  type.set_length(OB_MAX_DATABASE_NAME_LENGTH);
  return OB_SUCCESS;
}

int ObExprDatabase::eval_database(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  const ObBasicSessionInfo *session_info = NULL;
  if (OB_ISNULL(session_info = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "session info is null", K(ret));
  } else {
    const ObString database_name = session_info->get_database_name();
    if (database_name.empty()) {
      expr_datum.set_null();
    } else {
      expr_datum.set_string(database_name);
    }
  }
  return ret;
}

int ObExprDatabase::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprDatabase::eval_database;
  return OB_SUCCESS;
}
}
}
