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
#include "sql/engine/expr/ob_expr_startup_mode.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprStartUpMode::ObExprStartUpMode(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_STARTUP_MODE, N_STARTUP_MODE, 0, NOT_VALID_FOR_GENERATED_COL)
{
}

ObExprStartUpMode::~ObExprStartUpMode()
{
}

int ObExprStartUpMode::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_varchar();
  type.set_default_collation_type();
  type.set_collation_level(CS_LEVEL_SYSCONST);
  type.set_length(OB_MAX_CONFIG_VALUE_LEN);
  return OB_SUCCESS;
}

int ObExprStartUpMode::eval_startup_mode(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  UNUSED(ctx);
  expr_datum.set_string(GCONF.ob_startup_mode.str());
  return ret;
}

int ObExprStartUpMode::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprStartUpMode::eval_startup_mode;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
