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

#include "sql/engine/expr/ob_expr_version.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

ObExprVersion::ObExprVersion(ObIAllocator& alloc) : ObStringExprOperator(alloc, T_FUN_SYS_VERSION, N_VERSION, 0)
{}

ObExprVersion::~ObExprVersion()
{}

int ObExprVersion::calc_result_type0(ObExprResType& type, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  type.set_varchar();
  type.set_length(static_cast<common::ObLength>(strlen(PACKAGE_VERSION)));
  type.set_default_collation_type();
  type.set_collation_level(CS_LEVEL_SYSCONST);
  return OB_SUCCESS;
}

int ObExprVersion::calc_result0(ObObj& result, ObExprCtx& expr_ctx) const
{
  UNUSED(expr_ctx);
  result.set_varchar(common::ObString(PACKAGE_VERSION));
  result.set_collation(result_type_);
  return OB_SUCCESS;
}

int ObExprVersion::eval_version(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  UNUSED(expr);
  UNUSED(ctx);
  expr_datum.set_string(common::ObString(PACKAGE_VERSION));
  return OB_SUCCESS;
}

int ObExprVersion::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprVersion::eval_version;
  return OB_SUCCESS;
}

}  // namespace sql
}  // namespace oceanbase
