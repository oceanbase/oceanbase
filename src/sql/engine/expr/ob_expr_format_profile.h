/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprFormatProfile : public ObFuncExprOperator
{
public:
  explicit ObExprFormatProfile(common::ObIAllocator &alloc);
  virtual ~ObExprFormatProfile();
  int calc_result_typeN(ObExprResType &result_type, ObExprResType *types_stack, int64_t param_num,
                        ObExprTypeCtx &type_ctx) const override final;
  int cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
              ObExpr &rt_expr) const override final;

  static int format_profile(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprFormatProfile);
};
} // namespace sql
} // namespace oceanbase
