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
