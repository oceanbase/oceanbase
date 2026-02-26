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
 * This file contains implementation for rb_build_empty.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_RB_BUILD_EMPTY_
#define OCEANBASE_SQL_OB_EXPR_RB_BUILD_EMPTY_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprRbBuildEmpty : public ObFuncExprOperator
{
public:
  explicit ObExprRbBuildEmpty(common::ObIAllocator &alloc);
  virtual ~ObExprRbBuildEmpty();
  virtual int calc_result_type0(ObExprResType &type, common::ObExprTypeCtx &type_ctx) const;

  static int eval_rb_build_empty(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRbBuildEmpty);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_RB_BUILD_EMPTY_