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
 * This file contains implementation for array_range expression.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ARRAY_RANGE
#define OCEANBASE_SQL_OB_EXPR_ARRAY_RANGE

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/udt/ob_array_type.h"

namespace oceanbase
{
namespace sql
{
class ObExprArrayRange : public ObFuncExprOperator
{
public:
  explicit ObExprArrayRange(common::ObIAllocator &alloc);
  virtual ~ObExprArrayRange();

  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx) const override;
  static int eval_array_range(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprArrayRange);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ARRAY_RANGE