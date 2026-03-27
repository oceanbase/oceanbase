/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_POS_LIST_H_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_POS_LIST_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprPosList : public ObFuncExprOperator
{
public:
  explicit ObExprPosList(common::ObIAllocator &alloc);
  virtual ~ObExprPosList() = default;
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int calc_resultN(common::ObObj &result,
                           const common::ObObj *objs_array,
                           int64_t param_num,
                           common::ObExprCtx &expr_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int generate_pos_list(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPosList);
};

} // namespace oceanbase
} // namespace sql

#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_POS_LIST_H_