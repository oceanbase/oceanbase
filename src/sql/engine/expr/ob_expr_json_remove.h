/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_REMOVE_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_REMOVE_H_

#include "sql/engine/expr/ob_expr_operator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprJsonRemove : public ObFuncExprOperator
{
public:
  explicit ObExprJsonRemove(common::ObIAllocator &alloc);
  virtual ~ObExprJsonRemove();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx)
                                const override;
  static int eval_json_remove(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
virtual bool need_rt_ctx() const override { return true; }
  private:
    DISALLOW_COPY_AND_ASSIGN(ObExprJsonRemove);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_REMOVE_H_