/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_EMPTY_LOB_H_
#define OCEANBASE_SQL_OB_EXPR_EMPTY_LOB_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprEmptyClob : public ObFuncExprOperator
{
public:
  explicit  ObExprEmptyClob(common::ObIAllocator &alloc);
  virtual ~ObExprEmptyClob();
  virtual int calc_result_type0(ObExprResType &type, common::ObExprTypeCtx &type_ctx) const;
  static int eval_empty_clob(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprEmptyClob);
};

class ObExprEmptyBlob : public ObFuncExprOperator
{
public:
  explicit  ObExprEmptyBlob(common::ObIAllocator &alloc);
  virtual ~ObExprEmptyBlob();
  virtual int calc_result_type0(ObExprResType &type, common::ObExprTypeCtx &type_ctx) const;
  static int eval_empty_blob(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprEmptyBlob);
};

}
}

#endif // OCEANBASE_SQL_OB_EXPR_EMPTY_LOB_H_

