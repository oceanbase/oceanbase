/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_ROWNUM_H_
#define OCEANBASE_SQL_ENGINE_EXPR_ROWNUM_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase
{
namespace sql
{
class ObExprCGCtx;
class ObExprRowNum: public ObFuncExprOperator
{
  OB_UNIS_VERSION(1);
public:
  explicit ObExprRowNum(common::ObIAllocator &alloc);
	virtual ~ObExprRowNum();
  virtual int calc_result_type0(ObExprResType &type, common::ObExprTypeCtx &type_ctx) const;
  // CG && evaluating function for static typing engine
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int rownum_eval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  void set_op_id(uint64_t operator_id) { operator_id_ = operator_id; }
private:
  uint64_t operator_id_;
  DISALLOW_COPY_AND_ASSIGN(ObExprRowNum);
};

} /* namespace sql */
} /* namespace oceanbase */

#endif /* OCEANBASE_SQL_ENGINE_EXPR_ROWNUM_H_ */
