/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_CHAR_TO_ROWID_
#define OCEANBASE_SQL_ENGINE_EXPR_CHAR_TO_ROWID_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprCharToRowID : public ObStringExprOperator
{
public:
  explicit  ObExprCharToRowID(common::ObIAllocator &alloc);
  virtual ~ObExprCharToRowID();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &text,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_char_to_rowid(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprCharToRowID);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_CHAR_TO_ROWID_ */
