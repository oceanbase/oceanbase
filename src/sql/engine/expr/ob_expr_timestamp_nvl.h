/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_SQL_EXPR_TIMESTAMP_NVL_H_
#define _OB_SQL_EXPR_TIMESTAMP_NVL_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprTimestampNvl : public ObStringExprOperator
{
public:
  explicit  ObExprTimestampNvl(common::ObIAllocator &alloc);
  virtual ~ObExprTimestampNvl();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_timestampnvl(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprTimestampNvl);
};
}
}
#endif /* _OB_SQL_EXPR_TIMESTAMP_NVL_H_ */
