/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_EXPR_SQL_MODE_CONVERT_H
#define _OB_EXPR_SQL_MODE_CONVERT_H

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprSqlModeConvert : public ObStringExprOperator
{
public:
  explicit  ObExprSqlModeConvert(common::ObIAllocator &alloc);
  virtual ~ObExprSqlModeConvert();
  virtual int calc_result_type1(ObExprResType &type, ObExprResType &type1, common::ObExprTypeCtx &type_ctx) const;
  static int sql_mode_convert(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSqlModeConvert);
};
}
}
#endif