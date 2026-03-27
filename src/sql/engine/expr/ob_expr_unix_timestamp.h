/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_UNIX_TIMESTAMP_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_UNIX_TIMESTAMP_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprUnixTimestamp : public ObFuncExprOperator
{
public:
  explicit  ObExprUnixTimestamp(common::ObIAllocator &alloc);
  virtual ~ObExprUnixTimestamp();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *type_array,
                                int64_t param,
                                common::ObExprTypeCtx &type_ctx) const;
  static int eval_unix_timestamp(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  int calc_result_type_literal(ObExprResType &type,
                               ObExprResType &type1) const;
  int calc_result_type_column(ObExprResType &type,
                              ObExprResType &type1) const;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprUnixTimestamp);
};
}
}

#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_UNIX_TIMESTAMP_H_ */
