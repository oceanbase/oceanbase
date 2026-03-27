/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_RANDOM_BYTES_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_RANDOM_BYTES_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprRandomBytes : public ObFuncExprOperator
{
public:
  explicit ObExprRandomBytes(common::ObIAllocator &alloc);
  virtual ~ObExprRandomBytes();
  virtual int calc_result_type1(ObExprResType &type, 
                                ObExprResType &len,
                                common::ObExprTypeCtx &type_ctx) const;
  
  virtual int calc_result1(common::ObObj &result,
                           const common::ObObj &len,
                           common::ObExprCtx &expr_ctx) const;
  
  static int generate_random_bytes(const ObExpr &expr, ObEvalCtx &ctx, 
                                   ObDatum &expr_datum);
  
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRandomBytes);
};
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_RANDOM_BYTES_