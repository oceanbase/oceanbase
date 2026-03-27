/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for _st_length.
 */
#ifndef OCEANBASE_SQL_OB_EXPR_ST_LENGTH_
#define OCEANBASE_SQL_OB_EXPR_ST_LENGTH_
#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/geo/ob_geo_utils.h"
using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{
class ObExprSTLength : public ObFuncExprOperator
{
public:
  explicit ObExprSTLength(common::ObIAllocator &alloc);
  virtual ~ObExprSTLength();
  virtual int calc_result_typeN(ObExprResType &type, ObExprResType *types, int64_t param_num,
      common::ObExprTypeCtx &type_ctx) const override;
  static int eval_st_length(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(
      ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSTLength);
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_EXPR_ST_LENGTH_
