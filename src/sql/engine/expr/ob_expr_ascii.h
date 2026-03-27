/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_ASCII_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_ASCII_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprAscii : public ObFuncExprOperator
{
public:

  explicit  ObExprAscii(common::ObIAllocator &alloc);
  virtual ~ObExprAscii() {};
  static int calc(common::ObObj &obj,
                  const common::ObObj &obj1,
                  common::ObExprCtx &expr_ctx);

  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const;
  static int calc_ascii_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprAscii);
};

class ObExprOrd : public ObFuncExprOperator
{
public:

  explicit  ObExprOrd(common::ObIAllocator &alloc);
  virtual ~ObExprOrd() {};
  static int calc(common::ObObj &obj,
                  const common::ObObj &obj1,
                  common::ObExprCtx &expr_ctx);

  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const;

  static int calc_ord_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprOrd);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_ASCII_
