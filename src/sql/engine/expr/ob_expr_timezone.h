/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TIMEZONE_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TIMEZONE_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprBaseTimezone : public ObFuncExprOperator
{
public:
  explicit  ObExprBaseTimezone(common::ObIAllocator &alloc, ObExprOperatorType type,
                               const char *name, const bool is_sessiontimezone);
  virtual ~ObExprBaseTimezone() {}
  virtual int calc_result_type0(ObExprResType &type, common::ObExprTypeCtx &type_ctx) const;

private:
  bool is_sessiontimezone_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprBaseTimezone);
};

class ObExprSessiontimezone : public ObExprBaseTimezone
{
public:
  explicit  ObExprSessiontimezone(common::ObIAllocator &alloc);
  virtual ~ObExprSessiontimezone() {}
  static int eval_session_timezone(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSessiontimezone);
};

class ObExprDbtimezone : public ObExprBaseTimezone
{
public:
  explicit  ObExprDbtimezone(common::ObIAllocator &alloc);
  virtual ~ObExprDbtimezone() {}
  static int eval_db_timezone(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDbtimezone);
};


}//sql
}//oceanbase
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TIMEZONE_ */
