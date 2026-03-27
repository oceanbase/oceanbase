/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_STATEMENT_DIGEST_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_STATEMENT_DIGEST_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "share/object/ob_obj_cast.h"
namespace oceanbase
{
namespace sql
{

class ObExprStatementDigest : public ObStringExprOperator
{
public:
  explicit ObExprStatementDigest(common::ObIAllocator &alloc);
  virtual ~ObExprStatementDigest();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_statement_digest(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprStatementDigest);
};

class ObExprStatementDigestText : public ObStringExprOperator
{
public:
  explicit ObExprStatementDigestText(common::ObIAllocator &alloc);
  virtual ~ObExprStatementDigestText();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_statement_digest_text(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprStatementDigestText);
};

}
}
#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_STATEMENT_DIGEST_H_ */
