/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
