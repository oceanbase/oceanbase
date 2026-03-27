/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR__CONV_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR__CONV_H_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprConv : public ObStringExprOperator
{
public:
  explicit  ObExprConv(common::ObIAllocator &alloc);
  virtual ~ObExprConv();
  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprResType &type3,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_conv(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  static const int16_t MIN_BASE = 2;
  static const int16_t MAX_BASE = 36;
  static const int16_t MAX_LENGTH = 65;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprConv);
};
}
}

#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR__CONV_H_ */
