/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_SQL_EXPR_KEYVALUE_H_
#define _OB_SQL_EXPR_KEYVALUE_H_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprKeyValue : public ObStringExprOperator
{
public:
  explicit ObExprKeyValue(common::ObIAllocator &alloc);

  virtual ~ObExprKeyValue();

  virtual int calc_result_typeN(ObExprResType &type,
                                 ObExprResType *types,
                                 int64_t param_num,
                                 common::ObExprTypeCtx &type_ctx) const;


  virtual int cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_key_value_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                        ObDatum &res);
  static int calc_key_value_expr_vector(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprKeyValue);
};
}
}

#endif /* _OB_SQL_EXPR_KEYVALUE_H_ */