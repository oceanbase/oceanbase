/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_EXPR_ELT_H_
#define OB_EXPR_ELT_H_

#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprElt : public ObExprOperator
{
public:

  explicit  ObExprElt(common::ObIAllocator &alloc);
  virtual ~ObExprElt() {};

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *type_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_elt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  DECLARE_SET_LOCAL_SESSION_VARS;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprElt);
};

}
}


#endif /* OB_EXPR_ELT_H_ */
