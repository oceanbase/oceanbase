/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_EXPR_NEG_H_
#define _OB_EXPR_NEG_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprNeg : public ObExprOperator
{
  typedef common::ObExprStringBuf IAllocator;
public:
  explicit  ObExprNeg(common::ObIAllocator &alloc);
  virtual ~ObExprNeg() {};

  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  static int calc_param_type(const ObExprResType &param_type,
                             common::ObObjType &calc_type,
                             common::ObObjType &result_type);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprNeg) const;

};
}
}
#endif  /* _OB_EXPR_NEG_H_ */
