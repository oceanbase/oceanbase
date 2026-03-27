/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_EXPR_VSIZE_H
#define OB_EXPR_VSIZE_H

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprVsize : public ObFuncExprOperator
{
public:
  explicit ObExprVsize(common::ObIAllocator& alloc);
  virtual ~ObExprVsize();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const;
  static int calc_vsize_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprVsize);

  bool is_blob_type(const common::ObObj &input) const;
};
}
}

#endif
