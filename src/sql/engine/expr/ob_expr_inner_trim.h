/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_SQL_EXPR_INNER_TRIM_H_
#define _OB_SQL_EXPR_INNER_TRIM_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprInnerTrim : public ObStringExprOperator
{
public:
  explicit  ObExprInnerTrim(common::ObIAllocator &alloc);
  virtual ~ObExprInnerTrim();
  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &trim_type,
                                ObExprResType &trim_pattern,
                                ObExprResType &text,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprInnerTrim);
};

}
}
#endif /* _OB_SQL_EXPR_TRIM_H_ */
