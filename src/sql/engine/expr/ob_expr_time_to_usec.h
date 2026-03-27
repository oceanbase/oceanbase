/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_TIME_TO_USEC_H_
#define OCEANBASE_SQL_OB_EXPR_TIME_TO_USEC_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprTimeToUsec : public ObFuncExprOperator
{
public:
  explicit  ObExprTimeToUsec(common::ObIAllocator &alloc);
  virtual ~ObExprTimeToUsec();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &date,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const override;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprTimeToUsec);
};

} //sql
} //oceanbase
#endif //OCEANBASE_SQL_OB_EXPR_TIME_TO_USEC_H_
