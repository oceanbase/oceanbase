/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_TIMESTAMP_TO_SCN_H_
#define OCEANBASE_SQL_OB_EXPR_TIMESTAMP_TO_SCN_H_
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprTimestampToScn : public ObFuncExprOperator
{
public:
  explicit ObExprTimestampToScn(common::ObIAllocator &alloc);
  virtual ~ObExprTimestampToScn();
  virtual int calc_result_type1(ObExprResType &type, ObExprResType &usec,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprTimestampToScn);
};

} //sql
} //oceanbase
#endif //OCEANBASE_SQL_OB_EXPR_TIMESTAMP_TO_SCN_H_
