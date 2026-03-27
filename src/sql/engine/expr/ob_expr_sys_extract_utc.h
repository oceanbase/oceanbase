/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_SYS_EXTRACT_UTC_H_
#define OCEANBASE_SQL_OB_EXPR_SYS_EXTRACT_UTC_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprSysExtractUtc : public ObFuncExprOperator
{
public:
  explicit  ObExprSysExtractUtc(common::ObIAllocator &alloc);
  virtual ~ObExprSysExtractUtc() {}
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &date,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const;
  static int calc_sys_extract_utc(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprSysExtractUtc);
};

} //sql
} //oceanbase
#endif //OCEANBASE_SQL_OB_EXPR_SYS_EXTRACT_UTC_H_
