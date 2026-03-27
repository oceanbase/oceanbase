/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_SQL_EXPR_GREATEST_H_
#define _OB_SQL_EXPR_GREATEST_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_least.h"

namespace oceanbase
{
namespace sql
{
class ObExprGreatest : public ObExprLeastGreatest
{
public:
  explicit  ObExprGreatest(common::ObIAllocator &alloc);
  virtual ~ObExprGreatest() {}
  static int calc_greatest(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprGreatest);
};


}
}
#endif /* _OB_SQL_EXPR_GREATEST_H_ */
