/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for array_first.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ARRAY_FIRST
#define OCEANBASE_SQL_OB_EXPR_ARRAY_FIRST

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_array_map.h"
#include "lib/udt/ob_array_type.h"

namespace oceanbase
{
namespace sql
{
class ObExprArrayFirst : public ObExprArrayMapCommon
{
public:
  explicit ObExprArrayFirst(common::ObIAllocator &alloc);
  explicit ObExprArrayFirst(common::ObIAllocator &alloc, ObExprOperatorType type,
                                const char *name, int32_t param_num, int32_t dimension);
  virtual ~ObExprArrayFirst();

  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx) const override;
  static int eval_array_first(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprArrayFirst);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ARRAY_FIRST