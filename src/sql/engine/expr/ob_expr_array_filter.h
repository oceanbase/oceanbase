/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for array_filter.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ARRAY_FILTER
#define OCEANBASE_SQL_OB_EXPR_ARRAY_FILTER

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_array_map.h"
#include "lib/udt/ob_array_type.h"

namespace oceanbase
{
namespace sql
{
class ObExprArrayFilter : public ObExprArrayMapCommon
{
public:
  explicit ObExprArrayFilter(common::ObIAllocator &alloc);
  explicit ObExprArrayFilter(common::ObIAllocator &alloc, ObExprOperatorType type,
                           const char *name, int32_t param_num, int32_t dimension);
  virtual ~ObExprArrayFilter();

  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx) const override;
  static int eval_array_filter(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int fill_array_by_filter(ObArenaAllocator &allocator, ObIArrayType *filter_arr,
                                  ObIArrayType *src_arr, ObIArrayType *res_arr);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprArrayFilter);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ARRAY_FILTER