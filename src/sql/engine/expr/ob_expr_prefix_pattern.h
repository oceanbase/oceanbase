/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_PREFIX_PATTERN_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_PREFIX_PATTERN_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprPrefixPattern : public ObStringExprOperator
{
public:
  explicit  ObExprPrefixPattern(common::ObIAllocator &alloc);
  virtual ~ObExprPrefixPattern() {};

  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprResType &type3,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  static int eval_prefix_pattern(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

private:
  static int calc_prefix_pattern(const ObString &pattern,
                                  ObCollationType pattern_coll,
                                  const ObString &escape,
                                  ObCollationType escape_coll,
                                  int64_t prefix_len,
                                  int64_t &result_len,
                                  bool &is_valid);
  DISALLOW_COPY_AND_ASSIGN(ObExprPrefixPattern) const;
};
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_POWER_
