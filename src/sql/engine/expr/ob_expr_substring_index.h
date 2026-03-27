/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SUBSTRING_INDEX_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SUBSTRING_INDEX_

#include "lib/container/ob_array.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprSubstringIndex : public ObStringExprOperator
{
public:
  explicit  ObExprSubstringIndex(common::ObIAllocator &alloc);
  virtual ~ObExprSubstringIndex();
  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &str,
                                ObExprResType &delim,
                                ObExprResType &count,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_substring_index(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int eval_substring_index_batch(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const ObBitVector &skip,
                                        const int64_t batch_size);
  virtual bool need_rt_ctx() const override { return true; }
  DECLARE_SET_LOCAL_SESSION_VARS;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSubstringIndex);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SUBSTRING_INDEX_
