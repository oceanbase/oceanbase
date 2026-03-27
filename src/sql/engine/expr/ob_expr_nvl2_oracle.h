/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_SQL_ENGINE_EXPR_NVL2_ORACLE_H_
#define _OCEANBASE_SQL_ENGINE_EXPR_NVL2_ORACLE_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_nvl.h"

namespace oceanbase
{
namespace sql
{
using namespace oceanbase::common;
class ObExprNvl2Oracle: public ObFuncExprOperator
{
public:
  explicit ObExprNvl2Oracle(ObIAllocator &alloc);
  virtual ~ObExprNvl2Oracle();

  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprResType &type3,
                                ObExprTypeCtx &type_ctx) const;
  static int calc_nvl2_oracle_expr_batch(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          const ObBitVector &skip,
                                          const int64_t batch_size);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprNvl2Oracle);
};
} // namespace sql
} // namespace oceanbase

#endif
