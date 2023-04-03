/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_BENCHMARK_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_BENCHMARK_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprBenchmark : public ObFuncExprOperator
{
public:
  explicit ObExprBenchmark(common::ObIAllocator &alloc);
  virtual ~ObExprBenchmark() {};
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const;

  static int eval_benchmark(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            ObDatum &expr_datum);

  static int eval_benchmark_batch(const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  const ObBitVector &skip,
                                  const int64_t batch_size);
private:
  static int collect_exprs(common::ObIArray<ObExpr *> &exprs,
                           const ObExpr &root_expr,
                           ObEvalCtx &ctx);
  static int clear_all_flags(common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx);
  DISALLOW_COPY_AND_ASSIGN(ObExprBenchmark);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_BENCHMARK_ */
