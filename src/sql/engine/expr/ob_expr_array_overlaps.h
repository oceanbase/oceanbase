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
 * This file contains implementation for array_overlaps.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ARRAY_OVERLAPS
#define OCEANBASE_SQL_OB_EXPR_ARRAY_OVERLAPS

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/udt/ob_array_type.h"


namespace oceanbase
{
namespace sql
{
class ObExprArrayOverlaps : public ObFuncExprOperator
{
public:

  enum Relation {
    OVERLAPS = 0,
    CONTAINS_ALL = 1,
  };
  explicit ObExprArrayOverlaps(common::ObIAllocator &alloc);
  explicit ObExprArrayOverlaps(common::ObIAllocator &alloc, ObExprOperatorType type, 
                                const char *name, int32_t param_num, int32_t dimension);
  virtual ~ObExprArrayOverlaps();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_array_overlaps(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_array_overlaps_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);
  static int eval_array_overlaps_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                        const ObBitVector &skip, const EvalBound &bound);
  static int eval_array_relations(const ObExpr &expr, ObEvalCtx &ctx, Relation relation, ObDatum &res);
  static int eval_array_relations_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                        const int64_t batch_size, Relation relation);
  static int eval_array_relation_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                        const ObBitVector &skip, const EvalBound &bound,
                                        Relation relation);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  
  DISALLOW_COPY_AND_ASSIGN(ObExprArrayOverlaps);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ARRAY_OVERLAPS