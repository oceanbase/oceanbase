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
 * This file contains implementation for array_position.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ARRAY_POSITION
#define OCEANBASE_SQL_OB_EXPR_ARRAY_POSITION

#include "lib/geo/ob_geo_utils.h"
#include "lib/udt/ob_array_type.h"
#include "lib/utility/ob_macro_utils.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprArrayPosition : public ObFuncExprOperator
{
public:
  explicit ObExprArrayPosition(common::ObIAllocator &alloc);
  virtual ~ObExprArrayPosition();

  virtual int calc_result_type2(ObExprResType &type, ObExprResType &type1, ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const override;

  static int eval_array_position(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_array_position_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                       const int64_t batch_size);
  static int eval_array_position_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                        const EvalBound &bound);

  static int array_position(const ObExpr &expr, ObIAllocator &alloc, ObEvalCtx &ctx,
                            ObIArrayType *src_arr, ObDatum *val_datum, int &idx);
  static int array_position_vector(const ObExpr &expr, ObIAllocator &alloc, ObEvalCtx &ctx,
                                   ObIArrayType *src_arr, ObIVector *val_vec, int vec_idx,
                                   int &idx);

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprArrayPosition);
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ARRAY_POSITION