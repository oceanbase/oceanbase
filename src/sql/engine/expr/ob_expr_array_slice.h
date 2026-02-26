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
 * This file contains implementation for array_slice.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ARRAY_SLICE
#define OCEANBASE_SQL_OB_EXPR_ARRAY_SLICE

#include "lib/udt/ob_array_type.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprArraySlice : public ObFuncExprOperator
{
public:
  explicit ObExprArraySlice(common::ObIAllocator &alloc);
  virtual ~ObExprArraySlice();

  virtual int calc_result_typeN(ObExprResType &type, ObExprResType *types, int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_array_slice(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_array_slice_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                    const int64_t batch_size);
  static int eval_array_slice_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                     const EvalBound &bound);

  static int get_subarray(ObIArrayType *&res_arr, ObIArrayType *src_arr, int64_t offset,
                          int64_t len, bool has_len);

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &expr) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprArraySlice);
};

} // namespace sql
} // namespace oceanbase

#endif