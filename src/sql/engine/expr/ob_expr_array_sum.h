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
 * This file contains implementation for array_sum.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ARRAY_SUM
#define OCEANBASE_SQL_OB_EXPR_ARRAY_SUM

#include "lib/udt/ob_array_type.h"
#include "lib/udt/ob_collection_type.h"
#include "share/datum/ob_datum.h"
#include "share/vector/ob_i_vector.h"
#include "sql/engine/expr/ob_batch_eval_util.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprArraySum : public ObFuncExprOperator
{
public:
  explicit ObExprArraySum(common::ObIAllocator &alloc);
  virtual ~ObExprArraySum();

  virtual int calc_result_type1(ObExprResType &type, ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const override;

  static int get_array_data(ObString &data_str, ObCollectionArrayType *arr_type, uint32_t &len,
                            uint8_t *&null_bitmaps, const char *&data, uint32_t &data_len);

  static int get_array_data(ObIVector *len_vec, ObIVector *nullbitmap_vec, ObIVector *data_vec,
                            int64_t idx, ObCollectionArrayType *arr_type, uint32_t &len,
                            uint8_t *&null_bitmaps, const char *&data, uint32_t &data_len);

  static int get_data(const ObExpr &expr, ObEvalCtx &ctx, ObString &data_str, int64_t idx,
                      uint32_t &len, uint8_t *null_bitmaps, const char *data, uint32_t &data_len);

  static int eval_array_sum(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_array_sum_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                  const int64_t batch_size);
  static int eval_array_sum_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                   const EvalBound &bound);

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprArraySum);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_OB_ARRAY_SUM