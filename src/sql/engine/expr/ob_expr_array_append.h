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
 * This file contains implementation for array_append.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ARRAY_APPEND
#define OCEANBASE_SQL_OB_EXPR_ARRAY_APPEND

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/udt/ob_array_type.h"

namespace oceanbase
{
namespace sql
{
class ObExprArrayAppendCommon : public ObFuncExprOperator
{
public:
  explicit ObExprArrayAppendCommon(common::ObIAllocator &alloc, ObExprOperatorType type, const char *name);
  explicit ObExprArrayAppendCommon(common::ObIAllocator &alloc, ObExprOperatorType type,
                                       const char *name, int32_t param_num, int32_t dimension);
  virtual ~ObExprArrayAppendCommon();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_append(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, bool is_preappend = false);
  static int eval_append_batch(const ObExpr &expr, ObEvalCtx &ctx,
                               const ObBitVector &skip, const int64_t batch_size,
                               bool is_preappend = false);
  static int eval_append_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                const ObBitVector &skip, const EvalBound &bound,
                                bool is_preappend = false);

  static int append_elem(ObIAllocator &tmp_allocator, ObEvalCtx &ctx,
                         ObCollectionArrayType *arr_type, ObDatum *val_datum,
                         uint16_t val_subschema_id, ObIArrayType *val_arr,
                         ObIArrayType *res_arr);

  static int append_elem_vector(ObIAllocator &tmp_allocator, ObEvalCtx &ctx,
                                ObCollectionArrayType *arr_type, ObIVector *val_vec, int64_t idx,
                                uint16_t val_subschema_id, ObExpr &param_expr, ObIArrayType *val_arr,
                                ObIArrayType *res_arr);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprArrayAppendCommon);
};

class ObExprArrayAppend : public ObExprArrayAppendCommon
{
public:
  explicit ObExprArrayAppend(common::ObIAllocator &alloc);
  explicit ObExprArrayAppend(common::ObIAllocator &alloc, ObExprOperatorType type,
                             const char *name, int32_t param_num, int32_t dimension);
  virtual ~ObExprArrayAppend();
  static int eval_array_append(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_array_append_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                     const ObBitVector &skip, const int64_t batch_size);
  static int eval_array_append_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                      const ObBitVector &skip, const EvalBound &bound);

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprArrayAppend);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ARRAY_APPEND