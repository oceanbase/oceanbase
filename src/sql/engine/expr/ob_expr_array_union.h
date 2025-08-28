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
 * This file contains implementation for array_union.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ARRAY_UNION
#define OCEANBASE_SQL_OB_EXPR_ARRAY_UNION

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/udt/ob_array_type.h"

namespace oceanbase
{
namespace sql
{
class ObExprArraySetOperation : public ObFuncExprOperator
{
public:
  enum SetOperation {
    UNIONINZE = 0,
    INTERSECT = 1,
    EXCEPT = 2,
  };
  explicit ObExprArraySetOperation(common::ObIAllocator &alloc, 
              ObExprOperatorType type, 
              const char *name, int32_t param_num, 
              int32_t dimension);
  virtual ~ObExprArraySetOperation();
  virtual int calc_result_type2(ObExprResType &type, 
                  ObExprResType &type1, 
                  ObExprResType &type2,
                  common::ObExprTypeCtx &type_ctx) const override;
  virtual int calc_result_typeN(ObExprResType& type,
                  ObExprResType* types,
                  int64_t param_num, 
                  common::ObExprTypeCtx& type_ctx) const override;
  static int eval_array_set_operation(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, SetOperation operation);
  static int eval_array_set_operation_batch(const ObExpr &expr, 
                ObEvalCtx &ctx,
                const ObBitVector &skip, 
                const int64_t batch_size,
                SetOperation operation);
  static int eval_array_set_operation_vector(const ObExpr &expr, 
                ObEvalCtx &ctx,
                const ObBitVector &skip, 
                const EvalBound &bound,
                SetOperation operation);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprArraySetOperation);
};

class ObExprArrayUnion : public ObExprArraySetOperation
{
public:
  explicit ObExprArrayUnion(common::ObIAllocator &alloc);
  virtual ~ObExprArrayUnion();
  static int eval_array_union(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_array_union_batch(const ObExpr &expr, 
                ObEvalCtx &ctx,
                const ObBitVector &skip, 
                const int64_t batch_size);
  static int eval_array_union_vector(const ObExpr &expr, 
                ObEvalCtx &ctx,
                const ObBitVector &skip, 
                const EvalBound &bound);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                  const ObRawExpr &raw_expr,
                  ObExpr &rt_expr) const override;
private: 
  DISALLOW_COPY_AND_ASSIGN(ObExprArrayUnion);
};

} // sql
} // oceanbase
#endif //OCEANBASE_SQL_OB_EXPR_ARRAY_UNION