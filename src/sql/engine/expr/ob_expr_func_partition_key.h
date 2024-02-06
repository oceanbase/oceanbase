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

#ifndef OCEANBASE_SQL_OB_EXPR_FUNC_PARTITION_KEY_H_
#define OCEANBASE_SQL_OB_EXPR_FUNC_PARTITION_KEY_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprFuncPartKey : public ObFuncExprOperator
{
public:
  explicit  ObExprFuncPartKey(common::ObIAllocator &alloc);
  virtual ~ObExprFuncPartKey();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;
  static int calc_partition_key(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_partition_key_batch(BATCH_EVAL_FUNC_ARG_DECL);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprFuncPartKey);

};

}  // namespace sql
}  // namespace oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_FUNC_PARTITION_KEY_H_
