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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_HASH_H_
#define OCEANBASE_SQL_ENGINE_EXPR_HASH_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprHash : public ObExprOperator {
public:
  explicit ObExprHash(common::ObIAllocator &alloc);
  virtual ~ObExprHash();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc_hash_value_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
  static int calc_hash_value_expr_batch(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const ObBitVector &skip,
                                        const int64_t batch_size);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  // hard code seed, 24bit max prime number
  static const int64_t HASH_SEED = 16777213;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprHash);
};
}  // namespace sql
}  // namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_EXPR_HASH_H_ */
