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

#ifndef _OB_EXPR_NOT_H_
#define _OB_EXPR_NOT_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprNot : public ObLogicalExprOperator
{
public:
  explicit  ObExprNot(common::ObIAllocator &alloc);
  virtual ~ObExprNot() {};

  virtual int calc_result_type1(ObExprResType &type,
                        ObExprResType &type1,
                        common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_not(const ObExpr &expr,
                      ObEvalCtx &ctx,
                      ObDatum &expr_datum);
  static int eval_not_batch(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            const ObBitVector &skip,
                            const int64_t batch_size);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprNot) const;
};
}
}
#endif  /* _OB_EXPR_NOT_H_ */
