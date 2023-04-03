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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_BIT_LENGTH_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_BIT_LENGTH_H_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprBitLength : public ObFuncExprOperator {
public:
  ObExprBitLength();
  explicit ObExprBitLength(common::ObIAllocator& alloc);
  virtual ~ObExprBitLength();
  virtual int calc_result_type1(ObExprResType& type, 
                                ObExprResType& text,
                                common::ObExprTypeCtx& type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_null(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_bit_length(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  private:
    DISALLOW_COPY_AND_ASSIGN(ObExprBitLength);                         
};
}
}
#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_BIT_LENGTH_H_ */
