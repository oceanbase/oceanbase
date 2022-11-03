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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_NULL_SAFE_EQUAL_H_
#define OCEANBASE_SQL_ENGINE_EXPR_NULL_SAFE_EQUAL_H_

#include "lib/ob_name_def.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprNullSafeEqual: public ObRelationalExprOperator
{
public:
  ObExprNullSafeEqual();
  explicit  ObExprNullSafeEqual(common::ObIAllocator &alloc);
  virtual ~ObExprNullSafeEqual() {};

  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;

  static int compare_row2(common::ObObj &result,
                  const common::ObNewRow *left_row,
                  const common::ObNewRow *right_row,
                  common::ObExprCtx &expr_ctx);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
              ObExpr &rt_expr) const override;

  static int ns_equal_eval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datm);
  static int row_ns_equal_eval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datm);

  static int ns_equal(const ObExpr &expr, ObDatum &res,
                      ObExpr **left, ObEvalCtx &lctx, ObExpr **right, ObEvalCtx &rctx);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprNullSafeEqual);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_EXPR_NULL_SAFE_EQUAL_H_ */
