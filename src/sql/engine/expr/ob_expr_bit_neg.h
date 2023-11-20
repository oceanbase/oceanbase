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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_BIT_NEG_
#define OCEANBASE_SQL_ENGINE_EXPR_BIT_NEG_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprBitNeg : public ObBitwiseExprOperator
{
public:
  explicit  ObExprBitNeg(common::ObIAllocator &alloc);
  virtual ~ObExprBitNeg() {};

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_bitneg_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum& res_datum);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprBitNeg);
};
}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_BIT_NEG_ */

