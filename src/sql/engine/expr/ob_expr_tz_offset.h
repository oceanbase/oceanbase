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

#ifndef OCEANBASE_SQL_OB_EXPR_TZ_OFFSET_H_
#define OCEANBASE_SQL_OB_EXPR_TZ_OFFSET_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprTzOffset : public ObFuncExprOperator
{
public:
  explicit  ObExprTzOffset(common::ObIAllocator &alloc);
  virtual ~ObExprTzOffset() {}
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &input,
                                common::ObExprTypeCtx &type_ctx) const;

  static int eval_tz_offset_val(const common::ObString &in_tz_str,
                                       ObSQLSessionInfo &my_session,
                                       common::ObIAllocator &alloc,
                                       common::ObString &res_str);
  static int eval_tz_offset(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &expr) const override;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprTzOffset);
};

} //sql
} //oceanbase
#endif //OCEANBASE_SQL_OB_EXPR_TZ_OFFSET_H_
