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

#ifndef OBDEV_SRC_SQL_ENGINE_EXPR_OB_EXPR_STMT_ID_H_
#define OBDEV_SRC_SQL_ENGINE_EXPR_OB_EXPR_STMT_ID_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprStmtId : public ObFuncExprOperator
{
public:
  explicit ObExprStmtId(common::ObIAllocator &alloc);
  virtual ~ObExprStmtId() {}

  virtual int calc_result_type0(ObExprResType &type,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_stmt_id(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprStmtId);

};
}//end namespace sql
}//end namespace oceanbase
#endif /* OBDEV_SRC_SQL_ENGINE_EXPR_OB_EXPR_STMT_ID_H_ */
