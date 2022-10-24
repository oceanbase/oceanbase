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

#ifndef _OCEANBASE_SQL_OB_EXPR_GET_USER_VAR_H_
#define _OCEANBASE_SQL_OB_EXPR_GET_USER_VAR_H_
#include "lib/ob_name_def.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace sql
{
class ObExprGetUserVar : public ObFuncExprOperator
{
public:
  explicit  ObExprGetUserVar(common::ObIAllocator &alloc);
  virtual ~ObExprGetUserVar();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &expr) const override;
  static int eval_get_user_var(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprGetUserVar);
};

} //sql
} //oceanbase
#endif //_OCEANBASE_SQL_OB_EXPR_GET_USER_VAR_H_
