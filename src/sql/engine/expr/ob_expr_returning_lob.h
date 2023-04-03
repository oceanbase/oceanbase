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

#ifndef OCEANBASE_EXPR_OB_EXPR_RETURNING_LOB_H_
#define OCEANBASE_EXPR_OB_EXPR_RETURNING_LOB_H_
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_res_type.h"
namespace oceanbase
{
namespace sql
{
// eval_lob() is add for dml returning lob_locator obj
// this expr is only for those type sql:
// create table t1 (c1 clob, c2 blob);
// insert into t1 values(empty_clob(), 'aaaa') returning c1, c2, concat(c1, '2');
// returning c1 and c2 expr is ObExprReturningLob expr, but concat(c1, '2') is longtext type
class ObExprReturningLob : public ObFuncExprOperator
{
public:
  explicit ObExprReturningLob(common::ObIAllocator &alloc);
  virtual ~ObExprReturningLob() {}
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &arg,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int calc_result_typeN(ObExprResType &type,
                                  ObExprResType *types,
                                  int64_t param_num,
                                  common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_lob(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprReturningLob);
};
} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_EXPR_OB_EXPR_RETURNING_LOB_H_
