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

#ifndef OB_EXPR_SET
#define OB_EXPR_SET

#include "sql/engine/expr/ob_expr_operator.h"
#include "pl/ob_pl_user_type.h"

namespace oceanbase
{
namespace sql
{
class ObExprSet : public ObFuncExprOperator
{
public:
  explicit ObExprSet(common::ObIAllocator& alloc);
  virtual ~ObExprSet();
  int assign(const ObExprOperator &other);
  virtual int calc_result_type1(ObExprResType &type, ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                        const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const override;
  static int calc_set(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
#ifdef OB_BUILD_ORACLE_PL
  static int eval_coll(const common::ObObj &obj, ObExecContext &ctx, pl::ObPLNestedTable *&coll);
#endif

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSet);
};
}
}

#endif
