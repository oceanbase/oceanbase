/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  static int eval_coll_composite(ObExecContext &exec_ctx, const common::ObObj &obj, pl::ObPLNestedTable *&result);
#endif

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSet);
};
}
}

#endif
