/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_FUNC_RANDSTR_H_
#define OCEANBASE_SQL_OB_EXPR_FUNC_RANDSTR_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprRandstr: public ObFuncExprOperator
{
public:
	explicit ObExprRandstr(common::ObIAllocator &alloc);
	virtual ~ObExprRandstr();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &len,
                                ObExprResType &seed,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  static int calc_random_str(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
	// disallow copy
	DISALLOW_COPY_AND_ASSIGN(ObExprRandstr);
};

} /* namespace sql */
} /* namespace oceanbase */

#endif /* OCEANBASE_SQL_OB_EXPR_FUNC_RANDSTR_H_ */
