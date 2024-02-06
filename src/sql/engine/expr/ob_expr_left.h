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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_LEFT_H_
#define OCEANBASE_SQL_ENGINE_EXPR_LEFT_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprLeft: public ObStringExprOperator
{
public:
	explicit ObExprLeft(common::ObIAllocator &alloc);
	virtual ~ObExprLeft();
	virtual int calc_result_type2(ObExprResType &type,
																ObExprResType &type1,
																ObExprResType &type2,
																common::ObExprTypeCtx &type_ctx) const;
  int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const;
private:
  int cast_param_type(const ObObj& in,
                      ObExprCtx& expr_ctx,
                      ObObj& out) const;
	DISALLOW_COPY_AND_ASSIGN(ObExprLeft);
};
} /* namespace sql */
} /* namespace oceanbase */

#endif /* OCEANBASE_SQL_ENGINE_EXPR_LEFT_H_ */
