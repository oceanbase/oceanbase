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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_FIND_IN_SET_H_
#define OCEANBASE_SQL_ENGINE_EXPR_FIND_IN_SET_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprFindInSet: public ObFuncExprOperator {
public:
	explicit ObExprFindInSet(common::ObIAllocator &alloc);
	virtual ~ObExprFindInSet();
	virtual int calc_result_type2(ObExprResType &type,
																ObExprResType &type1,
																ObExprResType &type2,
																common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const;
  virtual bool need_rt_ctx() const override
  {
    return true;
  }
  static int calc_find_in_set_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
	 DISALLOW_COPY_AND_ASSIGN(ObExprFindInSet);
};

} /* namespace sql */
} /* namespace oceanbase */

#endif /* OCEANBASE_SQL_ENGINE_EXPR_FIND_IN_SET_H_ */
