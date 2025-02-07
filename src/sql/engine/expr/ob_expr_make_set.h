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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_MAKE_SET_H_
#define OCEANBASE_SQL_ENGINE_EXPR_MAKE_SET_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprMakeSet: public ObStringExprOperator {
public:
	explicit ObExprMakeSet(common::ObIAllocator &alloc);
	virtual ~ObExprMakeSet();
	virtual int calc_result_typeN(ObExprResType &type,
																ObExprResType *types,
																int64_t param_num,
																common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const;
  static int calc_make_set_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int calc_text(const ObExpr &expr, ObEvalCtx &ctx, const ObDatum &input_bits_dat, ObDatum &res);
  DECLARE_SET_LOCAL_SESSION_VARS;

private:
	DISALLOW_COPY_AND_ASSIGN(ObExprMakeSet);
};

} /* namespace sql */
} /* namespace oceanbase */

#endif /* OCEANBASE_SQL_ENGINE_EXPR_MAKE_SET_H_ */
