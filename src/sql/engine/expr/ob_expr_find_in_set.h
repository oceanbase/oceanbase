/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  static int calc_find_in_set_vector(VECTOR_EVAL_FUNC_ARG_DECL);
  DECLARE_SET_LOCAL_SESSION_VARS;

public:
  // hashmap is better than array when str_list is large
  // the threshold is set to 6 after a simple test
  static const int HASH_MAP_THRESHOLD = 6;

private:
  template <typename Arg0Vec, typename Arg1Vec, typename ResVec>
  static int calc_find_in_set_vector_dispatch(VECTOR_EVAL_FUNC_ARG_DECL);

  template <typename Arg0Vec, typename ResVec>
  static int calc_find_in_set_vector_dispatch(VECTOR_EVAL_FUNC_ARG_DECL);
  DISALLOW_COPY_AND_ASSIGN(ObExprFindInSet);
};

} /* namespace sql */
} /* namespace oceanbase */

#endif /* OCEANBASE_SQL_ENGINE_EXPR_FIND_IN_SET_H_ */
