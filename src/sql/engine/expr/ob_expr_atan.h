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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_ATAN_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_ATAN_

#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprAtan : public ObFuncExprOperator
{
public:
  explicit  ObExprAtan(common::ObIAllocator &alloc);
  virtual ~ObExprAtan();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t type_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  static int eval_number_atan_vector(VECTOR_EVAL_FUNC_ARG_DECL);
  static int eval_double_atan_vector(VECTOR_EVAL_FUNC_ARG_DECL);

private:
  template <typename Arg0Vec, typename ResVec, bool IS_DOUBLE>
  static int calc_one_param_vector(VECTOR_EVAL_FUNC_ARG_DECL);

  template <typename Arg0Vec, typename Arg1Vec, typename ResVec>
  static int calc_two_param_vector(VECTOR_EVAL_FUNC_ARG_DECL);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprAtan);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_HEX_ */
