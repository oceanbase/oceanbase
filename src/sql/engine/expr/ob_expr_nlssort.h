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

#ifndef OB_EXPR_NLSSORT_H
#define OB_EXPR_NLSSORT_H

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprNLSSort : public ObFuncExprOperator
{
public:
  explicit ObExprNLSSort(common::ObIAllocator &alloc);
  virtual ~ObExprNLSSort() {}
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_nlssort(const ObExpr &expr,
                          ObEvalCtx &ctx,
                          ObDatum &expr_datum);
  bool need_rt_ctx() const override { return true; }

private:
  static int convert_to_coll_code(ObEvalCtx &ctx,
                        const ObCollationType &from_type,
                        const ObString &from_str,
                        const ObCollationType &to_type,
                        ObString &to_str);
  static int eval_nlssort_inner(const ObExpr &expr,
                                ObEvalCtx &ctx,
                                ObDatum &expr_datum,
                                const ObCollationType &coll_type,
                                const ObCollationType &arg0_coll_type,
                                const ObObjType &arg0_obj_type,
                                ObString input_str);

  DISALLOW_COPY_AND_ASSIGN(ObExprNLSSort) const;
};

class ObExprNLSSORTParseCtx : public ObExprOperatorCtx
{
public:
  ObExprNLSSORTParseCtx() : coll_type_(common::CS_TYPE_INVALID) {}
  ObCollationType coll_type_;
};

}
}
#endif // OB_EXPR_NLSSORT_H
