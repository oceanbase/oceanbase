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

#ifndef _OB_EXPR_DIV_H_
#define _OB_EXPR_DIV_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprDiv: public ObArithExprOperator
{
public:
  ObExprDiv();
  explicit  ObExprDiv(common::ObIAllocator &alloc, ObExprOperatorType type = T_OP_DIV);
  virtual ~ObExprDiv() {}
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc(common::ObObj &res,
                  const common::ObObj &obj1,
                  const common::ObObj &obj2,
                  common::ObIAllocator *allocator,
                  common::ObScale calc_scale);
  static int calc_for_avg(common::ObObj &res,
                          const common::ObObj &obj1,
                          const common::ObObj &obj2,
                          common::ObExprCtx &expr_ctx,
                          common::ObScale res_scale);
  static int calc_for_avg(common::ObDatum &result,
                          const common::ObDatum &sum,
                          const int64_t count,
                          ObEvalCtx &expr_ctx,
                          const common::ObObjType type);

  static int div_float(EVAL_FUNC_ARG_DECL);
  static int div_float_batch(BATCH_EVAL_FUNC_ARG_DECL);
  static int div_double(EVAL_FUNC_ARG_DECL);
  static int div_double_batch(BATCH_EVAL_FUNC_ARG_DECL);
  static int div_number(EVAL_FUNC_ARG_DECL);
  static int div_number_batch(BATCH_EVAL_FUNC_ARG_DECL);
  static int div_intervalym_number(EVAL_FUNC_ARG_DECL);
  static int div_intervalym_number_batch(BATCH_EVAL_FUNC_ARG_DECL);
  static int div_intervalds_number(EVAL_FUNC_ARG_DECL);
  static int div_intervalds_number_batch(BATCH_EVAL_FUNC_ARG_DECL);

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int div_float(common::ObObj &res,
                       const common::ObObj &left,
                       const common::ObObj &right,
                       common::ObIAllocator *allocator,
                       common::ObScale scale);
  static int div_double(common::ObObj &res,
                        const common::ObObj &left,
                        const common::ObObj &right,
                        common::ObIAllocator *allocator,
                        common::ObScale scale);
  static int div_double_no_overflow(common::ObObj &res,
                                    const common::ObObj &left,
                                    const common::ObObj &right,
                                    common::ObIAllocator *allocator,
                                    common::ObScale scale);
   static int div_number(common::ObObj &res,
                        const common::ObObj &left,
                        const common::ObObj &right,
                        common::ObIAllocator *allocator,
                        common::ObScale calc_scale);
  static int div_interval(common::ObObj &res,
                          const common::ObObj &left,
                          const common::ObObj &right,
                          common::ObIAllocator *allocator,
                          common::ObScale calc_scale);

  DISALLOW_COPY_AND_ASSIGN(ObExprDiv);
private:
  static ObArithFunc div_funcs_[common::ObMaxTC];
  static ObArithFunc avg_div_funcs_[common::ObMaxTC];
  static const common::ObScale DIV_CALC_SCALE;
  static const common::ObScale DIV_MAX_CALC_SCALE;
};

// Div expr for aggregation, different with ObExprDiv:
//  No overflow check for double type.
class ObExprAggDiv : public ObExprDiv
{
public:
  explicit ObExprAggDiv(common::ObIAllocator &alloc)
      : ObExprDiv(alloc, T_OP_AGG_DIV)
  {
  }
};

}
}
#endif  /* _OB_EXPR_DIV_H_ */
