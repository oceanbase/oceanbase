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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_INT_DIV_H_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_INT_DIV_H_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprIntDiv: public ObArithExprOperator
{
public:
  ObExprIntDiv();
  explicit  ObExprIntDiv(common::ObIAllocator &alloc);
  virtual ~ObExprIntDiv() {};
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc(common::ObObj &res,
                  const common::ObObj &ojb1,
                  const common::ObObj &obj2,
                  common::ObIAllocator *allocator,
                  common::ObScale scale);
  static int div_int_int(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int div_int_uint(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int div_uint_int(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int div_uint_uint(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int div_number(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

  // temporary used, remove after all expr converted
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

private:
  OB_INLINE static int intdiv_int(common::ObObj &res,
                        const common::ObObj &left,
                        const common::ObObj &right,
                        common::ObIAllocator *allocator,
                        common::ObScale scale);
  OB_INLINE static int intdiv_uint(common::ObObj &res,
                         const common::ObObj &left,
                         const common::ObObj &right,
                         common::ObIAllocator *allocator,
                         common::ObScale scale);
  static int intdiv_number(common::ObObj &res,
                           const common::ObObj &left,
                           const common::ObObj &right,
                           common::ObIAllocator *allocator,
                           common::ObScale scale);
  static ObArithFunc int_div_funcs_[common::ObMaxTC];
  OB_INLINE static bool is_int_int_out_of_range(int64_t val1, int64_t val2, int64_t res)
  {
    // top digit:
    // 0 div 0     : safe.
    // 0 div 1     : safe.
    // 1 div 0     : safe.
    // 1 div 1 = 0 : safe.
    // 1 div 1 = 1 : overflow.
    return (val1 >> 63)  && (val2 >> 63) && (res >> 63);
  }
  DISALLOW_COPY_AND_ASSIGN(ObExprIntDiv);
};
}// sql
}// oceanbase


#endif  /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_INT_DIV_H_ */
