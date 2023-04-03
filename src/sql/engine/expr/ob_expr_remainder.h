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

#ifndef _OB_EXPR_REMAINDER_H_
#define _OB_EXPR_REMAINDER_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprRemainder: public ObArithExprOperator
{
public:
  ObExprRemainder();
  explicit  ObExprRemainder(common::ObIAllocator &alloc);
  virtual ~ObExprRemainder() {};
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc(common::ObObj &res,
                  const common::ObObj &ojb1,
                  const common::ObObj &obj2,
                  common::ObIAllocator *allocator,
                  common::ObScale scale);
  static int remainder_int64(const int64_t dividend, const int64_t divisor, int64_t& value);

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const;
  static int calc_remainder_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
private:
  static int remainder_float(common::ObObj &res,
                        const common::ObObj &left,
                        const common::ObObj &right,
                        common::ObIAllocator *allocator,
                        common::ObScale scale);
  static int remainder_double(common::ObObj &res,
                        const common::ObObj &left,
                        const common::ObObj &right,
                        common::ObIAllocator *allocator,
                        common::ObScale scale);
  static int remainder_number(common::ObObj &res,
                        const common::ObObj &left,
                        const common::ObObj &right,
                        common::ObIAllocator *allocator,
                        common::ObScale scale);
  DISALLOW_COPY_AND_ASSIGN(ObExprRemainder);
private:
  static ObArithFunc remainder_funcs_[common::ObMaxTC];
};
}
}


#endif  /* _OB_EXPR_MOD_H_ */
