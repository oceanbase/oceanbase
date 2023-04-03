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

#ifndef _OB_EXPR_MOD_H_
#define _OB_EXPR_MOD_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprMod: public ObArithExprOperator
{
public:
  ObExprMod();
  explicit  ObExprMod(common::ObIAllocator &alloc);
  virtual ~ObExprMod() {};
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc(common::ObObj &res,
                  const common::ObObj &ojb1,
                  const common::ObObj &obj2,
                  common::ObIAllocator *allocator,
                  common::ObScale scale);

  static int mod_int_int(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int mod_int_uint(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int mod_uint_int(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int mod_uint_uint(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int mod_float(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int mod_double(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int mod_number(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  // temporary used, remove after all expr converted
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

private:
  OB_INLINE static int mod_int(common::ObObj &res,
                     const common::ObObj &left,
                     const common::ObObj &right,
                     common::ObIAllocator *allocator,
                     common::ObScale scale);
  OB_INLINE static int mod_uint(common::ObObj &res,
                      const common::ObObj &left,
                      const common::ObObj &right,
                      common::ObIAllocator *allocator,
                      common::ObScale scale);
  static int mod_float(common::ObObj &res,
                        const common::ObObj &left,
                        const common::ObObj &right,
                        common::ObIAllocator *allocator,
                        common::ObScale scale);
  static int mod_double(common::ObObj &res,
                        const common::ObObj &left,
                        const common::ObObj &right,
                        common::ObIAllocator *allocator,
                        common::ObScale scale);
  static int mod_number(common::ObObj &res,
                        const common::ObObj &left,
                        const common::ObObj &right,
                        common::ObIAllocator *allocator,
                        common::ObScale scale);
  DISALLOW_COPY_AND_ASSIGN(ObExprMod);
private:
  static ObArithFunc mod_funcs_[common::ObMaxTC];
};
}
}


#endif  /* _OB_EXPR_MOD_H_ */
