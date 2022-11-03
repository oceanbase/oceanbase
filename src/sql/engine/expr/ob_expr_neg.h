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

#ifndef _OB_EXPR_NEG_H_
#define _OB_EXPR_NEG_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprNeg : public ObExprOperator
{
  typedef common::ObExprStringBuf IAllocator;
public:
  explicit  ObExprNeg(common::ObIAllocator &alloc);
  virtual ~ObExprNeg() {};

  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  static int calc_param_type(const ObExprResType &param_type,
                             common::ObObjType &calc_type,
                             common::ObObjType &result_type);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprNeg) const;

};
}
}
#endif  /* _OB_EXPR_NEG_H_ */
