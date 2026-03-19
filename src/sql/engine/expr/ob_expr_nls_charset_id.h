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

#ifndef OB_EXPR_NLS_CHARSET_ID_H
#define OB_EXPR_NLS_CHARSET_ID_H

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprNLSCharsetId : public ObFuncExprOperator
{
public:
  explicit ObExprNLSCharsetId(common::ObIAllocator &alloc);
  virtual ~ObExprNLSCharsetId() {}
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_nls_charset_id(const ObExpr &expr,
                                 ObEvalCtx &ctx,
                                 ObDatum &expr_datum);
  static int eval_nls_charset_id_vector(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const ObBitVector &skip,
                                        const EvalBound &bound);

private:
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprTypeCtx &type_ctx) const;
  DISALLOW_COPY_AND_ASSIGN(ObExprNLSCharsetId);
};

}
}
#endif // OB_EXPR_NLS_CHARSET_ID_H