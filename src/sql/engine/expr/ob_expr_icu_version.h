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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_ICU_VERSION_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_ICU_VERSION_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
// MySQL function
// The version of the International Components for Unicode (ICU) library used to support
// regular expression operations. This function is primarily intended for use in test cases.
class ObExprICUVersion : public ObStringExprOperator
{
public:
  explicit ObExprICUVersion(common::ObIAllocator &alloc);
  virtual ~ObExprICUVersion();
  virtual int calc_result_type0(ObExprResType &type, common::ObExprTypeCtx &type_ctx) const;

  static int eval_version(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
};
}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_OBVERSION_ */
