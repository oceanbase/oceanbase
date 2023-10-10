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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_HELLO_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_HELLO_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {

class ObExprHello: public ObStringExprOperator
{
public:
  explicit ObExprHello(common::ObIAllocator& alloc);
  virtual ~ObExprHello() = default;

  virtual int calc_result_type0(ObExprResType& type, common::ObExprTypeCtx& type_ctx) const override;
  static int eval(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
private:
  static const char hello_world[];
  static const int64_t hello_len;
  DISABLE_COPY_ASSIGN(ObExprHello);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_HELLO_ */
