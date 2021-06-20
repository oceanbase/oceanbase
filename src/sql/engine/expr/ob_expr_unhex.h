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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_UNHEX_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_UNHEX_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {
class ObExprUnhex : public ObStringExprOperator {
public:
  explicit ObExprUnhex(common::ObIAllocator& alloc);
  virtual ~ObExprUnhex();
  virtual int calc_result_type1(ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const;
  static int calc(common::ObObj& result, const common::ObObj& text, common::ObCastCtx& cast_ctx);
  virtual int calc_result1(common::ObObj& result, const common::ObObj& text, common::ObExprCtx& expr_ctx) const;
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  static int eval_unhex(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUnhex);
};

inline int ObExprUnhex::calc_result_type1(
    ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);

  text.set_calc_type(common::ObVarcharType);

  type.set_varchar();
  type.set_length(text.get_length() / 2 + (text.get_length() % 2));
  type.set_collation_level(common::CS_LEVEL_COERCIBLE);
  type.set_collation_type(common::CS_TYPE_BINARY);
  return common::OB_SUCCESS;
}
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_UNHEX_ */
