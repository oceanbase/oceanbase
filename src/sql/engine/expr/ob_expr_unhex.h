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

namespace oceanbase
{
namespace sql
{
class ObExprUnhex : public ObStringExprOperator
{
public:
  explicit  ObExprUnhex(common::ObIAllocator &alloc);
  virtual ~ObExprUnhex();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &text,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_unhex(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUnhex);
};

inline int ObExprUnhex::calc_result_type1(ObExprResType &type,
                                          ObExprResType &text,
                                          common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);

  if (!ob_is_text_tc(text.get_type())) {
    text.set_calc_type(common::ObVarcharType);
  }

  if (ObTinyTextType == text.get_type()) {
    const int32_t MAX_TINY_TEXT_BUFFER_SIZE = 383;
    type.set_varbinary();
    type.set_length(MAX_TINY_TEXT_BUFFER_SIZE);
  } else if (ObTextType == text.get_type()
      || ObMediumTextType == text.get_type()
      || ObLongTextType == text.get_type()) {
    type.set_type(ObLongTextType);
    type.set_length(OB_MAX_LONGTEXT_LENGTH);
  } else {
    type.set_varchar();
    type.set_length(text.get_length() / 2 + (text.get_length() % 2));
  }
  type.set_collation_level(common::CS_LEVEL_COERCIBLE);
  type.set_collation_type(common::CS_TYPE_BINARY);
  return ret;
}
}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_UNHEX_ */
