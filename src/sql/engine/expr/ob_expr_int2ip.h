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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_INT2IP_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_INT2IP_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprInt2ip : public ObStringExprOperator
{
public:
  explicit  ObExprInt2ip(common::ObIAllocator &alloc);
  virtual ~ObExprInt2ip();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &text,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc(common::ObObj &result,
                  const common::ObObj &text,
                  common::ObExprStringBuf &string_buf);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int int2ip_varchar(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  // helper func
  static int int2ip(common::ObObj &result,
                    const int64_t text,
                    common::ObExprStringBuf &string_buf);
  static int int2ip(common::ObDatum &result,
                    const int64_t text);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprInt2ip);
};

inline int ObExprInt2ip::calc_result_type1(ObExprResType &type,
                                           ObExprResType &text,
                                           common::ObExprTypeCtx &type_ctx) const
{
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
  text.set_calc_type(common::ObIntType);
  type.set_varchar();
  type.set_length(common::MAX_IP_ADDR_LENGTH);
  type.set_collation_level(common::CS_LEVEL_COERCIBLE);
  type.set_default_collation_type();
  return common::OB_SUCCESS;
}

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_INT2IP_ */
