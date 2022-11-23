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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_IP2INT_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_IP2INT_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprIp2int : public ObFuncExprOperator
{
public:
  explicit  ObExprIp2int(common::ObIAllocator &alloc);
  virtual ~ObExprIp2int();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &text,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int ip2int_varchar(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  // helper func
  template <typename T>
  static int ip2int(T &result, const common::ObString &text);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprIp2int);
};

inline int ObExprIp2int::calc_result_type1(ObExprResType &type,
                                           ObExprResType &text,
                                           common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(text);
  type.set_int();
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
  //set calc type
  text.set_calc_type(common::ObVarcharType);
  return common::OB_SUCCESS;
}
}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_IP2INT_ */
