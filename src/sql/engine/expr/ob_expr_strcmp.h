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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_STRCMP_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_STRCMP_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprStrcmp: public ObRelationalExprOperator
{
public:
  ObExprStrcmp();
  explicit  ObExprStrcmp(common::ObIAllocator &alloc);
  virtual ~ObExprStrcmp() {};
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
              ObExpr &rt_expr) const override
  {
    return ObRelationalExprOperator::cg_expr(expr_cg_ctx, raw_expr, rt_expr);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprStrcmp);
};
}
}

#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_STRCMP_H_ */
