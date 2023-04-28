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
 * This file is for func xmlcast.
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_XMLCAST_H
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_XMLCAST_H

#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{

namespace sql
{

class ObExprXmlcast : public ObFuncExprOperator
{
  public:
  explicit ObExprXmlcast(common::ObIAllocator &alloc);
  virtual ~ObExprXmlcast();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;

  static int eval_xmlcast(const ObExpr &expr, ObEvalCtx &ctx,  ObDatum &res) { return OB_NOT_SUPPORTED; }
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr)
                      const override;
private:
  int set_dest_type(ObExprResType &param_type, ObExprResType &dst_type, ObExprTypeCtx &type_ctx) const;
private:
    DISALLOW_COPY_AND_ASSIGN(ObExprXmlcast);
};
} // sql
} // oceanbase


#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_XMLCAST_H