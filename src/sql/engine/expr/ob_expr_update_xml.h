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
 * This file is for func updatexml.
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_UPDATE_XML_H
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_UPDATE_XML_H

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{

namespace sql
{

class ObExprUpdateXml : public ObFuncExprOperator
{
public:
  explicit ObExprUpdateXml(common::ObIAllocator &alloc);
  virtual ~ObExprUpdateXml();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_update_xml(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) { return OB_NOT_SUPPORTED; }
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr)
                      const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUpdateXml);
};

} // sql
} // oceanbase


#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_UPDATE_XML_H