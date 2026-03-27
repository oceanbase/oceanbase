/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_EXRACT_XML_H
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_EXRACT_XML_H

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/xml/ob_xpath.h"
namespace oceanbase
{

namespace sql
{
class ObExprExtractXml : public ObFuncExprOperator
{
  public:
  explicit ObExprExtractXml(common::ObIAllocator &alloc);
  virtual ~ObExprExtractXml();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_extract_xml(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr)
                      const override;
private:
  static int check_and_set_res(const ObExpr &expr,
                               ObEvalCtx &ctx,
                               ObDatum &res,
                               ObLibTreeNodeVector &node_vector,
                               ObIAllocator &allocator);
private:
    DISALLOW_COPY_AND_ASSIGN(ObExprExtractXml);
};

} // sql
} // oceanbase


#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_EXRACT_XML_H