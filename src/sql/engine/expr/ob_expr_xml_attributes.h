/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_XML_ATTRIBUTES_H_
#define OCEANBASE_SQL_OB_EXPR_XML_ATTRIBUTES_H_

#include "sql/engine/expr/ob_expr_operator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprXmlAttributes : public ObFuncExprOperator
{
public:
  explicit ObExprXmlAttributes(common::ObIAllocator &alloc);
  virtual ~ObExprXmlAttributes();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx)
                                const override;
  static int eval_xml_attributes(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprXmlAttributes);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_XML_ATTRIBUTES_H_