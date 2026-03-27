/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_PRIV_XML_BINARY_
#define OCEANBASE_SQL_OB_EXPR_PRIV_XML_BINARY_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprPrivXmlBinary : public ObFuncExprOperator
{
public:
  explicit ObExprPrivXmlBinary(common::ObIAllocator &alloc);
  virtual ~ObExprPrivXmlBinary() {}
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx)
                                const override;
  static int eval_priv_xml_binary(const ObExpr &expr, ObEvalCtx &ctx,  ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
    DISALLOW_COPY_AND_ASSIGN(ObExprPrivXmlBinary);
};
} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_PRIV_XML_BINARY_