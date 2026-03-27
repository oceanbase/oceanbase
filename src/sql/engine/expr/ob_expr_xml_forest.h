/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_XML_FOREST_H_
#define OCEANBASE_SQL_OB_EXPR_XML_FOREST_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "lib/container/ob_vector.h"
#include "lib/xml/ob_xml_util.h"

namespace oceanbase
{
namespace sql
{
class ObExprXmlForest  : public ObFuncExprOperator
{
public:
  explicit ObExprXmlForest(common::ObIAllocator &alloc);
  virtual ~ObExprXmlForest();
  virtual int calc_result_typeN(
      ObExprResType& type,
      ObExprResType* types,
      int64_t param_num,
      common::ObExprTypeCtx& type_ctx)
      const override;

  static int eval_xml_forest(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

  virtual int cg_expr(
      ObExprCGCtx &expr_cg_ctx,
      const ObRawExpr &raw_expr,
      ObExpr &rt_expr)
      const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprXmlForest);
};
} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_XML_FOREST_H_
