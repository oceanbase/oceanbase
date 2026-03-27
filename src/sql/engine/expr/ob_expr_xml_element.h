/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for xmlelement.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_XML_ELEMENT_H_
#define OCEANBASE_SQL_OB_EXPR_XML_ELEMENT_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/json_type/ob_json_base.h" // for ObIJsonBase may need json for kv pairs
#include "lib/json_type/ob_json_bin.h" // for ObJsonBin
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "lib/container/ob_vector.h"
#include "lib/xml/ob_xml_util.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class
ObExprXmlElement : public ObFuncExprOperator
{
public:
  explicit ObExprXmlElement(common::ObIAllocator &alloc);
  virtual ~ObExprXmlElement();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx)
                                const override;
  static int eval_xml_element(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprXmlElement);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_XML_ELEMENT_H_