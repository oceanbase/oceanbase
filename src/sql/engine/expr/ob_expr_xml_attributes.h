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
 * This file is for func xmlattributes.
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
#ifdef OB_BUILD_ORACLE_XML
  static int eval_xml_attributes(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
#else
  static int eval_xml_attributes(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) { return OB_NOT_SUPPORTED; }
#endif
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprXmlAttributes);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_XML_ATTRIBUTES_H_