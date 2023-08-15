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
 * This file is for func extract(xml).
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_EXRACT_XML_H
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_EXRACT_XML_H

#include "sql/engine/expr/ob_expr_operator.h"
#ifdef OB_BUILD_ORACLE_XML
#include "lib/xml/ob_xpath.h"
#endif
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
#ifdef OB_BUILD_ORACLE_XML
  static int eval_extract_xml(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
#else
  static int eval_extract_xml(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) { return OB_NOT_SUPPORTED; }
#endif
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr)
                      const override;
#ifdef OB_BUILD_ORACLE_XML
private:
  static int append_node_to_res(ObIAllocator &allocator,ObXmlDocument *root, ObIMulModeBase *node);
  static int concat_xpath_result(ObPathExprIter &xpath_iter,
                                 ObCollationType cs_type,
                                 ObXmlDocument *&root,
                                 ObMulModeNodeType &node_type,
                                 ObMulModeMemCtx* ctx);
  static int check_and_set_res(const ObExpr &expr,
                               ObEvalCtx &ctx,
                               ObDatum &res,
                               ObLibTreeNodeVector &node_vector,
                               ObIAllocator &allocator);
#endif
private:
    DISALLOW_COPY_AND_ASSIGN(ObExprExtractXml);
};

} // sql
} // oceanbase


#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_EXRACT_XML_H