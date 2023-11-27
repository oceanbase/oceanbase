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
 * This file is for func insertchildxml.
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_INSERT_CHILD_XML_H
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_INSERT_CHILD_XML_H

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/xml/ob_xpath.h"

namespace oceanbase
{

namespace sql
{
class ObExprInsertChildXml : public ObFuncExprOperator
{
public:
  explicit ObExprInsertChildXml(common::ObIAllocator &alloc);
  virtual ~ObExprInsertChildXml();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;

  static int eval_insert_child_xml(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr)
                      const override;
private:
  static int insert_child_xml(const ObExpr &expr,
                              ObEvalCtx &ctx,
                              ObMulModeMemCtx* mem_ctx,
                              ObArenaAllocator &allocator,
                              ObPathExprIter &xpath_iter,
                              ObString child_str,
                              ObString value_str,
                              bool is_insert_attributes);

  static int check_child_expr(const ObExpr &expr,
                              ObEvalCtx &ctx,
                              ObArenaAllocator &allocator,
                              ObMulModeMemCtx* mem_ctx,
                              ObString &child_str,
                              ObString &value_str,
                              bool &is_insert_attributes);
  static bool is_first_char_attribute(ObString child_str);

  static int insert_element_node(ObArenaAllocator &allocator, ObIMulModeBase *insert_node, ObIMulModeBase *value_node);

  static int insert_attributes_node(ObString key_str,
                                    ObString value_str,
                                    ObIMulModeBase *insert_node);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprInsertChildXml);
};

} // sql
} // oceanbase


#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_EXRACTVALUE_H