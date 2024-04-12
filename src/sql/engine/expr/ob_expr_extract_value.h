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
 * This file is for func extractvalue.
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_EXRACTVALUE_H
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_EXRACTVALUE_H

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/xml/ob_xpath.h"

namespace oceanbase
{

namespace sql
{
class ObExprExtractValue : public ObFuncExprOperator
{
  public:
  explicit ObExprExtractValue(common::ObIAllocator &alloc);
  virtual ~ObExprExtractValue();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;

  static int eval_extract_value(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_mysql_extract_value(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr)
                      const override;
private:
static int extract_xpath_result(ObMulModeMemCtx *xml_mem_ctx, ObString& xpath_str, ObString& default_ns,
                         ObIMulModeBase* xml_doc, ObPathVarObject* prefix_ns, ObString &xml_res);
static int extract_mysql_xpath_result(ObMulModeMemCtx *xml_mem_ctx, ObString& xpath_str,
                                      ObIMulModeBase* xml_doc, ObStringBuffer &xml_res);
static int extract_node_value(ObIAllocator &allocator, ObIMulModeBase *node, ObString &xml_res);
static int has_same_parent_node(ObMulModeMemCtx *xml_mem_ctx, ObString& xpath_str, ObString& default_ns,
                         ObIMulModeBase* xml_doc, ObPathVarObject* prefix_ns, bool &is_same_parent);
static int merge_text_nodes_with_same_parent(ObIAllocator *allocator, ObIArray<ObIMulModeBase *> &result_nodes,
                                             ObString &xml_res);
static int append_text_into_buffer(ObIAllocator *allocator,
                                   ObIArray<ObIMulModeBase *> &result_nodes,
                                   ObStringBuffer &xml_res);
static int append_text_value(ObStringBuffer &buffer, ObIMulModeBase *node);
static int get_new_xpath(ObString xpath_str, ObString &new_xpath, bool &cal_count);

private:
    DISALLOW_COPY_AND_ASSIGN(ObExprExtractValue);
};

} // sql
} // oceanbase


#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_EXRACTVALUE_H