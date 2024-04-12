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
#include "lib/xml/ob_multi_mode_interface.h"
#include "lib/xml/ob_xml_tree.h"
#include "lib/xml/ob_xpath.h"

namespace oceanbase
{

namespace sql
{

enum ObUpdateXMLRetType: uint32_t {
  ObRetInputStr,
  ObRetNullType,
  ObRetMax
};

class ObExprUpdateXml : public ObFuncExprOperator
{
public:
  explicit ObExprUpdateXml(common::ObIAllocator &alloc);
  virtual ~ObExprUpdateXml();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_update_xml(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_mysql_update_xml(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr)
                      const override;
private:
  static int update_xml_tree_mysql(ObMulModeMemCtx* xml_mem_ctx,
                                   ObString xml_target,
                                   ObEvalCtx &ctx,
                                   ObString &xpath_str,
                                   ObIMulModeBase *&xml_tree,
                                   ObUpdateXMLRetType &res_origin);
  static int update_xml_child_text(ObXmlNode *old_node, ObXmlNode *text_node);
  static int update_xml_tree(ObMulModeMemCtx* xml_mem_ctx,
                             const ObExpr *expr,
                             ObEvalCtx &ctx,
                             ObString &xpath_str,
                             ObString &default_ns,
                             ObPathVarObject *prefix_ns,
                             ObIMulModeBase *xml_tree);
  static int update_xml_node(ObMulModeMemCtx* xml_mem_ctx, const ObExpr *expr, ObEvalCtx &ctx, ObIMulModeBase *node);
  // for text node and attribute
  static int update_text_or_attribute_node(ObMulModeMemCtx* xml_mem_ctx,
                                           ObXmlNode *xml_node,
                                           const ObExpr *expr,
                                           ObEvalCtx &ctx,
                                           bool is_text);
  static int update_attribute_node(ObIAllocator &allocator, ObXmlNode *xml_node, const ObExpr *expr, ObEvalCtx &ctx);
  static int update_attribute_xml_node(ObXmlNode *old_node, ObXmlNode *update_node);
  // for cdata and comment
  static int update_cdata_and_comment_node(ObMulModeMemCtx* xml_mem_ctx, ObXmlNode *xml_node, const ObExpr *expr, ObEvalCtx &ctx);
  // for namespace
  static int update_namespace_node(ObMulModeMemCtx* xml_mem_ctx, ObXmlNode *xml_node, const ObExpr *expr, ObEvalCtx &ctx);
  static int update_namespace_xml_node(ObIAllocator &allocator, ObXmlNode *old_node, ObXmlNode *update_node);
  static int update_namespace_value(ObIAllocator &allocator, ObXmlNode *xml_node, const ObString &ns_value);
  static int get_valid_default_ns_from_parent(ObXmlNode *cur_node, ObXmlAttribute* &default_ns);
  static int set_ns_recrusively(ObXmlNode *update_node, ObXmlAttribute *ns);
  static int update_new_nodes_ns(ObIAllocator &allocator, ObXmlNode *parent, ObXmlNode *update_node);
  static int update_exist_nodes_ns(ObXmlElement *parent, ObXmlAttribute *prefix_ns);
  // for element
  static int update_element_node(ObMulModeMemCtx* xml_mem_ctx, ObXmlNode *xml_node, const ObExpr *expr, ObEvalCtx &ctx);
  static int clear_element_child_node(ObXmlElement *ele_node);
  static int update_xml_child_node(ObIAllocator &allocator, ObXmlNode *old_node, ObXmlNode *update_node);
  static int remove_and_insert_element_node(ObXmlElement *ele_node, ObXmlNode *update_node, int64_t pos, bool is_remove);
  // for pi node
  static int update_pi_node(ObMulModeMemCtx* xml_mem_ctx, ObXmlNode *xml_node, const ObExpr *expr, ObEvalCtx &ctx);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUpdateXml);
};

} // sql
} // oceanbase


#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_UPDATE_XML_H