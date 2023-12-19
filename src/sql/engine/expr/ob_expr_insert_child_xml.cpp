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

#include "ob_expr_insert_child_xml.h"
#include "ob_expr_lob_utils.h"
#include "lib/xml/ob_xml_parser.h"
#include "lib/xml/ob_xml_util.h"
#include "sql/engine/expr/ob_expr_xml_func_helper.h"
#include "lib/utility/utility.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

#define USING_LOG_PREFIX SQL_ENG


using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprInsertChildXml::ObExprInsertChildXml(common::ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_INSERTCHILDXML, N_INSERTCHILDXML, MORE_THAN_ONE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprInsertChildXml::~ObExprInsertChildXml() {}

int ObExprInsertChildXml::calc_result_typeN(ObExprResType &type,
                                            ObExprResType *types,
                                            int64_t param_num,
                                            common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (param_num != 5) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid argument number.", K(ret), K(param_num));
  } else if (!is_called_in_sql()) {
    ret = OB_ERR_SP_LILABEL_MISMATCH;
    LOG_WARN("expr call in pl semantics disallowed", K(ret), K(N_INSERTCHILDXML));
    LOG_USER_ERROR(OB_ERR_SP_LILABEL_MISMATCH, static_cast<int>(strlen(N_INSERTCHILDXML)), N_INSERTCHILDXML);
  } else {
    ObObjType in_type = types[0].get_type();
    if (types[0].is_ext() && types[0].get_udt_id() == T_OBJ_XML) {
      types[0].get_calc_meta().set_sql_udt(ObXMLSqlType);
    }

    if (ob_is_xml_pl_type(types[3].get_type(), types[3].get_udt_id())) {
      types[3].get_calc_meta().set_sql_udt(ObXMLSqlType);
    }

    if (OB_FAIL(ret)) {
    } else if (!ob_is_xml_sql_type(in_type, types[0].get_subschema_id())) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "ANYDATA", "-");
      LOG_WARN("inconsistent datatypes", K(ret), K(ob_obj_type_str(in_type)));
    } else if (!types[1].is_string_type()) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("inconsistent datatypes", K(ret), K(types[1].get_type()));
    } else if (!types[2].is_string_type()) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("inconsistent datatypes", K(ret), K(types[2].get_type()));
    } else if (!ob_is_string_tc(types[3].get_type()) &&
               !ob_is_xml_sql_type(types[3].get_type(), types[3].get_subschema_id())) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("inconsistent datatypes", K(ret), K(types[3].get_type()));
    } else if (!types[4].is_string_type() && !types[4].is_null()) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("inconsistent datatypes", K(ret), K(types[4].get_type()));
    }

    for (int8_t i = 1; OB_SUCC(ret) && i < param_num; i++) {
      ObObjType param_type = types[i].get_type();
      if (param_type == ObNullType) {
      } else if (ob_is_string_type(param_type)) {
        if (types[i].get_charset_type() != CHARSET_UTF8MB4) {
          types[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      }
    }

    if (OB_SUCC(ret)) {
      type.set_sql_udt(ObXMLSqlType);
    }
  }

  return ret;
}


int ObExprInsertChildXml::eval_insert_child_xml(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &allocator = tmp_alloc_g.get_allocator();
  ObDatum *xml_datum = NULL;
  ObIMulModeBase *xml_tree = NULL;
  ObString xpath_str;
  ObString child_str;
  ObString value_str;
  ObString namespace_str;
  ObString default_ns;
  ObPathVarObject prefix_ns(allocator);
  ObString xml_res;
  ObMulModeNodeType node_type = M_MAX_TYPE;
  ObPathExprIter xpath_iter(&allocator);
  bool is_insert_attributes = false;

  ObMulModeMemCtx* mem_ctx = nullptr;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "XMLModule"));
  if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(&allocator, mem_ctx))) {
    LOG_WARN("fail to create tree memory context", K(ret));
  } else if (OB_UNLIKELY(expr.arg_cnt_ != 5)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg_cnt_", K(ret), K(expr.arg_cnt_));
  } else if (ObNullType == expr.args_[1]->datum_meta_.type_) {
    ret = OB_ERR_INVALID_XPATH_EXPRESSION;
    LOG_WARN("invalid xpath expression", K(ret));
  } else if (OB_FAIL(ObXMLExprHelper::get_xmltype_from_expr(expr.args_[0], ctx, xml_datum))) {
    LOG_WARN("fail to get xmltype value", K(ret));
  } else if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr.args_[1], ctx, xpath_str, allocator))) {
    LOG_WARN("fail to get xpath str", K(ret));
  } else if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr.args_[2], ctx, child_str, allocator))) {
    LOG_WARN("fail to get xpath str", K(ret));
  } else if (ObNullType == expr.args_[4]->datum_meta_.type_) {
  } else if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr.args_[4], ctx, namespace_str, allocator))) {
    LOG_WARN("fail to get xpath str", K(ret));
  } else if (OB_FAIL(ObXMLExprHelper::construct_namespace_params(namespace_str, default_ns, prefix_ns, allocator))) {
    LOG_WARN("fail to construct namespace params", K(ret), K(namespace_str));
  }

  if (OB_FAIL(ret)) {
  } else if (xpath_str.empty()) {
    // do nothing
  } else if (OB_FAIL(check_child_expr(expr, ctx, allocator, mem_ctx, child_str, value_str, is_insert_attributes))) {
    LOG_WARN("failed to check child expr.", K(ret));
  } else if (OB_FAIL(ObXMLExprHelper::get_xml_base(mem_ctx, xml_datum, ObCollationType::CS_TYPE_INVALID, ObNodeMemType::TREE_TYPE, xml_tree, node_type, ObGetXmlBaseType::OB_IS_REPARSE))) {
    LOG_WARN("fail to parse xml doc", K(ret));
  } else if (OB_FAIL(xpath_iter.init(mem_ctx, xpath_str, default_ns, xml_tree, &prefix_ns))) {
    LOG_WARN("fail to init xpath iterator", K(xpath_str), K(default_ns), K(ret));
    ObXMLExprHelper::replace_xpath_ret_code(ret);
  } else if (OB_FAIL(insert_child_xml(expr, ctx, mem_ctx, allocator, xpath_iter, child_str, value_str, is_insert_attributes))) {
    LOG_WARN("fail to concat xpath result", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(xml_tree) || xml_tree->count() == 0) {
    res.set_null();
  } else {
    ObString plain_text;
    ObXmlDocument *xml_doc = NULL;
    ObXmlDocument *null_doc = NULL;
    ObStringBuffer buff(&allocator);
    int element_count = 0;
    int text_count = 0;
    int cdata_count = 0;
    if (ObMulModeNodeType::M_CONTENT == node_type) {
      node_type = ObMulModeNodeType::M_UNPARSED;
    } else if (OB_FAIL(xml_tree->get_node_count(ObMulModeNodeType::M_ELEMENT, element_count))) {
      LOG_WARN("get element count node failed", K(ret));
    } else if (OB_FAIL(xml_tree->get_node_count(ObMulModeNodeType::M_TEXT, text_count))) {
      LOG_WARN("get text count node failed", K(ret));
    } else if (OB_FAIL(xml_tree->get_node_count(ObMulModeNodeType::M_CDATA, cdata_count))) {
      LOG_WARN("get cdata count node failed", K(ret));
    } else if (element_count > 1 || element_count == 0) {
      node_type = ObMulModeNodeType::M_UNPARSED;
    } else if (element_count == 1 && (text_count > 0 || cdata_count > 0)) {
      node_type = ObMulModeNodeType::M_UNPARSED;
    } else {
      node_type = ObMulModeNodeType::M_DOCUMENT;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(xml_doc = static_cast<ObXmlDocument *>(xml_tree))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("xml tree to xmldocument failed", K(ret));
    } else if (ObMulModeNodeType::M_UNPARSED == node_type) {
      if (OB_FAIL(xml_doc->print(buff, ObXmlFormatType::NO_FORMAT))) {
        LOG_WARN("fail to print xml tree", K(ret));
      } else if (OB_FALSE_IT(plain_text.assign_ptr(buff.ptr(), buff.length()))) {
      } else if (OB_FAIL(ObXMLExprHelper::pack_xml_res(expr, ctx, res, null_doc, mem_ctx, node_type, plain_text))) {
        LOG_WARN("failed to pack xml res", K(ret));
      }
    } else if (OB_FAIL(ObXMLExprHelper::pack_xml_res(expr, ctx, res, xml_doc, mem_ctx, node_type, plain_text))) {
      LOG_WARN("failed to pack xml res", K(ret));
    }

  }

  return ret;
}

int ObExprInsertChildXml::insert_child_xml(const ObExpr &expr,
                                           ObEvalCtx &ctx,
                                           ObMulModeMemCtx* mem_ctx,
                                           ObArenaAllocator &allocator,
                                           ObPathExprIter &xpath_iter,
                                           ObString child_str,
                                           ObString value_str,
                                           bool is_insert_attributes)
{
  int ret = OB_SUCCESS;
  ObIMulModeBase *node = NULL;
  ObArray<ObIMulModeBase*> res_array;
  ObXmlElement *value_ele = NULL;
  ObIMulModeBase *value_doc = NULL;
  uint64_t ele_count = 0;
  uint64_t ele_index = 0;
  ObDatum *value_datum = NULL;
  CK(OB_NOT_NULL(expr.args_[3]));
  if (expr.args_[3]->datum_meta_.type_ == ObUserDefinedSQLType) {
    if (OB_FAIL(ObXMLExprHelper::get_xmltype_from_expr(expr.args_[3], ctx, value_datum))) {
      LOG_WARN("fail to get xmltype value", K(ret));
    } else if (OB_FAIL(ObXMLExprHelper::get_xml_base(mem_ctx, value_datum, CS_TYPE_INVALID, ObNodeMemType::TREE_TYPE, value_doc))) {
      LOG_WARN("fail to parse xml doc", K(ret));
    }

    for (uint64_t i = 0; OB_SUCC(ret) && i < value_doc->count(); i++) {
      CK(OB_NOT_NULL(value_doc->at(i)));
      ObMulModeNodeType type = value_doc->at(i)->type();
      if (type == M_COMMENT) {
      } else if (type == M_ELEMENT) {
        ele_count++;
        ele_index = i;
      } else {
        ret = OB_ERR_INVALID_XML_CHILD_NAME;
        LOG_WARN("value doc invalid.", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (ele_count > 1) {
      ret = OB_ERR_INVALID_XML_CHILD_NAME;
      LOG_WARN("value doc invalid.", K(ret));
    } else if (OB_ISNULL(value_ele = static_cast<ObXmlElement*>(value_doc->at(ele_index)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get value element failed.", K(ret));
    } else {
      ObString value_key;
      if (OB_FAIL(value_ele->get_key(value_key))) {
        LOG_WARN("get key failed.", K(ret));
      } else if (child_str.compare(value_key) != 0) {
        ret = OB_ERR_INVALID_XML_CHILD_NAME;
        LOG_WARN("child str and element key is't equal.", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(xpath_iter.open())) {
    LOG_WARN("fail to open xpath iterator", K(ret));
    ObXMLExprHelper::replace_xpath_ret_code(ret);
  }

  while (OB_SUCC(ret)) {
    if (OB_FAIL(xpath_iter.get_next_node(node))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get next xml node", K(ret));
      }
    } else if (node->type() != ObMulModeNodeType::M_ELEMENT &&
               node->type() != ObMulModeNodeType::M_CONTENT &&
               node->type() != ObMulModeNodeType::M_DOCUMENT) {
      // do nothing
    } else if (OB_FAIL(res_array.push_back(node))) {
      LOG_WARN("fail to push xml node", K(ret));
    }
  }

  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }

  for (int i = 0; OB_SUCC(ret) && i < res_array.size(); i++) {
    ObIMulModeBase* insert_node = res_array[i];
    if (OB_ISNULL(insert_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("xpath result node is null", K(ret));
    } else if (is_insert_attributes) { // add attributes
      if (OB_FAIL(insert_attributes_node(child_str, value_str, insert_node))) {
        LOG_WARN("fail to insert attributes node", K(ret), K(child_str), K(value_str));
      }
    } else {
      ObIMulModeBase *value_doc = NULL;
      if (OB_FAIL(ObXMLExprHelper::get_xml_base(mem_ctx, value_datum, CS_TYPE_INVALID, ObNodeMemType::TREE_TYPE, value_doc))) {
        LOG_WARN("fail to parse xml doc", K(ret));
      } else if (OB_FAIL(insert_element_node(allocator, insert_node, value_doc->at(ele_index)))) {
        LOG_WARN("fail to insert element node", K(ret));
      }
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = xpath_iter.close())) {
    LOG_WARN("fail to close xpath iter", K(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }

  return ret;
}

int ObExprInsertChildXml::insert_element_node(ObArenaAllocator &allocator,
                                              ObIMulModeBase *insert_node,
                                              ObIMulModeBase *value_node)
{
  int ret = OB_SUCCESS;
  ObMulModeNodeType insert_type = ObMulModeNodeType::M_MAX_TYPE;
  ObXmlElement *value_ele = NULL;
  if (OB_ISNULL(insert_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get insert node null", K(ret));
  } else if (OB_FALSE_IT(insert_type = insert_node->type())) {
  } else if (value_node->type() != M_ELEMENT || OB_ISNULL(value_ele = static_cast<ObXmlElement*>(value_node))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get value node null", K(ret));
  } else if (insert_type == M_ELEMENT) {
    ObXmlElement *insert = NULL;
    if (OB_ISNULL(insert = static_cast<ObXmlElement*>(insert_node))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get insert node null", K(ret));
    } else if (OB_FAIL(ObXMLExprHelper::update_new_nodes_ns(allocator, insert, value_ele))) {
      LOG_WARN("fail to update new node ns", K(ret));
    } else if (OB_FAIL(insert->add_element(value_ele))) {
      LOG_WARN("failed to add element.", K(ret));
    }
  } else if (insert_type == M_DOCUMENT || insert_type == M_CONTENT) {
    ObXmlDocument *insert = NULL;
    if (OB_ISNULL(insert = static_cast<ObXmlDocument*>(insert_node))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get insert node null", K(ret));
    } else if (OB_FAIL(insert->add_element(value_ele))) {
      LOG_WARN("failed to add element.", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert node type invalid.", K(ret), K(insert_type));
  }

  return ret;
}

int ObExprInsertChildXml::insert_attributes_node(ObString key_str,
                                                 ObString value_str,
                                                 ObIMulModeBase *insert_node)
{
  int ret = OB_SUCCESS;
  ObXmlElement *element = NULL;
  if (OB_ISNULL(insert_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get insert node null", K(ret));
  } else if (OB_ISNULL(element = static_cast<ObXmlElement*>(insert_node))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get insert node null", K(ret));
  } else {
    int64_t count = insert_node->attribute_size();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      ObXmlAttribute *att = nullptr;
      if (OB_ISNULL(insert_node->attribute_at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get attribute null", K(ret), K(i));
      } else if (insert_node->attribute_at(i)->type() != M_ATTRIBUTE) {
        // do nothing
      } else if (OB_ISNULL(att = static_cast<ObXmlAttribute*>(insert_node->attribute_at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cast to attribute null", K(ret), K(i));
      } else if (key_str.compare(att->get_key()) == 0) {
        ObString ele_err_info = element->get_key();
        ret = OB_ERR_XML_PARENT_ALREADY_CONTAINS_CHILD;
        LOG_WARN("Parent already contains child entry", K(ret), K(i), K(value_str), K(key_str));
        LOG_USER_ERROR(OB_ERR_XML_PARENT_ALREADY_CONTAINS_CHILD, ele_err_info.length(), ele_err_info.ptr(), "@", key_str.length(), key_str.ptr());
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(element->add_attr_by_str(key_str, value_str, ObMulModeNodeType::M_ATTRIBUTE))) {
      LOG_WARN("add element failed", K(ret), K(key_str), K(value_str));
    }
  }

  return ret;
}

int ObExprInsertChildXml::check_child_expr(const ObExpr &expr,
                                           ObEvalCtx &ctx,
                                           ObArenaAllocator &allocator,
                                           ObMulModeMemCtx* mem_ctx,
                                           ObString &child_str,
                                           ObString &value_str,
                                           bool &is_insert_attributes)
{
  int ret = OB_SUCCESS;
  const ObExpr *value_expr = expr.args_[3];
  ObXmlElement *value_ele = NULL;
  ObDatum *value_datum = NULL;
  ObIMulModeBase *value_doc = NULL;
  if (child_str.empty()) {
    ret = OB_ERR_INVALID_XML_CHILD_NAME;
    LOG_WARN("invalid xml child name.", K(ret));
  } else if (OB_ISNULL(value_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get value expr unexpected.", K(ret));
  } else if (value_expr->datum_meta_.type_ != ObUserDefinedSQLType) {
    if (child_str.ptr()[0] != '@') {
      if (is_first_char_attribute(child_str)) {
        ret = OB_ERR_XML_PARSE;
        LOG_WARN("child str invalid.", K(ret));
      } else {
        ret = OB_ERR_INVALID_XPATH_EXPRESSION;
        LOG_WARN("get invalid value type.", K(ret));
      }
    } else if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(value_expr, ctx, value_str, allocator))) {
      LOG_WARN("fail to get value str", K(ret));
    } else {
      is_insert_attributes = true;
      child_str = ObString(child_str.length() - 1, child_str.ptr() + 1);
    }
  }

  return ret;
}

bool ObExprInsertChildXml::is_first_char_attribute(ObString child_str)
{
  bool res = false;
  char *ptr = child_str.ptr();
  bool get_next_char = true;
  for (int i = 0; get_next_char && i < child_str.length(); i++) {
    if (ptr[i] == ' ') {
      // do nothing
    } else if (ptr[i] == '@') {
      res = true;
      get_next_char = false;
    } else {
      get_next_char = false;
    }
  }
  return res;
}

int ObExprInsertChildXml::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_insert_child_xml;
  return OB_SUCCESS;
}

} // end of sql
} // end of oceanbase