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
 * This file is for func deletexml.
 */

#include "ob_expr_xml_delete_xml.h"
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

ObExprDeleteXml::ObExprDeleteXml(common::ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_DELETEXML, N_DELETEXML, MORE_THAN_ONE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprDeleteXml::~ObExprDeleteXml() {}

int ObExprDeleteXml::calc_result_typeN(ObExprResType &type,
                                       ObExprResType *types,
                                       int64_t param_num,
                                       common::ObExprTypeCtx &type_ctx) const
{
  INIT_SUCC(ret);

  if (param_num != 3) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid argument number.", K(ret), K(param_num));
  } else if (!is_called_in_sql()) {
    ret = OB_ERR_SP_LILABEL_MISMATCH;
    LOG_WARN("expr call in pl semantics disallowed", K(ret), K(N_DELETEXML));
    LOG_USER_ERROR(OB_ERR_SP_LILABEL_MISMATCH, static_cast<int>(strlen(N_DELETEXML)), N_DELETEXML);
  } else {
    ObObjType in_type = types[0].get_type();
    if (types[0].is_ext() && types[0].get_udt_id() == T_OBJ_XML) {
      types[0].get_calc_meta().set_sql_udt(ObXMLSqlType);
    } else if (!ob_is_xml_sql_type(in_type, types[0].get_subschema_id())) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "ANYDATA", "-");
      LOG_WARN("inconsistent datatypes", K(ret), K(ob_obj_type_str(in_type)));
    } else if (!types[1].is_string_type()) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("inconsistent datatypes", K(ret), K(types[1].get_type()));
    } else if (!types[2].is_string_type() && !types[2].is_null()) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("inconsistent datatypes", K(ret), K(types[2].get_type()));
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

int ObExprDeleteXml::eval_delete_xml(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &allocator = tmp_alloc_g.get_allocator();
  ObDatum *xml_datum = NULL;
  ObIMulModeBase *xml_tree = NULL;
  ObXmlDocument *xml_doc = NULL;
  ObString xpath_str;
  ObString namespace_str;
  ObString default_ns;
  ObPathVarObject prefix_ns(allocator);
  ObMulModeNodeType node_type = M_MAX_TYPE;
  ObPathExprIter xpath_iter(&allocator);
  bool should_reparse = false;

  ObMulModeMemCtx* mem_ctx = nullptr;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "XMLModule"));
  if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(&allocator, mem_ctx))) {
    LOG_WARN("fail to create tree memory context", K(ret));
  } else if (OB_UNLIKELY(expr.arg_cnt_ != 3)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg_cnt_", K(ret), K(expr.arg_cnt_));
  } else if (ObNullType == expr.args_[1]->datum_meta_.type_) {
    ret = OB_ERR_INVALID_XPATH_EXPRESSION;
    LOG_WARN("invalid xpath expression", K(ret));
  } else if (OB_FAIL(ObXMLExprHelper::get_xmltype_from_expr(expr.args_[0], ctx, xml_datum))) {
    LOG_WARN("fail to get xmltype value", K(ret));
  } else if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr.args_[1], ctx, xpath_str, allocator))) {
    LOG_WARN("fail to get xpath str", K(ret));
  } else if (ObNullType == expr.args_[2]->datum_meta_.type_) {
  } else if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr.args_[2], ctx, namespace_str, allocator))) {
    LOG_WARN("fail to get xpath str", K(ret));
  } else if (OB_FAIL(ObXMLExprHelper::construct_namespace_params(namespace_str, default_ns, prefix_ns, allocator))) {
    LOG_WARN("fail to construct namespace params", K(ret), K(namespace_str));
  }

  if (OB_FAIL(ret)) {
  } else if (xpath_str.empty()) {
    // do nothing
  } else if (OB_FAIL(ObXMLExprHelper::get_xml_base(mem_ctx, xml_datum, ObCollationType::CS_TYPE_INVALID, ObNodeMemType::TREE_TYPE, xml_tree, node_type, ObGetXmlBaseType::OB_IS_REPARSE))) {
    LOG_WARN("fail to parse xml doc", K(ret));
  } else if (OB_ISNULL(xml_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get xml tree null", K(ret));
  } else if (OB_FAIL(xpath_iter.init(mem_ctx, xpath_str, default_ns, xml_tree, &prefix_ns))) {
    LOG_WARN("fail to init xpath iterator", K(xpath_str), K(default_ns), K(ret));
    ObXMLExprHelper::replace_xpath_ret_code(ret);
  } else if (OB_FAIL(delete_xml(xpath_iter, should_reparse))) {
    LOG_WARN("delete xml failed.", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(xml_tree) || xml_tree->count() == 0) {
    res.set_null();
  } else {
    ObString plain_text;
    int element_count = 0;
    int text_count = 0;
    ObStringBuffer buff(&allocator);

    for (int i = 0; OB_SUCC(ret) && i < xml_tree->count(); i++) {
      ObIMulModeBase *child_node = xml_tree->at(i);
      if (OB_ISNULL(child_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("xml tree child null", K(ret), K(i));
      } else if (ObMulModeNodeType::M_TEXT == child_node->type() ||
                 ObMulModeNodeType::M_CDATA == child_node->type()) {
        text_count++;
      } else if (ObMulModeNodeType::M_ELEMENT == child_node->type()) {
        element_count++;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (element_count == 0) {
      res.set_null();
    } else if (node_type == M_CONTENT) {
      node_type = ObMulModeNodeType::M_UNPARSED;
    } else if (element_count > 1) {
      node_type = ObMulModeNodeType::M_UNPARSED;
    } else if (element_count == 1 && text_count > 0) {
      node_type = ObMulModeNodeType::M_UNPARSED;
    } else {
      node_type = ObMulModeNodeType::M_DOCUMENT;
    }

    if (OB_FAIL(ret) || element_count == 0) {
    } else if (OB_ISNULL(xml_doc = static_cast<ObXmlDocument *>(xml_tree))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("xml tree to xmldocument failed", K(ret));
    } else if (should_reparse) {
      ObString plain_text;
      if (OB_FAIL(xml_doc->print(buff, ObXmlFormatType::NO_FORMAT))) {
        LOG_WARN("fail to print xml tree", K(ret));
      } else if (OB_FALSE_IT(plain_text.assign_ptr(buff.ptr(), buff.length()))) {
      } else if (node_type == ObMulModeNodeType::M_DOCUMENT &&
                 OB_FAIL(ObXmlParserUtils::parse_document_text(mem_ctx, plain_text, xml_doc))) {
        if (ret == OB_ERR_PARSER_SYNTAX) {
          ret = OB_ERR_XML_PARSE;
        }
        LOG_WARN("parsing document failed", K(ret), K(plain_text));
      } else if (node_type != ObMulModeNodeType::M_DOCUMENT &&
                 OB_FAIL(ObXmlParserUtils::parse_content_text(mem_ctx, plain_text, xml_doc))) {
        if (ret == OB_ERR_PARSER_SYNTAX) {
          ret = OB_ERR_XML_PARSE;
        }
        LOG_WARN("parsing document failed", K(ret), K(plain_text));
      } else if (OB_FAIL(ObXMLExprHelper::pack_xml_res(expr, ctx, res, xml_doc, mem_ctx, node_type, plain_text))) {
        LOG_WARN("failed to pack xml res", K(ret));
      }
    } else if (ObMulModeNodeType::M_UNPARSED == node_type) {
      if (OB_FAIL(xml_doc->print(buff, ObXmlFormatType::NO_FORMAT))) {
        LOG_WARN("fail to print xml tree", K(ret));
      } else if (OB_FALSE_IT(plain_text.assign_ptr(buff.ptr(), buff.length()))) {
      } else if (OB_FAIL(ObXMLExprHelper::pack_xml_res(expr, ctx, res, NULL, mem_ctx, node_type, plain_text))) {
        LOG_WARN("failed to pack xml res", K(ret));
      }
    } else if (OB_FAIL(ObXMLExprHelper::pack_xml_res(expr, ctx, res, xml_doc, mem_ctx, node_type, plain_text))) {
      LOG_WARN("failed to pack xml res", K(ret));
    }
  }

  return ret;
}

int ObExprDeleteXml::delete_xml(ObPathExprIter &xpath_iter, bool &should_reparse)
{
  INIT_SUCC(ret);
  ObIMulModeBase *node = NULL;
  ObArray<ObIMulModeBase*> res_array;

  if (OB_FAIL(xpath_iter.open())) {
    LOG_WARN("fail to open xpath iterator", K(ret));
    ObXMLExprHelper::replace_xpath_ret_code(ret);
  }

  while (OB_SUCC(ret)) {
    if (OB_FAIL(xpath_iter.get_next_node(node))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get next xml node", K(ret));
      }
    } else if (node->type() == ObMulModeNodeType::M_CONTENT) {
      ret = OB_ERR_XML_NOT_SUPPORT_OPERATION;
      LOG_USER_ERROR(OB_ERR_XML_NOT_SUPPORT_OPERATION, "fragment");
      LOG_WARN("XML node '' (type=fragment) does not support this operation", K(ret));
    } else if (node->type() == ObMulModeNodeType::M_DOCUMENT) {
      ret = OB_ERR_XML_NOT_SUPPORT_OPERATION;
      LOG_USER_ERROR(OB_ERR_XML_NOT_SUPPORT_OPERATION, "document");
      LOG_WARN("XML node '' (type=document) does not support this operation", K(ret));
    } else if (OB_FAIL(res_array.push_back(node))) {
      LOG_WARN("fail to push xml node", K(ret));
    }
  }

  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }

  for (int i = 0; OB_SUCC(ret) && i < res_array.size(); i++) {
    ObMulModeNodeType delete_type = M_MAX_TYPE;
    ObXmlNode *delete_node = NULL;
    if (OB_ISNULL(res_array[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("xpath result node is null", K(ret));
    } else if (OB_ISNULL(delete_node = static_cast<ObXmlNode*>(res_array[i]))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("xpath result node xmlnode is null", K(ret));
    } else {
      delete_type = delete_node->type();
      switch (delete_type) {
        case M_TEXT:
        case M_COMMENT:
        case M_CDATA:
        case M_ELEMENT:
        case M_INSTRUCT: {
          if (OB_FAIL(delete_leaf_node(delete_node))) {
            LOG_WARN("delete leaf node failed.", K(ret), K(delete_type));
          }
          break;
        }
        case M_ATTRIBUTE: {
          if (OB_FAIL(delete_attribute_node(delete_node))) {
            LOG_WARN("delete attributes failed.", K(ret));
          }
          break;
        }
        case M_NAMESPACE: {
          should_reparse = true;
          if (OB_FAIL(delete_namespace_node(delete_node))) {
            LOG_WARN("delete namespace failed.", K(ret));
          }
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupported xml node type", K(ret), K(delete_type));
          break;
        }
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

int ObExprDeleteXml::delete_namespace_node(ObXmlNode *delete_node)
{
  INIT_SUCC(ret);
  ObString key;
  int64_t pos = -1;
  ObXmlNode *parent = NULL;
  ObXmlElement *ele_node = NULL;
  ObXmlAttribute *ns_node = NULL;

  if (OB_ISNULL(delete_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get delete node null.", K(ret));
  } else if (M_NAMESPACE != delete_node->type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get delete node null.", K(ret));
  } else if (OB_ISNULL(ns_node = static_cast<ObXmlAttribute*>(delete_node))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("delete node cast to attributes node null.", K(ret));
  } else if (OB_ISNULL(parent = ns_node->get_parent())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attribute parent node is NULL", K(ret));
  } else if (OB_FALSE_IT(key = ns_node->get_key())) {
  } else if (parent->type() != M_ELEMENT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parent of namespace node is not an element node", K(ret), K(parent->type()));
  } else if (FALSE_IT(ele_node = static_cast<ObXmlElement *>(parent))) {
  } else if (OB_FAIL(ele_node->get_attribute_pos(ns_node->type(), key, pos))) {
    LOG_WARN("fail to get attribute pos", K(ret), K(key), K(ns_node->type()));
  } else if (OB_FAIL(ele_node->remove_namespace(pos))) {
    LOG_WARN("remove namespace failed.", K(ret), K(pos));
  }

  return ret;
}

int ObExprDeleteXml::delete_attribute_node(ObXmlNode *delete_node)
{
  INIT_SUCC(ret);
  int64_t pos = -1;
  ObXmlNode *parent = NULL;
  ObXmlElement *ele_node = NULL;
  ObString key;

  if (OB_ISNULL(delete_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get delete node null.", K(ret));
  } else if (OB_FALSE_IT(key = delete_node->get_key())) {
  } else if (OB_ISNULL(parent = delete_node->get_parent())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("xml node get parent is null", K(ret));
  } else if (parent->type() != M_ELEMENT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parent of attribute node is not an element node", K(ret), K(parent->type()));
  } else if (OB_ISNULL(ele_node = static_cast<ObXmlElement*>(parent))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get parent cast to element node null.", K(ret));
  } else if (OB_FAIL(ele_node->get_attribute_pos(delete_node->type(), key, pos))) {
    LOG_WARN("get attributes pos failed.", K(ret), K(key), K(delete_node->type()));
  } else if (OB_FAIL(ele_node->remove_attribute(pos))) {
    LOG_WARN("failed to remove attributes.", K(ret), K(pos));
  }

  return ret;
}

int ObExprDeleteXml::delete_leaf_node(ObXmlNode *delete_node)
{
  INIT_SUCC(ret);
  int64_t pos = -1;
  ObXmlNode *parent = NULL;

  if (OB_ISNULL(delete_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get delete node null.", K(ret));
  } else if (OB_FALSE_IT(pos = delete_node->get_index())) {
  } else if (OB_ISNULL(parent = delete_node->get_parent())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("xml node get parent is null", K(ret));
  } else if (pos >= parent->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get pos invalid.", K(ret), K(pos), K(parent->count()));
  } else if (OB_FAIL(parent->remove(pos))) {
    LOG_WARN("failed to remove text.", K(ret), K(pos));
  }

  return ret;
}

int ObExprDeleteXml::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_delete_xml;
  return OB_SUCCESS;
}

} // end of sql
} // end of oceanbase