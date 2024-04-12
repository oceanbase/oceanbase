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

#include "ob_expr_update_xml.h"
#include "sql/engine/expr/ob_expr_xml_func_helper.h"
#include "lib/xml/ob_xml_parser.h"
#include "lib/xml/ob_xml_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#define USING_LOG_PREFIX SQL_ENG


using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprUpdateXml::ObExprUpdateXml(common::ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_UPDATE_XML, N_UPDATEXML, MORE_THAN_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprUpdateXml::~ObExprUpdateXml() {}

int ObExprUpdateXml::calc_result_typeN(ObExprResType &type,
                                       ObExprResType *types,
                                       int64_t param_num,
                                       common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  bool is_mysql_mode = lib::is_mysql_mode();
  if (is_mysql_mode) {
    if (param_num != 3) {
      ret = OB_ERR_PARAM_SIZE;
      LOG_WARN("invalid param number", K(ret), K(param_num));
    } else {
      for (int8_t i = 0; OB_SUCC(ret) && i < param_num; i++) {
        ObObjType param_type = types[i].get_type();
        if (param_type == ObNullType) {
        } else if (ob_is_string_type(param_type)) {
          if (types[i].get_charset_type() != CHARSET_UTF8MB4) {
            types[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
          }
        } else if (i != 1 && !ob_is_string_type(param_type)) {
          types[i].set_calc_type(ObVarcharType);
          types[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      }
    }

  } else if (param_num < 3) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid param number", K(ret), K(param_num));
  } else if (!is_called_in_sql()) {
    ret = OB_ERR_SP_LILABEL_MISMATCH;
    LOG_WARN("expr call in pl semantics disallowed", K(ret), K(N_UPDATEXML));
    LOG_USER_ERROR(OB_ERR_SP_LILABEL_MISMATCH, static_cast<int>(strlen(N_UPDATEXML)), N_UPDATEXML);
  } else if (types[0].is_ext() && types[0].get_udt_id() == T_OBJ_XML) {
      types[0].get_calc_meta().set_sql_udt(ObXMLSqlType);
  } else if (!ob_is_xml_sql_type(types[0].get_type(), types[0].get_subschema_id())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "-", "-");
    LOG_WARN("inconsistent datatypes", K(ret), K(ob_obj_type_str(types[0].get_type())));
  }
  if (OB_FAIL(ret)) {
  } else if (is_mysql_mode) {
    type.set_type(ObLongTextType);
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_length(OB_MAX_LONGTEXT_LENGTH / 4);
    type.set_length_semantics(LS_BYTE);
  } else {
    bool has_ns_str = (param_num - 1) % 2 == 1;
    int64_t xpath_value_end = has_ns_str ? param_num - 1 : param_num;
    for (int64_t i = 1; i < xpath_value_end && OB_SUCC(ret); i++) {
      ObObjType param_type = types[i].get_type();
      ObCollationType cs_type = types[i].get_collation_type();
      if (i % 2 == 1) {
        // xpath string
        if (param_type == ObNullType) {
        } else if (ob_is_string_type(param_type)) {
          if (types[i].get_charset_type() != CHARSET_UTF8MB4) {
            types[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
          }
        }
      } else {
        // value expr
        if (param_type == ObNullType || ob_is_xml_sql_type(param_type, types[i].get_subschema_id())) {
        } else if (types[i].is_ext() && types[i].get_udt_id() == T_OBJ_XML) {
          types[i].get_calc_meta().set_sql_udt(ObXMLSqlType);
        } else if (ob_is_clob(param_type, cs_type) || ob_is_blob(param_type, cs_type)) {
          types[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        } else {
          types[i].set_calc_type(ObVarcharType);
          types[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      }
    }
    if (has_ns_str) {
      ObObjType param_type = types[param_num - 1].get_type();
      if (param_type == ObNullType) {
      } else if (ob_is_string_type(param_type)) {
        if (types[param_num - 1].get_charset_type() != CHARSET_UTF8MB4) {
          types[param_num - 1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      }
    }
    if (OB_SUCC(ret)) {
      type.set_sql_udt(ObXMLSqlType);
    }
  }
  return ret;
}

int ObExprUpdateXml::eval_mysql_update_xml(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &allocator = tmp_alloc_g.get_allocator();
  int64_t num_child = expr.arg_cnt_;
  ObString xml_target;
  ObString xpath_expr;
  ObString new_xml;
  ObString xml_res;
  ObIMulModeBase *xml_base = nullptr;
  ObXmlDocument *xml_doc = nullptr;
  bool return_null = false;
  ObUpdateXMLRetType res_origin = ObUpdateXMLRetType::ObRetMax;

  ObMulModeMemCtx* xml_mem_ctx = nullptr;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "XMLModule"));
  if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(&allocator, xml_mem_ctx))) {
    LOG_WARN("fail to create tree memory context", K(ret));
  } else if (num_child != 3) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid param number", K(ret), K(num_child));
  } else if (!expr.args_[1]->is_const_expr()) {
    ret = OB_XPATH_EXPRESSION_UNSUPPORTED;
    LOG_WARN("args_[1] get const expr invalid", K(ret), K(expr.args_[1]));
  } else if (ObNullType == expr.args_[0]->datum_meta_.type_ ||
             !ob_is_string_type(expr.args_[1]->datum_meta_.type_) ||
             ObNullType == expr.args_[2]->datum_meta_.type_) {
    return_null = true;
  } else if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr.args_[0], ctx, xml_target, allocator))) {
    LOG_WARN("failed to get xml_target str from expr", K(ret));
  } else if (xml_target.empty()) {
    // do nothing
  } else if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr.args_[1], ctx, xpath_expr, allocator))) {
    LOG_WARN("failed to get xpath expr.", K(ret));
  } else if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr.args_[2], ctx, new_xml, allocator))) {
    LOG_WARN("failed to get new xml.", K(ret));
  } else if (OB_FAIL(ObMulModeFactory::get_xml_base(xml_mem_ctx, xml_target, ObNodeMemType::TREE_TYPE, ObNodeMemType::TREE_TYPE, xml_base, M_DOCUMENT))) {
    ret = OB_SUCCESS;
    if (OB_FAIL(ObMulModeFactory::get_xml_base(xml_mem_ctx, xml_target, ObNodeMemType::TREE_TYPE, ObNodeMemType::TREE_TYPE, xml_base, M_CONTENT))) {
      LOG_USER_WARN(OB_ERR_XML_PARSE);
      ret = OB_SUCCESS;
      return_null = true;
      LOG_WARN("parse xml_frag failed.", K(xml_target));
    }
  }

  if (OB_FAIL(ret) || OB_ISNULL(xml_base)) {
  } else if (OB_FAIL(update_xml_tree_mysql(xml_mem_ctx, new_xml, ctx, xpath_expr, xml_base, res_origin))) {
    LOG_WARN("update xml tree failed", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (return_null) {
    res.set_null();
  } else if (OB_ISNULL(xml_base)) {
    if (OB_FAIL(ObXMLExprHelper::set_string_result(expr, ctx, res, xml_res))) {
      LOG_WARN("pack res origin failed.", K(ret), K(xml_res));
    }
  } else if (res_origin == ObUpdateXMLRetType::ObRetInputStr) {
    if (OB_FAIL(ObXMLExprHelper::set_string_result(expr, ctx, res, xml_target))) {
      LOG_WARN("pack res origin failed.", K(ret), K(xml_target));
    }
  } else if (res_origin == ObUpdateXMLRetType::ObRetNullType) {
    res.set_null();
  } else {
    ObStringBuffer buff(&allocator);
    if (OB_ISNULL(xml_doc = static_cast<ObXmlDocument *>(xml_base))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get xml base failed.", K(ret));
    } else if (OB_FAIL(xml_doc->print_document(buff, CS_TYPE_INVALID, ObXmlFormatType::NO_FORMAT | ObXmlFormatType::NO_ENTITY_ESCAPE))) {
      LOG_WARN("failed to print document.", K(ret));
    } else {
      ObString res_str = buff.string();
      if (OB_FAIL(ObXMLExprHelper::set_string_result(expr, ctx, res, res_str))) {
        LOG_WARN("failed to pack long text res.", K(ret));
      }
    }
  }

  return ret;
}

int ObExprUpdateXml::eval_update_xml(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &allocator = tmp_alloc_g.get_allocator();
  ObDatum *xml_datum = NULL;
  ObString namespace_str;
  ObIMulModeBase *xml_tree = NULL;
  bool has_namespace_str = false;
  int64_t num_child = expr.arg_cnt_;
  ObPathVarObject prefix_ns(allocator);
  ObString default_ns;
  ObCollationType cs_type = CS_TYPE_INVALID;
  ObMulModeMemCtx* xml_mem_ctx = nullptr;
  bool input_is_doc = false;
  ObMulModeNodeType node_type = M_MAX_TYPE;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ObXMLExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session()), "XMLModule"));

  if (OB_ISNULL(ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get session failed.", K(ret));
  } else if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(&allocator, xml_mem_ctx))) {
    LOG_WARN("fail to create tree memory context", K(ret));
  } else if (num_child < 3) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid param number", K(ret), K(num_child));
  } else if (OB_FAIL(ObXMLExprHelper::get_xmltype_from_expr(expr.args_[0], ctx, xml_datum))) {
    LOG_WARN("fail to get xmltype", K(ret));
  } else if (FALSE_IT(has_namespace_str = ((num_child - 1) % 2 != 0))) {
  } else if (has_namespace_str) {
    // namespace can be NULL
    if (ObNullType == expr.args_[num_child - 1]->datum_meta_.type_) {
    } else if (!ob_is_string_type(expr.args_[num_child - 1]->datum_meta_.type_)) {
      ret = OB_ERR_INVALID_XPATH_EXPRESSION;
    } else if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr.args_[num_child - 1], ctx, namespace_str, allocator))) {
      LOG_WARN("fail to get namespace string", K(ret));
    } else if (OB_FAIL(ObXMLExprHelper::construct_namespace_params(namespace_str, default_ns, prefix_ns, allocator))) {
      LOG_WARN("fail to construct namespace params", K(ret), K(namespace_str));
    }
  }

  if (OB_SUCC(ret)) {
    int64_t xpath_value_size = has_namespace_str ? num_child - 1 : num_child;
    if (OB_FAIL(ObXMLExprHelper::get_xml_base(xml_mem_ctx, xml_datum, cs_type, ObNodeMemType::TREE_TYPE, xml_tree, node_type, ObGetXmlBaseType::OB_IS_REPARSE))) {
      LOG_WARN("fail to get xml base", K(ret));
    }
    // do update xml
    for (int64_t i = 1; i < xpath_value_size && OB_SUCC(ret); i+=2) {
      ObString xpath_str;
      if (ObNullType == expr.args_[i]->datum_meta_.type_) {
        ret = OB_ERR_INVALID_XPATH_EXPRESSION;
        LOG_WARN("invalid xpath expression", K(ret));
      } else if (!ob_is_string_type(expr.args_[i]->datum_meta_.type_)) {
      } else if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr.args_[i], ctx, xpath_str, allocator))) {
        LOG_WARN("fail to get xpath string", K(ret), K(i));
      } else if (xpath_str.empty()) {
        ret = OB_ERR_INVALID_XPATH_EXPRESSION;
        LOG_WARN("xpath is empty", K(ret));
      } else if (OB_FAIL(update_xml_tree(xml_mem_ctx,  expr.args_[i+1], ctx, xpath_str, default_ns, &prefix_ns, xml_tree))) {
        LOG_WARN("fail to do update in xml tree", K(ret), K(xml_tree), K(xpath_str), K(default_ns), K(i+1));
      }
    }
    // set result
    if (OB_SUCC(ret)) {
      ObXmlDocument *xml_doc = static_cast<ObXmlDocument *>(xml_tree);
      ObStringBuffer buff(&allocator);
      ObString xml_plain_text;
      if (OB_ISNULL(xml_doc)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate xml doc failed", K(ret));
      } else if (OB_FAIL(xml_doc->print_document(buff, CS_TYPE_INVALID, ObXmlFormatType::NO_FORMAT))) {
        LOG_WARN("fail to print xml tree", K(ret));
      } else if (FALSE_IT(xml_plain_text.assign_ptr(buff.ptr(), buff.length()))) {
      } else if (node_type == ObMulModeNodeType::M_DOCUMENT &&
                  OB_FAIL(ObXmlParserUtils::parse_document_text(xml_mem_ctx, xml_plain_text, xml_doc))) {
        if (ret == OB_ERR_PARSER_SYNTAX) ret = OB_ERR_XML_PARSE;
        LOG_WARN("parsing document failed", K(ret), K(xml_plain_text));
      } else if (node_type != ObMulModeNodeType::M_DOCUMENT && OB_FAIL(ObXmlParserUtils::parse_content_text(xml_mem_ctx, xml_plain_text, xml_doc))) {
        if (ret == OB_ERR_PARSER_SYNTAX) ret = OB_ERR_XML_PARSE;
        LOG_WARN("parsing content failed", K(ret), K(xml_plain_text));
      }
      if (OB_FAIL(ret)) {
      } else if (xml_doc->count() == 0) {
        res.set_null();
      } else if (OB_FAIL(ObXMLExprHelper::pack_xml_res(expr, ctx, res, xml_doc,
                                                                xml_mem_ctx,
                                                                node_type == ObMulModeNodeType::M_DOCUMENT ?
                                                                ObMulModeNodeType::M_DOCUMENT : ObMulModeNodeType::M_CONTENT,
                                                                xml_plain_text))) {
        LOG_WARN("fail to pack xml res", K(ret), K(xml_doc), K(xml_plain_text));
      }
    }
  }
  return ret;
}

int ObExprUpdateXml::update_xml_tree_mysql(ObMulModeMemCtx* xml_mem_ctx,
                                           ObString xml_target,
                                           ObEvalCtx &ctx,
                                           ObString &xpath_str,
                                           ObIMulModeBase *&xml_tree,
                                           ObUpdateXMLRetType &res_origin)
{
  INIT_SUCC(ret);
  ObPathExprIter xpath_iter((static_cast<ObXmlNode*>(xml_tree))->get_mem_ctx()->allocator_);
  ObIMulModeBase *node = NULL;
  ObString default_ns;
  ObPathVarObject prefix_ns(*(static_cast<ObXmlNode*>(xml_tree))->get_mem_ctx()->allocator_);
  ObArray<ObIMulModeBase*> res_array;
  ObXmlNode *xml_node = nullptr;
  ObXmlText *text_node = NULL;
  bool has_get_node = false;
  if (OB_FAIL(xpath_iter.init(xml_mem_ctx, xpath_str, default_ns, xml_tree, &prefix_ns))) {
    LOG_WARN("fail to init xpath iterator", K(xpath_str), K(default_ns), K(ret));
    ObXMLExprHelper::replace_xpath_ret_code(ret);
  } else if (OB_FAIL(xpath_iter.open())) {
    LOG_WARN("fail to open xpath iterator", K(ret));
    ObXMLExprHelper::replace_xpath_ret_code(ret);
  }

  while (OB_SUCC(ret) && ObUpdateXMLRetType::ObRetMax == res_origin) {
    if (OB_FAIL(xpath_iter.get_next_node(node))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get next xml node", K(ret));
      }
    } else if (has_get_node) {
      res_origin = ObUpdateXMLRetType::ObRetInputStr;
    } else {
      has_get_node = true;
    }
  }

  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }

  if (OB_FAIL(ret)) {
    if (ret == OB_NOT_IMPLEMENT) {
      ret = OB_SUCCESS;
      res_origin = ObUpdateXMLRetType::ObRetNullType;
    }
  } else if (res_origin != ObUpdateXMLRetType::ObRetMax) {
    // do nothing
  } else if (OB_ISNULL(node)) {
    res_origin = ObUpdateXMLRetType::ObRetInputStr;
  } else if (OB_ISNULL(xml_node = static_cast<ObXmlNode *>(node))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get xml node null", K(ret));
  } else if (OB_ISNULL(text_node = OB_NEWx(ObXmlText, xml_mem_ctx->allocator_, ObMulModeNodeType::M_TEXT, xml_mem_ctx))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("new xml text failed", K(ret));
  } else {
    text_node->set_value(xml_target);
    switch (xml_node->type()) {
      case M_DOCUMENT:
      case M_CONTENT: {
        ObXmlDocument *document = NULL;
        if (OB_ISNULL(document = OB_NEWx(ObXmlDocument, xml_mem_ctx->allocator_, ObMulModeNodeType::M_DOCUMENT, xml_mem_ctx))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("new xml text failed", K(ret));
        } else if (OB_FAIL(document->add_element(text_node))) {
          LOG_WARN("add text failed.", K(ret));
        } else if (OB_ISNULL(xml_tree = static_cast<ObXmlNode*>(document))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("document cast to xml node failed.", K(ret));
        }
        break;
      }
      case M_CDATA:
      case M_INSTRUCT:
      case M_COMMENT:
      case M_TEXT: {
        // update parent node
        ObXmlNode *parent = NULL;
        if (OB_ISNULL(parent = xml_node->get_parent())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get xml node parent failed.", K(ret));
        } else if (OB_FAIL(update_xml_child_text(parent, text_node))) {
          LOG_WARN("failed to update text.", K(ret), K(xml_node->type()));
        }

        break;
      }
      case M_ATTRIBUTE: {
        // rewrite tree
        ObXmlAttribute *attribute = NULL;
        if (OB_ISNULL(attribute = OB_NEWx(ObXmlAttribute, xml_mem_ctx->allocator_, ObMulModeNodeType::M_ATTRIBUTE, xml_mem_ctx))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("new xml text failed", K(ret));
        } else {
          attribute->set_xml_key(xml_target);
          attribute->set_only_key();
          if (OB_FAIL(update_xml_child_text(xml_node, attribute))) {
            LOG_WARN("update xml child failed.", K(ret), K(xml_node->type()));
          }
        }

        break;
      }
      case M_ELEMENT: {
        if (OB_FAIL(update_xml_child_text(xml_node, text_node))) {
          LOG_WARN("update xml child failed.", K(ret), K(xml_node->type()));
        }
        break;
      }
      case M_NAMESPACE: {
        res_origin = ObUpdateXMLRetType::ObRetInputStr;
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported xml node type", K(ret), K(xml_node->type()));
        break;
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

int ObExprUpdateXml::update_xml_child_text(ObXmlNode *old_node, ObXmlNode *text_node)
{
  INIT_SUCC(ret);
  ObXmlNode *parent = NULL;
  ObXmlElement *ele_node = NULL;
  int64_t pos = -1;
  if (OB_ISNULL(old_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null node", K(ret));
  } else if (OB_ISNULL(parent = old_node->get_parent())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parat node is null", K(ret));
  } else if (OB_ISNULL(ele_node = static_cast<ObXmlElement*>(parent))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parat node is null", K(ret));
  } else {
    pos = old_node->get_index();
    if (pos < 0 || pos > ele_node->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pos is invalid", K(ret), K(pos));
    } else if (old_node->type() == M_ATTRIBUTE) {
      if (OB_FAIL(ele_node->remove_attribute(pos))) {
        LOG_WARN("failed to remove attribute.", K(ret), K(pos));
      } else if (OB_FAIL(ele_node->add_attribute(text_node, false, pos))) {
        LOG_WARN("failed to add attribute.", K(ret), K(pos));
      }
    } else {
      if (OB_FAIL(ele_node->remove_element(ele_node->at(pos)))) {
        LOG_WARN("remove element failed.", K(ret), K(pos));
      } else if (OB_FAIL(ele_node->add_element(text_node, false, pos))) {
        LOG_WARN("failed to add text.", K(ret));
      }
    }
  }

  return ret;
}

int ObExprUpdateXml::update_xml_tree(ObMulModeMemCtx* xml_mem_ctx,
                                     const ObExpr *expr,
                                     ObEvalCtx &ctx,
                                     ObString &xpath_str,
                                     ObString &default_ns,
                                     ObPathVarObject *prefix_ns,
                                     ObIMulModeBase *xml_tree)
{
  int ret = OB_SUCCESS;
  ObPathExprIter xpath_iter((static_cast<ObXmlNode*>(xml_tree))->get_mem_ctx()->allocator_);
  ObIMulModeBase *node = NULL;
  if (OB_FAIL(xpath_iter.init((static_cast<ObXmlNode*>(xml_tree))->get_mem_ctx(), xpath_str, default_ns, xml_tree, prefix_ns))) {
    LOG_WARN("fail to init xpath iterator", K(xpath_str), K(default_ns), K(ret));
    ObXMLExprHelper::replace_xpath_ret_code(ret);
  } else if (OB_FAIL(xpath_iter.open())) {
    LOG_WARN("fail to open xpath iterator", K(ret));
    ObXMLExprHelper::replace_xpath_ret_code(ret);
  }

  ObArray<ObIMulModeBase*> res_array;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(xpath_iter.get_next_node(node))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get next xml node", K(ret));
      }
    } else if (OB_FAIL(res_array.push_back(node))) {
      LOG_WARN("fail to push xml node", K(ret));
    }
  }

  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }

  for (int i = 0; i < res_array.size() && OB_SUCC(ret); ++i) {
    ObIMulModeBase* update_node = res_array[i];
    if (OB_ISNULL(update_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("xpath result node is null", K(ret));
    } else if (OB_FAIL(update_xml_node(xml_mem_ctx, expr, ctx, update_node))) {
      LOG_WARN("fail to update xml node", K(ret), K(node));
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = xpath_iter.close())) {
    LOG_WARN("fail to close xpath iter", K(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }
  return ret;
}

int ObExprUpdateXml::update_xml_node(ObMulModeMemCtx* xml_mem_ctx,
                                     const ObExpr *expr,
                                     ObEvalCtx &ctx,
                                     ObIMulModeBase *node)
{
  int ret = OB_SUCCESS;
  ObXmlNode *xml_node = static_cast<ObXmlNode *>(node);
  ObMulModeNodeType xml_type = xml_node->type();
  bool is_empty_content = false;
  if (OB_ISNULL(xml_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update node is NULL", K(ret));
  } else if (xml_type == M_DOCUMENT) {
    int64_t child_size = xml_node->size();
    bool is_found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < child_size ; i++) {
      ObXmlNode *child_node = xml_node->at(i);
      if (OB_ISNULL(child_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child node is NULL", K(ret));
      } else if (child_node->type() == M_ELEMENT) {
        xml_node = child_node;
        is_found = true;
      }
    }
    if (!is_found) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to found the root element node", K(ret));
    }
  } else if (xml_type == M_CONTENT) {
    if (xml_node->size() > 0) {
      xml_node = xml_node->at(xml_node->size() - 1);
    } else {
      is_empty_content = true;
    }
  }
  if (!is_empty_content) {
    switch (xml_node->type()) {
      case M_TEXT: {
        if (OB_FAIL(update_text_or_attribute_node(xml_mem_ctx, xml_node, expr, ctx, true))) {
          LOG_WARN("fail to update text node", K(ret), K(xml_type));
        }
        break;
      }
      case M_ATTRIBUTE: {
        if (OB_FAIL(update_text_or_attribute_node(xml_mem_ctx, xml_node, expr, ctx, false))) {
          LOG_WARN("fail to update attribute node", K(ret));
        }
        break;
      }
      case M_NAMESPACE: {
        if (OB_FAIL(update_namespace_node(xml_mem_ctx, xml_node, expr, ctx))) {
          LOG_WARN("fail to update namespace node", K(ret));
        }
        break;
      }
      case M_COMMENT:
      case M_CDATA: {
        if (OB_FAIL(update_cdata_and_comment_node(xml_mem_ctx, xml_node, expr, ctx))) {
          LOG_WARN("fail to update cdata node", K(ret), K(xml_type));
        }
        break;
      }
      case M_ELEMENT: {
        if (OB_FAIL(update_element_node(xml_mem_ctx, xml_node, expr, ctx))) {
          LOG_WARN("fail to update element node", K(ret), K(xml_type));
        }
        break;
      }
      case M_INSTRUCT: {
        if (OB_FAIL(update_pi_node(xml_mem_ctx, xml_node, expr, ctx))) {
          LOG_WARN("fail to pi node", K(ret), K(xml_type));
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported xml node type", K(ret), K(xml_type));
        break;
      }
    }
  }
  return ret;
}

int ObExprUpdateXml::update_pi_node(ObMulModeMemCtx* xml_mem_ctx,
                                    ObXmlNode *xml_node,
                                    const ObExpr *expr,
                                    ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObObjType val_type = expr->datum_meta_.type_;
  uint16_t sub_schema_id = expr->obj_meta_.get_subschema_id();
  ObDatum *datum = NULL;
  if (OB_FAIL(expr->eval(ctx, datum))) {
    LOG_WARN("fail to eval datum", K(ret));
  } else {
    if (val_type == ObNullType) {
     ret = OB_ERR_UPDATE_XML_WITH_INVALID_NODE;
     LOG_WARN("XML nodes must be updated with valid nodes and of the same type", K(ret));
    } else if (ob_is_string_type(val_type)) {
      ObXmlDocument *xml_doc = NULL;
      ObString value_str;
      if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr, ctx, value_str, *xml_mem_ctx->allocator_))) {
        LOG_WARN("fail to get value str", K(ret), K(value_str));
      } else if (OB_FAIL(ObXmlParserUtils::parse_content_text(xml_mem_ctx, value_str, xml_doc))) {
        if (ret == OB_ERR_PARSER_SYNTAX) {
          ret = OB_ERR_UPDATE_XML_WITH_INVALID_NODE;
        }
        LOG_WARN("fail to parse xml str", K(ret));
      } else if (xml_doc->size() > 0 && xml_doc->at(0)->type() == M_INSTRUCT) {
        if (OB_FAIL(update_xml_child_node(*xml_mem_ctx->allocator_, xml_node, xml_doc))) {
          LOG_WARN("fail to update pi node with content node", K(ret));
        }
      } else {
        ret = OB_ERR_UPDATE_XML_WITH_INVALID_NODE;
        LOG_WARN("update pi node with invalid node", K(ret), K(value_str));
      }
    } else if (ob_is_xml_sql_type(val_type, sub_schema_id)) {
      ObIMulModeBase *update_node = NULL;
      ObXmlNode *update_xml_node = NULL;
      ObCollationType cs_type = CS_TYPE_INVALID;
      if (OB_FAIL(ObXMLExprHelper::get_xml_base(xml_mem_ctx, datum, cs_type, ObNodeMemType::TREE_TYPE, update_node))) {
        LOG_WARN("fail to get update xml node", K(ret));
      } else if (OB_ISNULL(update_xml_node = static_cast<ObXmlNode *>(update_node))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update node is NULL", K(ret));
      } else if (update_xml_node->size() == 0) {
        ret = OB_ERR_UPDATE_XML_WITH_INVALID_NODE;
        LOG_WARN("update pi node with invalid node", K(ret));
      } else if (OB_FAIL(update_xml_child_node(*xml_mem_ctx->allocator_, xml_node, update_xml_node))) {
        LOG_WARN("fail to update xml content node ", K(ret));
      }
    }
  }
  return ret;
}

int ObExprUpdateXml::update_text_or_attribute_node(ObMulModeMemCtx* xml_mem_ctx,
                                                   ObXmlNode *xml_node,
                                                   const ObExpr *expr,
                                                   ObEvalCtx &ctx,
                                                   bool is_text)
{
  int ret = OB_SUCCESS;
  ObObjType val_type = expr->datum_meta_.type_;
  ObCollationType cs_type = expr->datum_meta_.cs_type_;
  uint16_t sub_schema_id = expr->obj_meta_.get_subschema_id();
  ObDatum *datum = NULL;
  if (OB_FAIL(expr->eval(ctx, datum))) {
    LOG_WARN("fail to eval datum", K(ret));
  } else {
    if (val_type == ObNullType) {
      xml_node->set_value(ObString::make_empty_string());
    } else if (is_text && ob_is_clob(val_type, cs_type)) {
      ObXmlDocument *xml_doc = NULL;
      ObString value_str;
      if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr, ctx, value_str, *xml_mem_ctx->allocator_))) {
        LOG_WARN("fail to get value str", K(ret), K(value_str));
      } else if (value_str.empty()) {
        ret = OB_LOB_VALUE_NOT_EXIST;
        LOG_WARN("LOB value is empty", K(ret), K(value_str));
      } else if (OB_FAIL(ObXmlParserUtils::parse_content_text(xml_mem_ctx, value_str, xml_doc))) {
        if (ret == OB_ERR_PARSER_SYNTAX) {
          ret = OB_ERR_XML_PARSE;
        }
        LOG_WARN("fail to parse xml str", K(ret));
      } else if (OB_FAIL(update_xml_child_node(*xml_mem_ctx->allocator_, xml_node, xml_doc))) {
        LOG_WARN("fail to update xml element node", K(ret));
      }
    } else if (ob_is_string_type(val_type)) {
      ObStringBuffer buff(xml_mem_ctx->allocator_);
      ObString value_str;
      if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr, ctx, value_str, *xml_mem_ctx->allocator_))) {
        LOG_WARN("fail to get value str", K(ret));
      } else {
        xml_node->set_value(value_str);
      }
    } else if (ob_is_xml_sql_type(val_type, sub_schema_id)) {
      ObIMulModeBase *update_node = NULL;
      ObXmlNode *update_xml_node = NULL;
      ObCollationType cs_type = CS_TYPE_INVALID;
      if (OB_FAIL(ObXMLExprHelper::get_xml_base(xml_mem_ctx, datum, cs_type, ObNodeMemType::TREE_TYPE, update_node))) {
        LOG_WARN("fail to get update xml node", K(ret));
      } else if (OB_ISNULL(update_xml_node = static_cast<ObXmlNode *>(update_node))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update node is NULL", K(ret));
      } else if (is_text && OB_FAIL(update_xml_child_node(*xml_mem_ctx->allocator_, xml_node, update_xml_node))) {
        LOG_WARN("fail to update xml node ", K(ret));
      } else if (!is_text && OB_FAIL(update_attribute_xml_node(xml_node, update_xml_node))) {
        LOG_WARN("fail to update xml node ", K(ret));
      }
    }
  }
  return ret;
}

int ObExprUpdateXml::update_namespace_node(ObMulModeMemCtx* xml_mem_ctx,
                                           ObXmlNode *xml_node,
                                           const ObExpr *expr,
                                           ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObObjType val_type = expr->datum_meta_.type_;
  uint16_t sub_schema_id = expr->obj_meta_.get_subschema_id();
  ObDatum *datum = NULL;
  if (OB_FAIL(expr->eval(ctx, datum))) {
    LOG_WARN("fail to eval datum", K(ret));
  } else {
    if (val_type == ObNullType) {
      ret = OB_ERR_XML_PARSE;
      LOG_WARN("update namespace node value to be NULL is unsupported", K(ret));
    } else if (ob_is_string_type(val_type)) {
      ObStringBuffer buff(xml_mem_ctx->allocator_);
      ObString value_str;
      if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr, ctx, value_str, *xml_mem_ctx->allocator_))) {
        LOG_WARN("fail to get value str", K(ret));
      } else if (OB_FAIL(update_namespace_value(*xml_mem_ctx->allocator_, xml_node, value_str))) {
        LOG_WARN("fail to update namespace node value", K(ret), K(value_str));
      }
    } else if (ob_is_xml_sql_type(val_type, sub_schema_id)) {
      ObIMulModeBase *update_node = NULL;
      ObXmlNode *update_xml_node = NULL;
      ObCollationType cs_type = CS_TYPE_INVALID;
      if (OB_FAIL(ObXMLExprHelper::get_xml_base(xml_mem_ctx, datum, cs_type, ObNodeMemType::TREE_TYPE, update_node))) {
        LOG_WARN("fail to get update xml node", K(ret));
      } else if (OB_ISNULL(update_xml_node = static_cast<ObXmlNode *>(update_node))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update node is NULL", K(ret));
      } else if (OB_FAIL(update_namespace_xml_node(*xml_mem_ctx->allocator_, xml_node, update_xml_node))) {
        LOG_WARN("fail to update xml node ", K(ret));
      }
    }
  }
  return ret;
}

int ObExprUpdateXml::update_element_node(ObMulModeMemCtx* xml_mem_ctx,
                                         ObXmlNode *xml_node,
                                         const ObExpr *expr,
                                         ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObObjType val_type = expr->datum_meta_.type_;
  uint16_t sub_schema_id = expr->obj_meta_.get_subschema_id();
  ObDatum *datum = NULL;
  if (OB_FAIL(expr->eval(ctx, datum))) {
    LOG_WARN("fail to eval datum", K(ret));
  } else {
    ObXmlElement *ele = static_cast<ObXmlElement *>(xml_node);
    if (val_type == ObNullType) {
      ObMulModeNodeType node_type = ele->type();
      if (OB_FAIL(clear_element_child_node(ele))) {
        LOG_WARN("fail to clear child node", K(ret));
      }
    } else if (ob_is_string_type(val_type)) {
      ObXmlDocument *xml_doc = NULL;
      ObString value_str;
      if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr, ctx, value_str, *xml_mem_ctx->allocator_))) {
        LOG_WARN("fail to get value str", K(ret), K(value_str));
      } else if (value_str.trim().empty()) {
        if (OB_FAIL(clear_element_child_node(ele))) {
          LOG_WARN("fail to clear child node", K(ret));
        }
      } else if (OB_FAIL(ObXmlParserUtils::parse_content_text(xml_mem_ctx, value_str, xml_doc))) {
        if (ret == OB_ERR_PARSER_SYNTAX) {
          ret = OB_ERR_XML_PARSE;
        }
        LOG_WARN("fail to parse xml str", K(ret));
      } else if (OB_FAIL(update_xml_child_node(*xml_mem_ctx->allocator_, ele, xml_doc))) {
        LOG_WARN("fail to update xml element node", K(ret));
      }
    } else if (ob_is_xml_sql_type(val_type, sub_schema_id)) {
      ObIMulModeBase *update_node = NULL;
      ObXmlNode *update_xml_node = NULL;
      ObCollationType cs_type = CS_TYPE_INVALID;
      if (OB_FAIL(ObXMLExprHelper::get_xml_base(xml_mem_ctx, datum, cs_type, ObNodeMemType::TREE_TYPE, update_node))) {
        LOG_WARN("fail to get update xml node", K(ret));
      } else if (OB_ISNULL(update_xml_node = static_cast<ObXmlNode *>(update_node))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update node is NULL", K(ret));
      } else if (update_xml_node->count() == 0) {
        if (OB_FAIL(clear_element_child_node(ele))) {
          LOG_WARN("fail to clear child node", K(ret));
        }
      } else if (OB_FAIL(update_xml_child_node(*xml_mem_ctx->allocator_, xml_node, update_xml_node))) {
        LOG_WARN("fail to update xml node ", K(ret));
      }
    }
  }
  return ret;
}

int ObExprUpdateXml::update_cdata_and_comment_node(ObMulModeMemCtx* xml_mem_ctx,
                                                   ObXmlNode *xml_node,
                                                   const ObExpr *expr,
                                                   ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObObjType val_type = expr->datum_meta_.type_;
  uint16_t sub_schema_id = expr->obj_meta_.get_subschema_id();
  ObDatum *datum = NULL;
  if (OB_FAIL(expr->eval(ctx, datum))) {
    LOG_WARN("fail to eval datum", K(ret));
  } else {
    if (val_type == ObNullType) {
      xml_node->set_value(ObString::make_empty_string());
    } else if (ob_is_string_type(val_type)) {
      ObXmlDocument *xml_doc = NULL;
      ObString value_str;
      if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr, ctx, value_str, *xml_mem_ctx->allocator_))) {
        LOG_WARN("fail to get value str", K(ret), K(value_str));
      } else if (OB_FAIL(ObXmlParserUtils::parse_content_text(xml_mem_ctx, value_str, xml_doc))) {
        if (ret == OB_ERR_PARSER_SYNTAX) {
          ret = OB_ERR_XML_PARSE;
        }
        LOG_WARN("fail to parse xml str", K(ret));
      } else if (OB_FAIL(update_xml_child_node(*xml_mem_ctx->allocator_, xml_node, xml_doc))) {
        LOG_WARN("fail to update xml element node", K(ret));
      }
    } else if (ob_is_xml_sql_type(val_type, sub_schema_id)) {
      ObIMulModeBase *update_node = NULL;
      ObXmlNode *update_xml_node = NULL;
      ObCollationType cs_type = CS_TYPE_INVALID;
      if (OB_FAIL(ObXMLExprHelper::get_xml_base(xml_mem_ctx, datum, cs_type, ObNodeMemType::TREE_TYPE, update_node))) {
        LOG_WARN("fail to get update xml node", K(ret));
      }  else if (OB_ISNULL(update_xml_node = static_cast<ObXmlNode *>(update_node))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update node is NULL", K(ret));
      } else if (OB_FAIL(update_xml_child_node(*xml_mem_ctx->allocator_, xml_node, update_xml_node))) {
        LOG_WARN("fail to update xml node ", K(ret));
      }
    }
  }
  return ret;
}

int ObExprUpdateXml::update_namespace_value(ObIAllocator &allocator, ObXmlNode *xml_node, const ObString &ns_value)
{
  int ret = OB_SUCCESS;
  ObXmlElement *parent = NULL;
  ObXmlAttribute *ns = NULL;
  ObXmlAttribute *new_ns = NULL;
  ObString key;
  if (OB_ISNULL(xml_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("xml node is NULL", K(ret));
  } else if (xml_node->type() != M_NAMESPACE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("xml node type is not expected", K(ret), K(xml_node->type()));
  } else {
    ns = static_cast<ObXmlAttribute *>(xml_node);
    parent = static_cast<ObXmlElement *>(xml_node->get_parent());
    key = ns->get_key();
  }
  if (OB_FAIL(ret)) {
  } else if (0 == key.compare(ObXmlConstants::XMLNS_STRING)) {
    ret = OB_ERR_XML_PARSE;
    LOG_WARN("defaul namespace is not allowed to update value", K(ret));
  } else if (OB_FAIL(update_exist_nodes_ns(parent, ns))) {
    LOG_WARN("fail to udpate exist node ns", K(ret));
  } else if (OB_ISNULL(new_ns = OB_NEWx(ObXmlAttribute, (&allocator), ObMulModeNodeType::M_NAMESPACE, parent->get_mem_ctx()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc failed", K(ret));
  } else {
    int64_t pos = -1;
    new_ns->set_value(ns_value);
    new_ns->set_prefix(ObXmlConstants::XMLNS_STRING);
    new_ns->set_xml_key(key);
    if (OB_FAIL(parent->get_attribute_pos(ObMulModeNodeType::M_NAMESPACE, key, pos))) {
      LOG_WARN("fail to get namespace node pos", K(ret));
    } else if (OB_FAIL(parent->remove_namespace(pos))) {
      LOG_WARN("fail to remove namespace node", K(ret));
    } else if (OB_FAIL(parent->add_attribute(new_ns, false, 0))) {
      LOG_WARN("fail to add new namespace node", K(ret));
    }
  }

  return ret;
}

int ObExprUpdateXml::clear_element_child_node(ObXmlElement *ele_node)
{
  int ret = OB_SUCCESS;
  // 1. first clear child node
  if (OB_ISNULL(ele_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("xml node is NULL", K(ret));
  } else {
    // remove all child node
    int64_t child_size = ele_node->size();
    for (int64_t i = 0; OB_SUCC(ret) && i < child_size; i++) {
      if (OB_FAIL(ele_node->remove_element(ele_node->at(0)))) {
        LOG_WARN("fail to remove element node", K(ret));
      }
    }
    // remove all attributes and namespaces not use
    int64_t attr_size = ele_node->attribute_size(); // attributes size will change after add or remove
    for (int64_t i = 0; OB_SUCC(ret) && i < attr_size; i++) {
      ObXmlAttribute *attr = NULL;
      // use as a queue, always remove the first node
      if (OB_FAIL(ele_node->get_attribute(attr, 0))) {
        LOG_WARN("fail to get attribute", K(ret), K(i));
      } else if (attr->type() == M_NAMESPACE) {
        if (OB_FAIL(ele_node->remove_namespace(0))) {
          LOG_WARN("fail to remove namespace", K(ret));
        }
      } else if (OB_FAIL(ele_node->remove_attribute(0))) {
        LOG_WARN("fail to remove attribute node", K(ret));
      }
    }
    // add namespace if use
    ObXmlAttribute *ns = ele_node->get_ns();
    if (OB_SUCC(ret) && OB_NOT_NULL(ns)) {
      if (ele_node->get_prefix().empty()) {
        ObXmlAttribute *default_ns = NULL;
        if (OB_FAIL(ObXMLExprHelper::get_valid_default_ns_from_parent(ele_node->get_parent(), default_ns))) {
          LOG_WARN("fail to get valid default ns", K(ret));
        } else if (OB_NOT_NULL(default_ns)
                   && default_ns->get_value().compare(ns->get_value()) == 0) {
          // do nothing
        } else if (OB_FAIL(ele_node->add_attribute(ns, false, 0))) {
          LOG_WARN("fail to add default namespace", K(ret));
        }
      } else if (OB_FAIL(ele_node->add_attribute(ns, false, 0))) {
        LOG_WARN("fail to add prefix namespace", K(ret));
      }
    }
  }
  return ret;
}

int ObExprUpdateXml::update_attribute_xml_node(ObXmlNode *old_node, ObXmlNode *update_node)
{
  int ret = OB_SUCCESS;
  int64_t pos = -1;
  ObXmlNode *parent = NULL;
  ObString key;
  ObXmlElement *ele_node = NULL;
  if (OB_ISNULL(old_node) || OB_ISNULL(update_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is NULL", K(ret), K(old_node), K(update_node));
  } else if (OB_ISNULL(parent = old_node->get_parent())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attribute parent node is NULL", K(ret));
  } else if (FALSE_IT(key = old_node->get_key())) {
  } else if (parent->type() != M_ELEMENT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parent of attribute node is not an element node", K(ret), K(parent->type()));
  } else if (FALSE_IT(ele_node = static_cast<ObXmlElement *>(parent))) {
  } else if (OB_FAIL(ele_node->get_attribute_pos(old_node->type(), key, pos))) {
    LOG_WARN("fail to get attribute pos", K(ret), K(key), K(old_node->type()));
  } else if (OB_FAIL(ele_node->remove_attribute(pos))) {  // remove attribute
    LOG_WARN("fail to remove attribute", K(ret), K(pos), K(key));
  } else if (OB_FAIL(remove_and_insert_element_node(ele_node, update_node, 0, false))) {
    LOG_WARN("fail to update element node", K(ret));
  }
  return ret;
}

int ObExprUpdateXml::update_namespace_xml_node(ObIAllocator &allocator, ObXmlNode *old_node, ObXmlNode *update_node)
{
  int ret = OB_SUCCESS;
  int64_t pos = -1;
  ObXmlNode *parent = NULL;
  ObXmlElement *ele_node = NULL;
  ObXmlAttribute *ns_node = NULL;
  ObString key;
  bool is_default_ns = false;
  if (OB_ISNULL(old_node) || OB_ISNULL(update_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is NULL", K(ret), K(old_node), K(update_node));
  } else if (FALSE_IT(ns_node = static_cast<ObXmlAttribute *>(old_node))) {
  } else if (OB_ISNULL(parent = ns_node->get_parent())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attribute parent node is NULL", K(ret));
  } else if (FALSE_IT(key = ns_node->get_key())) {
  } else if (FALSE_IT(is_default_ns = 0 == key.compare(ObXmlConstants::XMLNS_STRING))) {
  } else if (parent->type() != M_ELEMENT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parent of namespace node is not an element node", K(ret), K(parent->type()));
  } else if (FALSE_IT(ele_node = static_cast<ObXmlElement *>(parent))) {
  } else if (OB_FAIL(ele_node->get_attribute_pos(ns_node->type(), key, pos))) {
    LOG_WARN("fail to get attribute pos", K(ret), K(key), K(ns_node->type()));
  } else if (!is_default_ns && OB_FAIL(update_exist_nodes_ns(ele_node, ns_node))) {
    LOG_WARN("fail to update exist node ns", K(ret));
  } else if (OB_FAIL(ObXMLExprHelper::update_new_nodes_ns(allocator, ele_node, update_node))) {
    LOG_WARN("fail to update new node ns", K(ret));
  } else {
    // remove prefix ns: not default ns && ns of element is not this prefix && attr of element not use, remove the prefix xmlns
    if (!is_default_ns && ele_node->get_ns() != ns_node  && !ele_node->has_attribute_with_ns(ns_node)) {
      if (OB_FAIL(ele_node->remove_namespace(pos))) {
        LOG_WARN("fail to remove prefix namespace", K(ret), K(key));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(remove_and_insert_element_node(ele_node, update_node, 0, false))) {
      LOG_WARN("fail to update element node", K(ret), K(pos));
    }
  }
  return ret;
}

// update the descendent node prefix ns when need to remove parent node prefix ns
int ObExprUpdateXml::update_exist_nodes_ns(ObXmlElement *parent, ObXmlAttribute *prefix_ns)
{
  int ret = OB_SUCCESS;
  ObXmlAttribute *ns = NULL;
  if (OB_ISNULL(parent) || OB_ISNULL(prefix_ns)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is NULL", K(ret), K(parent), K(prefix_ns));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < parent->size(); i++) {
      if (OB_FAIL(ObXMLExprHelper::set_ns_recrusively(parent->at(i), prefix_ns))) {
        LOG_WARN("fail to set exist nodes ns", K(ret));
      }
    }
  }
  return ret;
}

// for xml nodes other than xmlattribute(including attribute and namespace)
int ObExprUpdateXml::update_xml_child_node(ObIAllocator &allocator, ObXmlNode *old_node, ObXmlNode *update_node)
{
  int ret = OB_SUCCESS;
  ObXmlNode *parent = NULL;
  ObXmlElement *ele_node = NULL;
  int64_t pos = -1;
  if (OB_ISNULL(old_node) || OB_ISNULL(update_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is NULL", K(ret), K(old_node), K(update_node));
  } else if (OB_ISNULL(parent = old_node->get_parent())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parent node is NULL", K(ret));
  } else {
    ele_node = static_cast<ObXmlElement *>(parent);
    pos = old_node->get_index();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObXMLExprHelper::update_new_nodes_ns(allocator, ele_node, update_node))) {
    LOG_WARN("fail to update new node ns", K(ret));
  } else if (OB_FAIL(remove_and_insert_element_node(ele_node, update_node, pos, true))) {
      LOG_WARN("fail to update element node", K(ret));
  }

  return ret;
}

int ObExprUpdateXml::remove_and_insert_element_node(ObXmlElement *ele_node, ObXmlNode *update_node, int64_t pos, bool is_remove)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ele_node) || OB_ISNULL(update_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is NULL", K(ret), K(ele_node), K(update_node));
  } else if (pos < 0 || pos > ele_node->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pos is invalid", K(ret), K(pos));
  } else if (ObXMLExprHelper::is_xml_root_node(update_node->type())) {
    if ((is_remove && ele_node->count() == 0) || update_node->count() == 0) {
      // skip and do nothing
    } else if (is_remove && OB_FAIL(ele_node->remove_element(ele_node->at(pos)))) { // remove the node
      LOG_WARN("fail to remove element node", K(ret), K(pos));
    } else {
      ObXmlDocument *xml_doc = static_cast<ObXmlDocument *>(update_node);
      for (int64_t i = 0; OB_SUCC(ret) && i < xml_doc->size(); i++) {
        if (OB_ISNULL(xml_doc->at(i)) ) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("xml node is null", K(ret), K(i));
        } else if (OB_FAIL(ele_node->add_element(xml_doc->at(i), false, pos + i))) { // insert the element node
          LOG_WARN("fail to add element node", K(ret), K(i));
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected node type", K(ret), K(update_node->type()));
  }
  return ret;
}

int ObExprUpdateXml::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (lib::is_oracle_mode()) {
    rt_expr.eval_func_ = eval_update_xml;
  } else {
    rt_expr.eval_func_ = eval_mysql_update_xml;
  }
  return OB_SUCCESS;
}

} // sql
} // oceanbase