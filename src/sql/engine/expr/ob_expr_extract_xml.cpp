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

#include "ob_expr_extract_xml.h"
#ifdef OB_BUILD_ORACLE_XML
#include "lib/xml/ob_xml_parser.h"
#include "lib/xml/ob_xml_util.h"
#include "sql/engine/expr/ob_expr_xml_func_helper.h"
#endif
#include "lib/utility/utility.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

#define USING_LOG_PREFIX SQL_ENG


using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprExtractXml::ObExprExtractXml(common::ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_XML_EXTRACT, N_EXTRACT_XML, MORE_THAN_ONE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprExtractXml::~ObExprExtractXml() {}

int ObExprExtractXml::calc_result_typeN(ObExprResType &type,
                                        ObExprResType *types,
                                        int64_t param_num,
                                        common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num != 3)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid argument number", K(ret), K(param_num));
  } else if (!is_called_in_sql()) {
    ret = OB_ERR_SP_LILABEL_MISMATCH;
    LOG_WARN("expr call in pl semantics disallowed", K(ret), K(N_EXTRACT_XML));
    LOG_USER_ERROR(OB_ERR_SP_LILABEL_MISMATCH, static_cast<int>(strlen(N_EXTRACT_XML)), N_EXTRACT_XML);
  } else {
    ObObjType in_type = types[0].get_type();
    if (types[0].is_ext() && types[0].get_udt_id() == T_OBJ_XML) {
      types[0].get_calc_meta().set_sql_udt(ObXMLSqlType);
    } else if (!ob_is_xml_sql_type(in_type, types[0].get_subschema_id())) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "-", "-");
      LOG_WARN("inconsistent datatypes", K(ret), K(ob_obj_type_str(in_type)));
    }
    for (int8_t i = 1; i < param_num && OB_SUCC(ret); i++) {
      ObObjType param_type = types[i].get_type();
      if (param_type == ObNullType) {
      } else if (ob_is_string_type(param_type)) {
        if (types[i].get_charset_type() != CHARSET_UTF8MB4) {
          types[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      } else {
        ret = OB_ERR_INVALID_XPATH_EXPRESSION;
      }
    }

    if (OB_SUCC(ret)) {
      type.set_sql_udt(ObXMLSqlType);
    }
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_XML
int ObExprExtractXml::eval_extract_xml(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &allocator = tmp_alloc_g.get_allocator();
  ObDatum *xml_datum = NULL;
  ObString xpath_str;
  ObString namespace_str;
  ObNodeMemType expect_type = ObNodeMemType::BINARY_TYPE;
  ObIMulModeBase *xml_doc = NULL;
  ObPathExprIter xpath_iter(&allocator);
  ObString default_ns;
  ObPathVarObject prefix_ns(allocator);
  ObString xml_res;
  ObXmlDocument *root = nullptr;
  ObMulModeNodeType node_type = M_MAX_TYPE;
  ObString input_str;
  ObCollationType cs_type = CS_TYPE_INVALID;
  // eval arg

  ObMulModeMemCtx* mem_ctx = nullptr;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ObXMLExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session()), "XMLModule"));

  if (OB_ISNULL(ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get session failed.", K(ret));
  } else if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(&allocator, mem_ctx))) {
    LOG_WARN("fail to create tree memory context", K(ret));
  } else if (OB_UNLIKELY(expr.arg_cnt_ != 3)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg_cnt_", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(ObXMLExprHelper::get_xmltype_from_expr(expr.args_[0], ctx, xml_datum))) {
    LOG_WARN("fail to get xmltype value", K(ret));
  } else if (ObNullType == expr.args_[1]->datum_meta_.type_) {
    ret = OB_ERR_INVALID_XPATH_EXPRESSION;
    LOG_WARN("invalid xpath expression", K(ret));
  } else if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr.args_[1], ctx, xpath_str, allocator))) {
    LOG_WARN("fail to get xpath str", K(ret));
  } else if (ObNullType == expr.args_[2]->datum_meta_.type_) {
  } else if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr.args_[2], ctx, namespace_str, allocator))) {
    LOG_WARN("fail to get xpath str", K(ret));
  } else if (OB_FAIL(ObXMLExprHelper::construct_namespace_params(namespace_str, default_ns, prefix_ns, allocator))) {
    LOG_WARN("fail to construct namespace params", K(ret), K(namespace_str));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObXMLExprHelper::get_xml_base(mem_ctx, xml_datum, cs_type, expect_type, xml_doc))) {
    LOG_WARN("fail to parse xml doc", K(ret));
  } else if (OB_FAIL(xpath_iter.init(mem_ctx, xpath_str, default_ns, xml_doc, &prefix_ns))) {
    LOG_WARN("fail to init xpath iterator", K(xpath_str), K(default_ns), K(ret));
    ObXMLExprHelper::replace_xpath_ret_code(ret);
  } else if (OB_FAIL(concat_xpath_result(xpath_iter, cs_type, root, node_type, mem_ctx))) {
    LOG_WARN("fail to concat xpath result", K(ret));
  } else if (OB_ISNULL(root) || root->size() == 0) {
    // root is not null and size = 0 if xpath='/.' or '//.' or '.' or 'self::*' and so on
    res.set_null();
  } else if (OB_FAIL(ObXMLExprHelper::pack_xml_res(expr, ctx, res, root, mem_ctx, node_type, input_str))) {
    LOG_WARN("fail to set result", K(xml_res), K(ret));
  }
  return ret;
}

int ObExprExtractXml::concat_xpath_result(ObPathExprIter &xpath_iter,
                                          ObCollationType cs_type,
                                          ObXmlDocument *&root,
                                          ObMulModeNodeType &node_type,
                                          ObMulModeMemCtx* mem_ctx)
{
  int ret = OB_SUCCESS;
  ObStringBuffer buff(mem_ctx->allocator_);
  ObIMulModeBase *node = NULL;
  int64_t append_node_num = 0;
  if (OB_FAIL(xpath_iter.open())) {
    LOG_WARN("fail to open xpath iterator", K(ret));
    ObXMLExprHelper::replace_xpath_ret_code(ret);
  }

  while (OB_SUCC(ret)) {
    ObIMulModeBase* tmp = nullptr;
    if (OB_FAIL(xpath_iter.get_next_node(node))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get next xml node", K(ret));
      }
    } else if (OB_ISNULL(node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("xpath result node is null", K(ret));
    } else if (node->is_binary() && OB_FAIL(ObMulModeFactory::transform(mem_ctx, node, TREE_TYPE, node))) {
      LOG_WARN("fail to transform to tree", K(ret));
    } else if (OB_ISNULL(root) && node->type() == M_DOCUMENT) {
      // if the xpath return document node, set it as root
      root = static_cast<ObXmlDocument *>(static_cast<ObXmlNode *>(node));
    } else {
      if (OB_ISNULL(root)) { // if root is NULL, alloc a content node as root
        if (OB_ISNULL(root = OB_NEWx(ObXmlDocument, (mem_ctx->allocator_), ObMulModeNodeType::M_CONTENT, (mem_ctx)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create an xml content node", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(append_node_to_res(*mem_ctx->allocator_, root, node))) {
        LOG_WARN("fail to append node text to result buffer", K(ret));
      } else {
        append_node_num++;
      }
    }
  }

  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
    if (OB_NOT_NULL(root)) { // res is NULL, do nothing
      int element_count = 0;
      int text_count = 0;
      int cdata_count = 0;

      if (node_type == ObMulModeNodeType::M_CONTENT) { // do nothing
      } else if (OB_FAIL(root->get_node_count(ObMulModeNodeType::M_ELEMENT, element_count))) {
        LOG_WARN("get element count node failed", K(ret));
      } else if (OB_FAIL(root->get_node_count(ObMulModeNodeType::M_TEXT, text_count))) {
        LOG_WARN("get text count node failed", K(ret));
      } else if (OB_FAIL(root->get_node_count(ObMulModeNodeType::M_CDATA, cdata_count))) {
        LOG_WARN("get cdata count node failed", K(ret));
      } else if (element_count > 1 || element_count == 0) {
        node_type = ObMulModeNodeType::M_CONTENT;
      } else if (element_count == 1 && (text_count > 0 || cdata_count > 0)) {
        node_type = ObMulModeNodeType::M_CONTENT;
      } else if (root->size() == 0) {
        // do nothing
      } else {
        if (append_node_num > 0) {
          root->set_has_xml_decl(false);
        }
        node_type = ObMulModeNodeType::M_DOCUMENT;
      }
      root->set_xml_type(node_type);
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = xpath_iter.close())) {
    LOG_WARN("fail to close xpath iter", K(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }
  return ret;
}

int ObExprExtractXml::append_node_to_res(ObIAllocator &allocator, ObXmlDocument *root, ObIMulModeBase *node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the append node is NULL", K(ret));
  } else if (!node->is_tree()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node type is not tree type", K(ret));
  } else {
    ObXmlNode *xml_node = static_cast<ObXmlNode *>(node);
    ObMulModeNodeType node_type = xml_node->type();
    ObString tmp_str;
    if (node_type == M_TEXT || node_type == M_ATTRIBUTE || node_type == M_NAMESPACE) {
      // extract attribute node value
      ObXmlText *xml_text = NULL;
      if (OB_FAIL(xml_node->get_value(tmp_str))) {
        LOG_WARN("fail to get node value", K(ret));
      } else {
        int64_t child_size = root->size();
        if (child_size > 0 && root->at(child_size-1)->type() == M_TEXT) {
          xml_text = static_cast<ObXmlText *>(root->at(child_size-1));
          ObStringBuffer buff(&allocator);
          if (OB_FAIL(buff.append(xml_text->get_text()))) {
            LOG_WARN("fail to append the orgin text", K(ret));
          } else if (OB_FAIL(buff.append(tmp_str))) {
            LOG_WARN("fail to append the new text", K(ret), K(tmp_str));
          } else {
            ObString new_str;
            new_str.assign_ptr(buff.ptr(), buff.length());
            xml_text->set_text(new_str);
          }
        } else {
          if (OB_ISNULL(xml_text = OB_NEWx(ObXmlText, (&allocator), ObMulModeNodeType::M_TEXT, (root->get_mem_ctx())))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to create xml text node", K(ret));
          } else if (FALSE_IT(xml_text->set_text(tmp_str))) {
          } else if (OB_FAIL(root->append(xml_text))) {
            LOG_WARN("fail to append node value to content", K(ret), K(tmp_str));
          }
        }
      }
    } else if (ObXMLExprHelper::is_xml_root_node(node_type)) {
      ObXmlDocument *xml_doc = static_cast<ObXmlDocument *>(xml_node);
      for (int64_t i = 0; OB_SUCC(ret) && i < xml_doc->size(); i++) {
        if (OB_ISNULL(xml_doc->at(i)) ) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("xml node is null", K(ret), K(i));
        } else if (OB_FAIL(root->append(xml_doc->at(i)))) {
          LOG_WARN("fail to append node to content", K(ret), K(i));
        }
      }
    } else if (OB_FAIL(root->append(node))) {
      LOG_WARN("fail to append node to content", K(ret), K(node_type));
    }
  }
  return ret;
}
#endif

int ObExprExtractXml::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_extract_xml;
  return OB_SUCCESS;
}

} // sql
} // oceanbase