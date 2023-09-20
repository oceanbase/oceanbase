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
 * This file is for func extractval.
 */

#include "ob_expr_extract_value.h"
#include "ob_expr_lob_utils.h"
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

ObExprExtractValue::ObExprExtractValue(common::ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_XML_EXTRACTVALUE, N_EXTRACTVALUE, MORE_THAN_ONE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprExtractValue::~ObExprExtractValue() {}

int ObExprExtractValue::calc_result_typeN(ObExprResType &type,
                                          ObExprResType *types,
                                          int64_t param_num,
                                          common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num != 2 && param_num != 3)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid argument number", K(ret), K(param_num));
  } else if (!is_called_in_sql()) {
    ret = OB_ERR_SP_LILABEL_MISMATCH;
    LOG_WARN("expr call in pl semantics disallowed", K(ret), K(N_EXTRACTVALUE));
    LOG_USER_ERROR(OB_ERR_SP_LILABEL_MISMATCH, static_cast<int>(strlen(N_EXTRACTVALUE)), N_EXTRACTVALUE);
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
      type.set_type(ObVarcharType);
      type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      type.set_collation_level(CS_LEVEL_IMPLICIT);
      // length == OB_MAX_ORACLE_VARCHAR_LENGTH is not supported by generated key, use OB_MAX_VARCHAR_LENGTH_KEY instead
      // length == OB_MAX_VARCHAR_LENGTH_KEY is not supported by generated column length check , use MAX_ORACLE_COMMENT_LENGTH instead
      type.set_length(MAX_ORACLE_COMMENT_LENGTH);
      type.set_length_semantics(LS_BYTE);
    }
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_XML
int ObExprExtractValue::eval_extract_value(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &allocator = tmp_alloc_g.get_allocator();
  ObDatum *xml_datum = NULL;
  ObString xpath_str;
  ObString namespace_str;
  ObIMulModeBase *xml_doc = NULL;
  ObPathExprIter xpath_iter(&allocator);
  ObString default_ns;
  ObPathVarObject prefix_ns(allocator);
  ObNodeMemType expect_type = ObNodeMemType::BINARY_TYPE;
  ObString xml_res;
  ObCollationType cs_type = CS_TYPE_INVALID;
  ObMulModeMemCtx* xml_mem_ctx = nullptr;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ObXMLExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session()), "XMLModule"));

  if (OB_ISNULL(ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get session failed.", K(ret));
  } else if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(&allocator, xml_mem_ctx))) {
    LOG_WARN("fail to create tree memory context", K(ret));
  } else if (OB_UNLIKELY(expr.arg_cnt_ != 2 && expr.arg_cnt_ != 3)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid arg_cnt_", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(ObXMLExprHelper::get_xmltype_from_expr(expr.args_[0], ctx, xml_datum))) {
    LOG_WARN("fail to get xml str", K(ret));
  } else if (ObNullType == expr.args_[1]->datum_meta_.type_) {
    ret = OB_ERR_INVALID_XPATH_EXPRESSION;
    LOG_WARN("invalid xpath expression", K(ret));
  } else if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr.args_[1], ctx, xpath_str, allocator))) {
    LOG_WARN("fail to get xpath str", K(ret));
  } else if (expr.arg_cnt_ == 3) {
    if (ObNullType == expr.args_[2]->datum_meta_.type_) {
    } else if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr.args_[2], ctx, namespace_str, allocator))) {
      LOG_WARN("fail to get xpath str", K(ret));
    } else if (OB_FAIL(ObXMLExprHelper::construct_namespace_params(namespace_str, default_ns, prefix_ns, allocator))) {
      LOG_WARN("fail to construct namespace params", K(ret), K(namespace_str));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObXMLExprHelper::get_xml_base(xml_mem_ctx, xml_datum, cs_type, expect_type, xml_doc))) {
    LOG_WARN("fail to parse xml doc", K(ret));
  } else if (OB_FAIL(extract_xpath_result(xml_mem_ctx, xpath_str, default_ns, xml_doc, &prefix_ns, xml_res))) {
    LOG_WARN("fail to extract xpath result", K(ret), K(xpath_str));
  } else if (xml_res.empty()) {
    res.set_null();
  } else if (OB_FAIL(ObXMLExprHelper::set_string_result(expr, ctx, res, xml_res))) {
    LOG_WARN("fail to set result datum", K(ret));
  }
  return ret;
}

int ObExprExtractValue::extract_xpath_result(ObMulModeMemCtx *xml_mem_ctx, ObString& xpath_str, ObString& default_ns,
                                             ObIMulModeBase* xml_doc, ObPathVarObject* prefix_ns, ObString &xml_res)
{
  int ret = OB_SUCCESS;
  ObPathExprIter xpath_iter(xml_mem_ctx->allocator_);
  int64_t valid_node_num = 0; // element ,ns, attribute node
  int64_t text_node_num = 0; // text, cdata
  int64_t skip_node_num = 0; // pi or comment node
  ObIMulModeBase *node = NULL;
  ObSEArray<ObIMulModeBase *, 8> result_nodes;
  if (OB_FAIL(xpath_iter.init(xml_mem_ctx, xpath_str, default_ns, xml_doc, prefix_ns))) {
    LOG_WARN("fail to init xpath iterator", K(xpath_str), K(default_ns), K(ret));
    ObXMLExprHelper::replace_xpath_ret_code(ret);
  } else if (OB_FAIL(xpath_iter.open())) {
    LOG_WARN("fail to open xpath iterator", K(ret));
    ObXMLExprHelper::replace_xpath_ret_code(ret);
  }

  while (valid_node_num < 2 && OB_SUCC(ret)) {
    if (OB_FAIL(xpath_iter.get_next_node(node))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get next xpath result node", K(ret));
      }
    } else if (OB_ISNULL(node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("xpath result node is null", K(ret));
    } else if (M_COMMENT == node->type() || M_INSTRUCT == node->type()) {
      skip_node_num++;
    } else if (ObXMLExprHelper::is_xml_text_node(node->type())) {
      text_node_num++;
      if (OB_FAIL(result_nodes.push_back(node))) {
        LOG_WARN("fail to push back result node", K(ret));
      }
    } else if (ObXMLExprHelper::is_xml_element_node(node->type()) ||
               ObXMLExprHelper::is_xml_attribute_node(node->type())) {
      valid_node_num++;
      if (OB_FAIL(result_nodes.push_back(node))) {
        LOG_WARN("fail to push back result node", K(ret));
      }
    }
  } // end while

  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }
  // check if match the rules
  if (OB_FAIL(ret)) {
  } else if (valid_node_num > 1 ||
             (valid_node_num == 1 && (skip_node_num > 0 || text_node_num > 0))) {
    ret = OB_ERR_EXTRACTVALUE_MULTI_NODES;
    LOG_WARN("EXTRACTVALUE returns value of only one node", K(ret));
  } else if (valid_node_num == 0) {
    if (skip_node_num > 0 || text_node_num > 0) {
      // classify whether has same parent
      bool is_same_parent = false;
      if (OB_FAIL(has_same_parent_node(xml_mem_ctx, xpath_str, default_ns, xml_doc, prefix_ns, is_same_parent))) {
        LOG_WARN("fail to classify whether has same parent", K(ret));
      } else if (!is_same_parent) {
        ret = OB_ERR_EXTRACTVALUE_MULTI_NODES;
        LOG_WARN("EXTRACTVALUE returns value of only one node", K(ret));
      }
    }
  } else {
    // do nothing when valid_node_num == 1 && skip_node_num > 0 && text_node_num > 0
  }

  // if match, extract the value
  if (OB_FAIL(ret)) {
  } else if (result_nodes.count() == 1 &&
             OB_FAIL(extract_node_value(*xml_mem_ctx->allocator_, result_nodes.at(0), xml_res))) {
    LOG_WARN("fail to extract node value", K(ret));
  } else if (result_nodes.count() > 1 &&
             OB_FAIL(merge_text_nodes_with_same_parent(xml_mem_ctx->allocator_, result_nodes, xml_res))) {
    LOG_WARN("fail to merge text nodes", K(ret), K(result_nodes.count()));
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = xpath_iter.close())) {
    LOG_WARN("fail to close xpath iter", K(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }

  return ret;
}

int ObExprExtractValue::merge_text_nodes_with_same_parent(ObIAllocator *allocator,
                                                          ObIArray<ObIMulModeBase *> &result_nodes,
                                                          ObString &xml_res)
{
  int ret = OB_SUCCESS;
  // text-nodes or skip-nodes has same parent
  ObIMulModeBase *child_node = NULL;
  ObStringBuffer *buffer = NULL;
  if (OB_ISNULL(buffer = OB_NEWx(ObStringBuffer, allocator, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate buffer", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < result_nodes.count(); i++) {
    if (OB_ISNULL(child_node = result_nodes.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node is NULL", K(ret));
    } else if (OB_FAIL(append_text_value(*buffer, child_node))) {
      LOG_WARN("fail to append text value", K(ret));
    }
  } // end for

  if (OB_SUCC(ret)) {
    xml_res.assign_ptr(buffer->ptr(), buffer->length());
  }
  return ret;
}


int ObExprExtractValue::append_text_value(ObStringBuffer &buffer, ObIMulModeBase *node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is NULL", K(ret));
  } else if (ObXMLExprHelper::is_xml_leaf_node(node->type())) {
    ObString tmp_res;
    if (OB_FAIL(node->get_value(tmp_res))) {
      LOG_WARN("fail to get node value", K(ret));
    } else if (OB_FAIL(buffer.append(tmp_res))) {
      LOG_WARN("fail to append buffer", K(ret), K(buffer));
    }
  }
  return ret;
}


int ObExprExtractValue::extract_node_value(ObIAllocator &allocator, ObIMulModeBase *node, ObString &xml_res)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("extract node is NULL", K(ret));
  } else if (ObXMLExprHelper::is_xml_leaf_node(node->type())) {
    if (OB_FAIL(node->get_value(xml_res))) {
      LOG_WARN("fail to get node value", K(ret));
    }
  } else if (ObXMLExprHelper::is_xml_element_node(node->type())) {
    int64_t child_size = node->size();
    if (child_size == 1 && ObXMLExprHelper::is_xml_element_node(node->at(0)->type())) {
      ret = OB_EXTRACTVALUE_NOT_LEAF_NODE;
      LOG_WARN("EXTRACTVALUE can only retrieve value of leaf node", K(ret));
    } else {
      int64_t ele_node_num = 0;
      ObIMulModeBase *child_node = NULL;
      ObStringBuffer *buffer = NULL;
      if (OB_ISNULL(buffer = OB_NEWx(ObStringBuffer, &allocator, (&allocator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate buffer", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < child_size; i++) {
        if (OB_ISNULL(child_node = node->at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("node is NULL", K(ret));
        } else if (OB_FAIL(append_text_value(*buffer, child_node))) {
           LOG_WARN("fail to append text value", K(ret));
        } else if (ObXMLExprHelper::is_xml_element_node(child_node->type())) {
          ele_node_num++;
        } else {
          // skip other node type
        }
      } // end for
      if (ele_node_num > 0) {
        ret = OB_ERR_EXTRACTVALUE_MULTI_NODES;
        LOG_WARN("EXTRACTVALUE returns value of only one node", K(ele_node_num));
      } else {
        xml_res.assign_ptr(buffer->ptr(), buffer->length());
      }
    }
  } // type == M_ELEMENT
  return ret;
}

int ObExprExtractValue::has_same_parent_node(ObMulModeMemCtx *xml_mem_ctx, ObString& xpath_str, ObString& default_ns,
                                             ObIMulModeBase* xml_doc, ObPathVarObject* prefix_ns, bool &is_same_parent)
{
  int ret = OB_SUCCESS;
  ObPathExprIter xpath_iter(xml_mem_ctx->allocator_);
  ObIMulModeBase *node = NULL;
  int64_t node_num = 0;
  ObStringBuffer buffer(xml_mem_ctx->allocator_);
  ObString parent_xpath;
  if (OB_FAIL(buffer.append(xpath_str))) {
    LOG_WARN("fail to append buffer", K(ret));
  } else if (OB_FAIL(buffer.append("/parent::node()"))) {
    LOG_WARN("fail to append buffer", K(ret));
  } else {
    parent_xpath.assign_ptr(buffer.ptr(), buffer.length());
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(xpath_iter.init(xml_mem_ctx, parent_xpath, default_ns, xml_doc, prefix_ns))) {
    LOG_WARN("fail to init xpath iterator", K(parent_xpath), K(default_ns), K(ret));
    ObXMLExprHelper::replace_xpath_ret_code(ret);
  } else if (OB_FAIL(xpath_iter.open())) {
    LOG_WARN("fail to open xpath iterator", K(ret));
    ObXMLExprHelper::replace_xpath_ret_code(ret);
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(xpath_iter.get_next_node(node))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get next xpath result node", K(ret));
      }
    } else {
      node_num++;
    }
  }

  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }

  if (node_num > 1) { // has different parent
    is_same_parent = false;
  } else {
    is_same_parent = true;
  }

  return ret;
}

#endif

int ObExprExtractValue::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_extract_value;
  return OB_SUCCESS;
}

} // sql
} // oceanbase