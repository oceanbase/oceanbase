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
#include "lib/xml/ob_xml_parser.h"
#include "lib/xml/ob_xml_util.h"
#include "sql/engine/expr/ob_expr_xml_func_helper.h"
#include "lib/xml/ob_binary_aggregate.h"
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
  } else if (OB_FAIL(ObXMLExprHelper::get_xml_base(mem_ctx, xml_datum, cs_type, expect_type, xml_doc, node_type, ObGetXmlBaseType::OB_SHOULD_CHECK))) {
    LOG_WARN("fail to parse xml doc", K(ret));
  } else if (OB_FAIL(xpath_iter.init(mem_ctx, xpath_str, default_ns, xml_doc, &prefix_ns))) {
    LOG_WARN("fail to init xpath iterator", K(xpath_str), K(default_ns), K(ret));
    ObXMLExprHelper::replace_xpath_ret_code(ret);
  } else if (OB_FAIL(concat_xpath_result(expr, ctx, xpath_iter, cs_type, res, node_type, mem_ctx))) {
    LOG_WARN("fail to concat xpath result", K(ret));
  }
  return ret;
}

int ObExprExtractXml::concat_xpath_result(const ObExpr &expr,
                                          ObEvalCtx &eval_ctx,
                                          ObPathExprIter &xpath_iter,
                                          ObCollationType cs_type,
                                          ObDatum &res,
                                          ObMulModeNodeType &node_type,
                                          ObMulModeMemCtx* mem_ctx)
{
  int ret = OB_SUCCESS;
  ObStringBuffer buff(mem_ctx->allocator_);
  ObIMulModeBase *node = NULL;
  int64_t append_node_num = 0;
  int element_count = 0;
  int text_count = 0;
  ObString version;
  ObString encoding;
  uint16_t standalone;
  ObString blob_locator;
  bool first_is_doc = false;
  ObIMulModeBase* last_parent = nullptr;
  common::hash::ObHashMap<ObString, ObString> ns_map;

  if (OB_FAIL(xpath_iter.open())) {
    LOG_WARN("fail to open xpath iterator", K(ret));
    ObXMLExprHelper::replace_xpath_ret_code(ret);
  } else if (OB_FAIL(ns_map.create(10, lib::ObMemAttr(MTL_ID(), "XMLModule")))) {
    LOG_WARN("ns map create failed", K(ret));
  }

  ObBinAggSerializer bin_agg(mem_ctx->allocator_, ObBinAggType::AGG_XML, static_cast<uint8_t>(M_CONTENT));

  while (OB_SUCC(ret)) {
    ObIMulModeBase* tmp = nullptr;
    ObXmlBin extend;
    if (OB_FAIL(xpath_iter.get_next_node(node))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get next xml node", K(ret));
      }
    } else if (OB_ISNULL(node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("xpath result node is null", K(ret));
    } else if (node->is_tree() && OB_FAIL(ObMulModeFactory::transform(mem_ctx, node, BINARY_TYPE, node))) {
      LOG_WARN("fail to transform to tree", K(ret));
    } else {
      ObXmlBin *bin = nullptr;
      if (OB_ISNULL(bin = static_cast<ObXmlBin*>(node))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get bin failed", K(ret));
      } else if (bin->meta_.len_ == 0) {
        // do nothing
      } else if (bin->check_extend()) {
        bool conflict = false;
        // check key conflict
        if (OB_FAIL(bin->get_extend(extend))) {
          LOG_WARN("fail to get extend", K(ret));
        } else if (OB_FAIL(ObXmlUtil::check_ns_conflict(xpath_iter.get_cur_res_parent(), last_parent, &extend, ns_map, conflict))) {
          LOG_WARN("fail to check conflict", K(ret));
        } else if (conflict) {
          // if conflict, merge bin
          if (OB_FAIL(bin->merge_extend(extend))) {
            LOG_WARN("fail to merge extend", K(ret));
          } else {
            bin = &extend;
          }
        } else if (OB_FAIL(bin->remove_extend())) { // if not conflict, erase extend
          LOG_WARN("fail to remove extend", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(bin_agg.append_key_and_value(bin))) {
        LOG_WARN("failed to append binary", K(ret));
      } else {
        ObMulModeNodeType type = node->type();
        if (append_node_num == 0 && type == ObMulModeNodeType::M_DOCUMENT) {
          version = node->get_version();
          encoding = node->get_encoding();
          standalone = node->get_standalone();
          first_is_doc = version.empty() ? false : true;
        }

        if (type == ObMulModeNodeType::M_ELEMENT || type == ObMulModeNodeType::M_DOCUMENT) {
          element_count++;
        } else if (type == ObMulModeNodeType::M_TEXT || type == ObMulModeNodeType::M_CDATA) {
          text_count++;
        } else if (type == ObMulModeNodeType::M_CONTENT) {
          append_node_num += bin->count() - 1;
        }
        append_node_num++;
      }
    }
  }

  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
    if (element_count > 1 || element_count == 0) {
      node_type = ObMulModeNodeType::M_CONTENT;
    } else if (element_count == 1 && text_count > 0) {
      node_type = ObMulModeNodeType::M_CONTENT;
    } else if (append_node_num == 0) {
      // do nothing
    } else {
      node_type = ObMulModeNodeType::M_DOCUMENT;
    }

    bin_agg.set_header_type(node_type);
    if (first_is_doc && append_node_num == 1) {
      bin_agg.set_xml_decl(version, encoding, standalone);
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = xpath_iter.close())) {
    LOG_WARN("fail to close xpath iter", K(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  } else if (append_node_num == 0) {
    res.set_null();
  } else if (OB_FAIL(bin_agg.serialize())) {
    LOG_WARN("failed to serialize binary.", K(ret));
  } else if (ns_map.size() > 0 && OB_FAIL(ObXmlUtil::ns_to_extend(mem_ctx, ns_map, bin_agg.get_buffer()))) {
     LOG_WARN("failed to serialize extend.", K(ret));
  } else if (OB_FAIL(ObXMLExprHelper::pack_binary_res(expr, eval_ctx, bin_agg.get_buffer()->string(), blob_locator))) {
    LOG_WARN("failed to pack binary res.", K(ret));
  } else {
    res.set_string(blob_locator.ptr(), blob_locator.length());
  }
  ns_map.clear();
  return ret;
}

int ObExprExtractXml::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_extract_xml;
  return OB_SUCCESS;
}

} // sql
} // oceanbase