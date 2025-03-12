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
#include "sql/engine/expr/ob_expr_xml_func_helper.h"
#include "lib/xml/ob_binary_aggregate.h"
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
  uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
  MultimodeAlloctor allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret);
  ObDatum *xml_datum = NULL;
  ObString xpath_str;
  ObString namespace_str;
  ObNodeMemType expect_type = ObNodeMemType::BINARY_TYPE;
  ObIMulModeBase *xml_doc = NULL;
  ObPathExprIter xpath_iter(&allocator);
  ObString default_ns;
  ObPathVarObject prefix_ns(allocator);
  ObString bin_str;
  ObMulModeNodeType node_type = M_MAX_TYPE;
  ObCollationType cs_type = CS_TYPE_INVALID;
  bool is_null_res = false;
  ObString blob_locator;
  // eval arg

  ObMulModeMemCtx* mem_ctx = nullptr;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, "XMLModule"));

  if (OB_ISNULL(ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get session failed.", K(ret));
  } else if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(&allocator, mem_ctx))) {
    LOG_WARN("fail to create tree memory context", K(ret));
  } else if (OB_UNLIKELY(expr.arg_cnt_ != 3)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg_cnt_", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(ObXMLExprHelper::get_xmltype_from_expr(expr.args_[0], ctx, xml_datum, allocator))) {
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
  } else if (OB_FAIL(ObXMLExprHelper::concat_xpath_result(mem_ctx, xpath_iter, bin_str, is_null_res))) {
    LOG_WARN("fail to concat xpath result", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (is_null_res) {
    res.set_null();
  } else if (OB_FAIL(ObXMLExprHelper::pack_binary_res(expr, ctx, bin_str, blob_locator))) {
    LOG_WARN("failed to pack binary res.", K(ret));
  } else {
    res.set_string(blob_locator.ptr(), blob_locator.length());
  }
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