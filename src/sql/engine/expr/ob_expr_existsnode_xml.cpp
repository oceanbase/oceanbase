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
 * This file is for func existsnode(xml).
 */

#include "ob_expr_existsnode_xml.h"
#include "lib/xml/ob_xml_parser.h"
#include "lib/xml/ob_xml_util.h"
#include "sql/engine/expr/ob_expr_xml_func_helper.h"
#include "sql/engine/ob_exec_context.h"

#define USING_LOG_PREFIX SQL_ENG

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprExistsNodeXml::ObExprExistsNodeXml(common::ObIAllocator &alloc)
  : ObFuncExprOperator(
      alloc,
      T_FUN_SYS_XML_EXISTSNODE,
      N_EXISTSNODE_XML,
      MORE_THAN_ONE,
      VALID_FOR_GENERATED_COL,
      NOT_ROW_DIMENSION)
{
}

ObExprExistsNodeXml::~ObExprExistsNodeXml() {}

int ObExprExistsNodeXml::calc_result_typeN(
    ObExprResType &type,
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
    LOG_WARN("expr call in pl semantics disallowed", K(ret), K(N_EXISTSNODE_XML));
    LOG_USER_ERROR(OB_ERR_SP_LILABEL_MISMATCH, static_cast<int>(strlen(N_EXISTSNODE_XML)), N_EXISTSNODE_XML);
  } else {
    ObObjType in_type = types[0].get_type();
    if (types[0].is_ext() && types[0].get_udt_id() == T_OBJ_XML) {
      types[0].get_calc_meta().set_sql_udt(ObXMLSqlType);
    } else if (!ob_is_xml_sql_type(in_type, types[0].get_subschema_id())) {
      ret = OB_ERR_INVALID_XML_DATATYPE;
      LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, "-", "-");
      LOG_WARN("inconsistent datatypes", K(ret), K(ob_obj_type_str(in_type)));
    }
    for (int8_t i = 1; i < param_num && OB_SUCC(ret); i++) {
      ObObjType param_type = types[i].get_type();
      if (param_type == ObNullType) {
        if (i == 1) {
          ret = OB_ERR_INVALID_XPATH_EXPRESSION;
          LOG_WARN("invalid xpath expression", K(ret));
        }
      } else if (ob_is_string_type(param_type)) {
        if (types[i].get_charset_type() != CHARSET_UTF8MB4) {
          types[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      } else {
        ret = OB_ERR_INVALID_XPATH_EXPRESSION;
        LOG_WARN("invalid xpath expression", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      type.set_int32();
    }
  }
  return ret;
}

int ObExprExistsNodeXml::eval_existsnode_xml(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  bool is_exists = false;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &allocator = tmp_alloc_g.get_allocator();
  ObDatum *xml_datum = NULL;
  ObIMulModeBase *xml_doc = NULL;
  ObMulModeNodeType node_type = M_MAX_TYPE;
  ObString xpath_str;
  ObPathExprIter xpath_iter(&allocator);
  ObString namespace_str;
  ObString default_ns;
  ObPathVarObject prefix_ns(allocator);
  ObMulModeMemCtx* mem_ctx = nullptr;

  // Check and read param
  CK(OB_NOT_NULL(ctx.exec_ctx_.get_my_session()));
  OZ(ObXmlUtil::create_mulmode_tree_context(&allocator, mem_ctx));
  CK(expr.arg_cnt_ == 3);
  OZ(ObXMLExprHelper::get_xmltype_from_expr(expr.args_[0], ctx, xml_datum), expr.args_[0]);
  if (OB_FAIL(ret)) {
  } else if (expr.args_[1]->datum_meta_.type_ == ObNullType) {
    ret = OB_ERR_INVALID_XPATH_EXPRESSION;
    LOG_WARN("invalid xpath expression", K(ret), K(expr.args_[1]->datum_meta_.type_ ));
  } else if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr.args_[1], ctx, xpath_str, allocator))) {
    LOG_WARN("fail to get xpath str", K(ret), K(expr.args_[1]));
  } else if (expr.args_[2]->datum_meta_.type_ == ObNullType){
    // do noting
  } else if (OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr.args_[2], ctx, namespace_str, allocator))) {
    LOG_WARN("fail to get namespace str", K(ret), K(expr.args_[2]));
  }

  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session()), "XMLModule"));
  // process xmltype and xpath
  OZ(ObXMLExprHelper::get_xml_base(
      mem_ctx, xml_datum,
      ObCollationType::CS_TYPE_INVALID,
      ObNodeMemType::BINARY_TYPE,
      xml_doc,
      node_type,
      ObGetXmlBaseType::OB_SHOULD_CHECK));
  OZ(ObXMLExprHelper::construct_namespace_params(namespace_str, default_ns, prefix_ns, allocator));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(xpath_iter.init(mem_ctx, xpath_str, default_ns, xml_doc, &prefix_ns))) {
    LOG_WARN("fail to init xpath iterator", K(xpath_str), K(default_ns), K(ret));
    ObXMLExprHelper::replace_xpath_ret_code(ret);
  } else if (OB_FAIL(xpath_iter.open())) {
    LOG_WARN("fail to open xpath iterator", K(ret));
    ObXMLExprHelper::replace_xpath_ret_code(ret);
  } else if (OB_FAIL(xpath_iter.get_node_exists(is_exists))) {
    LOG_WARN("fail to get node exists", K(ret));
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = xpath_iter.close())) {
    LOG_WARN("fail to close xpath iter", K(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }
  if (OB_FAIL(ret)) {
  } else if (is_exists) {
    res.set_int(1);
  } else {
    res.set_int(0);
  }
  return ret;
}

int ObExprExistsNodeXml::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_existsnode_xml;
  return OB_SUCCESS;
}

} // sql
} // oceanbase