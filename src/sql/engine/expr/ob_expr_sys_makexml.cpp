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
 * This file contains implementation for sys_makexml.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_sys_makexml.h"
#ifdef OB_BUILD_ORACLE_XML
#include "sql/engine/expr/ob_expr_xml_func_helper.h"
#endif
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
// Internal expression used for generate xmltype data from blob or udt with schema
ObExprSysMakeXML::ObExprSysMakeXML(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_MAKEXML, N_SYS_MAKEXML, MORE_THAN_ONE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSysMakeXML::~ObExprSysMakeXML()
{
}

int ObExprSysMakeXML::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num < 1 || param_num == 3 || param_num > 4)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid argument number", K(ret), K(param_num));
  } else if (!is_called_in_sql()) {
    ret = OB_ERR_SP_LILABEL_MISMATCH;
    LOG_WARN("expr call in pl semantics disallowed", K(ret), K(N_SYS_MAKEXML));
    LOG_USER_ERROR(OB_ERR_SP_LILABEL_MISMATCH, static_cast<int>(strlen(N_SYS_MAKEXML)), N_SYS_MAKEXML);
  } else if (param_num == 1) {
    // 1st param is blob
    // oracle will rewrite the query to sys_makexml(0, ...), but there is no need to compat it.
    if (!types_stack[0].is_blob() && !types_stack[0].is_clob()) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "LOB", N_SYS_MAKEXML);
    } else if (types_stack[0].is_clob()) {
      types_stack[0].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
  } else if (param_num == 2) {
    // 1st param is always treat as 0
    // 2st param is blob
    if (!types_stack[1].is_blob() && !types_stack[1].is_clob()) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "LOB", N_SYS_MAKEXML);
    } else if (types_stack[1].is_clob()) {
      types_stack[1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
  } else if (param_num == 4) {
    // 1st param is schema oid, 2nd param is root propertyid,
    // 3rd param is extra udt or 256, 4rd param is blob or xml udt
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only binary xml without schema is supported", K(ret), K(param_num));
  }

  if (OB_SUCC(ret)) {
    type.set_type(ObUserDefinedSQLType);
    type.set_subschema_id(ObXMLSqlType);
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_XML
int ObExprSysMakeXML::eval_sys_makexml(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *xml_datum = NULL;
  ObExpr *xml_arg = NULL;
  if (expr.arg_cnt_ == 1) {
    xml_arg = expr.args_[0];
  } else if (expr.arg_cnt_ == 2) {
    xml_arg = expr.args_[1];
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only binary xml without schema is supported", K(ret), K(expr.arg_cnt_), K(expr));
  }

  bool is_null_result = false;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &allocator = tmp_alloc_g.get_allocator();
  ObString full_xml_data;
  ObMulModeMemCtx* mem_ctx = nullptr;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "XMLModule"));

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(&allocator, mem_ctx))) {
    LOG_WARN("fail to create tree memory context", K(ret));
  } else if (xml_arg->datum_meta_.type_ != ObLongTextType
             || (xml_arg->datum_meta_.cs_type_ != CS_TYPE_BINARY
                 && xml_arg->datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_BIN)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("input type error", K(xml_arg->datum_meta_));
  } else if (OB_FAIL(xml_arg->eval(ctx, xml_datum))) {
    LOG_WARN("eval xml arg failed", K(ret));
  } else if (xml_datum->is_null()) {
    res.set_null();
  } else if (xml_arg->datum_meta_.cs_type_ == CS_TYPE_UTF8MB4_BIN) {
    // incase this function used directly with clob
    ObXmlDocument *xml_doc = NULL;
    ObIMulModeBase *xml_root = NULL;
    ObString xml_plain_text;
    if (OB_FAIL(ObTextStringHelper::get_string(expr, allocator, 1, xml_datum, xml_plain_text))) {
      LOG_WARN("get xml plain text failed", K(ret), K(xml_datum));
    } else if (xml_plain_text.empty()) {
      res.set_null();
    } else if (OB_FAIL(ObXmlParserUtils::parse_document_text(mem_ctx, xml_plain_text, xml_doc))) {
      LOG_WARN("parse xml plain text as document failed.", K(ret), K(xml_plain_text));
      ret = OB_ERR_XML_PARSE;
      LOG_USER_ERROR(OB_ERR_XML_PARSE);
    } else if (OB_ISNULL(xml_doc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null xml document.", K(ret));
    } else {
      xml_root = static_cast<ObIMulModeBase *>(xml_doc);
      ObString res_string;
      if (OB_FAIL(xml_root->get_raw_binary(res_string, &allocator))) {
        LOG_WARN("failed to get xml binary", K(ret), K(xml_plain_text));
      } else {
        ObExprStrResAlloc expr_res_alloc(expr, ctx);
        ObTextStringResult blob_res(ObLongTextType, true, &expr_res_alloc);

        if (OB_FAIL(blob_res.init(res_string.length()))) {
          LOG_WARN("init lob result failed");
        } else if (OB_FAIL(blob_res.append(res_string))) {
          LOG_WARN("append lob result failed");
        } else {
          ObString blob_locator;
          blob_res.get_result_buffer(blob_locator);
          res.set_string(blob_locator.ptr(), blob_locator.length());
        }
      }
    }
  } else { // CS_TYPE_BINARY
    // call by internal rewrite or used direclty with blob, need validation ?
    // check is xml binary
    ObMulModeNodeType node_type;
    ObXmlBinaryType binary_head_type;
    ObString val = xml_datum->get_string();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator, ObObjType::ObLongTextType,
                                                          CS_TYPE_UTF8MB4_BIN, true, val))) {
      LOG_WARN("fail to get real data", K(ret), K(val));
    } else if (val.length() == 0) { // do nothing
      res.set_string(xml_datum->get_string());
    } else {
      res.set_string(xml_datum->get_string());
    }
  }
  return ret;
}
#endif

int ObExprSysMakeXML::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_sys_makexml;

  return OB_SUCCESS;
}

}
}
