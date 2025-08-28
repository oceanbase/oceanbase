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
* This file is for func xmlforest.
*/

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_xml_forest.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/xml/ob_xml_util.h"
#include "sql/engine/expr/ob_expr_xml_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprXmlForest::ObExprXmlForest(ObIAllocator &alloc)
    : ObFuncExprOperator(
        alloc,
        T_FUN_SYS_XML_FOREST,
        N_XML_FOREST,
        PARAM_NUM_UNKNOWN,
        VALID_FOR_GENERATED_COL,
        NOT_ROW_DIMENSION)
{
}

ObExprXmlForest::~ObExprXmlForest()
{
}

int ObExprXmlForest::calc_result_typeN(
    ObExprResType& type,
    ObExprResType* types_stack,
    int64_t param_num,
    ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx); 
  INIT_SUCC(ret);
  if (OB_UNLIKELY(param_num % 3 != 0)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid param number", K(ret), K(param_num));
  } else if (!is_called_in_sql()) {
    ret = OB_ERR_SP_LILABEL_MISMATCH;
    LOG_WARN("expr call in pl semantics disallowed", K(ret), K(N_XML_FOREST));
    LOG_USER_ERROR(OB_ERR_SP_LILABEL_MISMATCH, static_cast<int>(strlen(N_XML_FOREST)), N_XML_FOREST);
  } else {
    // check
    for (int i = 0; i < param_num && OB_SUCC(ret); i++) {
      const ObObjType obj_type = types_stack[i].get_type();
      if (i % 3 == 0) {
        // value param
        if (ob_is_string_type(obj_type)) {
          if (types_stack[i].get_collation_type() == CS_TYPE_BINARY) {
            types_stack[i].set_calc_collation_type(CS_TYPE_BINARY);
          } else if (types_stack[i].get_charset_type() != CHARSET_UTF8MB4) {
            types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
          }
        } else if (obj_type == ObUserDefinedSQLType) {
          // xmltype, do nothing
        } else if (obj_type == ObExtendType) {
          types_stack[i].set_calc_type(ObUserDefinedSQLType);
        } else {
          types_stack[i].set_calc_type(ObVarcharType);
          types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      } else if (i % 3 == 1) {
        // tag param
        if (ob_is_null(obj_type)) {
          // do nothing
        } else if (obj_type == ObNumberType) {
          // do nothing
        } else if (types_stack[i].get_collation_type() == CS_TYPE_BINARY) {
          ret = OB_ERR_INVALID_XML_DATATYPE;
          LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, "Character", "-");
          LOG_WARN("Unsupport for string type with binary charset input.", K(ret), K(obj_type));
        } else if (ob_is_string_tc(obj_type)) {
          types_stack[i].set_calc_type(ObVarcharType);
          types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
        } else {
          ret = OB_ERR_INVALID_XML_DATATYPE;
          LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, "Character", "-");
          LOG_WARN("Unsupport for string typee with binary charset input.", K(ret), K(obj_type));
        }
      } else {
        // lable param do nothing
      }
    }
  }
  if (OB_SUCC(ret)) {
    type.set_sql_udt(ObXMLSqlType);
  }
  return ret;
}

int ObExprXmlForest::eval_xml_forest(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &allocator = tmp_alloc_g.get_allocator();
  ObVector<ObString> xml_bin_str_vec;
  ObBinAggSerializer bin_agg(&allocator, ObBinAggType::AGG_XML, static_cast<uint8_t>(M_CONTENT));
  ObString res_bin_str;
  ObString blob_locator;
  ObMulModeMemCtx* mem_ctx = nullptr;
  uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());

  CK(OB_NOT_NULL(ctx.exec_ctx_.get_my_session()));
  OZ(ObXmlUtil::create_mulmode_tree_context(&allocator, mem_ctx));
  CK(expr.arg_cnt_ % 3 == 0);

  for (int i = 0; OB_SUCC(ret) && i < expr.arg_cnt_ / 3; i++) {
    ObDatum *value_datum = NULL;
    ObDatum *tag_datum = NULL;
    ObVector<ObObj, ElementObjCacheStatArena> value_vec;
    ObString name_tag;
    ObXmlDocument *xml_doc = NULL;
    ObString xml_bin_str;

    // value
    if (OB_FAIL(expr.args_[i * 3]->eval(ctx, value_datum))) {
      LOG_WARN("value expr args failed", K(ret), K(expr.args_[i * 3]));
    } else if (OB_FAIL(expr.args_[i * 3 + 1]->eval(ctx, tag_datum))) {
      LOG_WARN("tag expr args failed", K(ret), K(expr.args_[i * 3 + 1]));
    }

    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, "XMLModule"));
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObXMLExprHelper::construct_element_value(
                          allocator,
                          expr.args_[i * 3],
                          value_datum,
                          true,
                          value_vec))) {
      LOG_WARN("construct_element_valuev failed", K(ret));
    } else if(expr.args_[i * 3 + 1]->datum_meta_.type_ == ObNumberType) {
      ret = OB_ERR_INVALID_XML_DATATYPE;
      LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, "Character", "-");
      LOG_WARN("Unsupport for string type with binary charset input.", K(ret), K(expr.args_[i * 3 + 1]->datum_meta_.type_)); 
    } else if (OB_FAIL(ObTextStringHelper::get_string(expr, allocator, i * 3 + 1, tag_datum, name_tag))) {
      LOG_WARN("get xml plain text failed", K(ret));
    }
    // construct xmltype
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObXMLExprHelper::construct_doc(mem_ctx, name_tag, value_vec, nullptr, false, xml_doc))) {
      LOG_WARN("construct_doc failed", K(ret));
    } else if (OB_ISNULL(xml_doc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to pack result, as xml_doc is nullptr", K(ret));
    } else if (OB_FAIL(xml_doc->get_raw_binary(xml_bin_str, &allocator))) {
      LOG_WARN("get raw binary failed", K(ret));
    } else if (OB_FAIL(xml_bin_str_vec.push_back(xml_bin_str))) {
      LOG_WARN("failed to push back.", K(ret), K(xml_bin_str));
    }
  }

  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, "XMLModule"));
  OZ(ObXMLExprHelper::concat_xml_type_nodes(mem_ctx, xml_bin_str_vec, res_bin_str)); 
  OZ(ObXMLExprHelper::pack_binary_res(expr, ctx, res_bin_str, blob_locator));
  OX(res.set_string(blob_locator.ptr(), blob_locator.length()));

  return ret;
}

int ObExprXmlForest::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_xml_forest;
  return OB_SUCCESS;
}
} //sql
} //oceanbase
