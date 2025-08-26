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
* This file is for func xmlconcat.
*/

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_xml_concat.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/xml/ob_xml_util.h"
#include "sql/engine/expr/ob_expr_xml_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprXmlConcat::ObExprXmlConcat(ObIAllocator &alloc)
    : ObFuncExprOperator(
        alloc,
        T_FUN_SYS_XML_CONCAT,
        N_XML_CONCAT,
        PARAM_NUM_UNKNOWN,
        VALID_FOR_GENERATED_COL,
        NOT_ROW_DIMENSION)
{
}

ObExprXmlConcat::~ObExprXmlConcat()
{
}

int ObExprXmlConcat::calc_result_typeN(
    ObExprResType& type,
    ObExprResType* types_stack,
    int64_t param_num,
    ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  if (!is_called_in_sql()) {
    ret = OB_ERR_SP_LILABEL_MISMATCH;
    LOG_WARN("expr call in pl semantics disallowed", K(ret), K(N_XML_CONCAT));
    LOG_USER_ERROR(OB_ERR_SP_LILABEL_MISMATCH, static_cast<int>(strlen(N_XML_CONCAT)), N_XML_CONCAT);
  } else {
    // The input paras should be n (n >= 0) XMLType obj.
    for (int i = 0; OB_SUCC(ret) && i < param_num; i++) {
      ObObjType in_type = types_stack[i].get_type();
      if (in_type != ObNullType) {
        if (ob_is_xml_pl_type(types_stack[i].get_type(), types_stack[i].get_udt_id())) {
          types_stack[i].get_calc_meta().set_sql_udt(ObXMLSqlType);
        }
        if (!ob_is_xml_sql_type(in_type, types_stack[i].get_subschema_id())) {
          ret = OB_ERR_INVALID_XML_DATATYPE;
          LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, "-", "-");
          LOG_WARN("inconsistent datatypes", K(ret), K(ob_obj_type_str(in_type)));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    type.set_sql_udt(ObXMLSqlType);
  }
  return ret;

}

int ObExprXmlConcat::eval_xml_concat(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &allocator = tmp_alloc_g.get_allocator();
  ObStringBuffer res_xml_buf(&allocator);
  ObMulModeMemCtx* mem_ctx = nullptr;
  if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(&allocator, mem_ctx))) {
    LOG_WARN("fail to create tree memory context", K(ret));
  }
  ObDatumMeta xml_meta;
  xml_meta.type_ = ObLongTextType;
  xml_meta.cs_type_ = CS_TYPE_UTF8MB4_BIN;
  ObVector<ObString> xml_bin_str_vec;
  ObString res_bin_str;
  ObString blob_locator;

  // pack input nodes
  for (int i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
    ObDatum *xml_datum = NULL;
    ObString xml_bin_str;

    if (!ob_is_xml_sql_type(expr.args_[i]->datum_meta_.type_, expr.args_[i]->obj_meta_.get_subschema_id())) {
      LOG_WARN("input is not xmltype, skip");
    } else if (OB_FAIL(ObXMLExprHelper::get_xmltype_from_expr(expr.args_[i], ctx, xml_datum))) {
      LOG_WARN("faild to get_xmltype_from_expr", K(ret), K(i), K(expr.args_[i]));
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator, *xml_datum, xml_meta, true, xml_bin_str))) {
      LOG_WARN("faild to read_real_string_data", K(ret), K(i), K(xml_datum));
    } else if (xml_bin_str.length() == 0) {
      // input is empty, skip
    } else if (OB_FAIL(xml_bin_str_vec.push_back(xml_bin_str))) {
      LOG_WARN("failed to push back xml bin str", K(ret), K(xml_bin_str));
    }
  }

  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session()), "XMLModule"));
  OZ(ObXMLExprHelper::concat_xml_type_nodes(mem_ctx, xml_bin_str_vec, res_bin_str));
  OZ(ObXMLExprHelper::pack_binary_res(expr, ctx, res_bin_str, blob_locator));
  OX(res.set_string(blob_locator.ptr(), blob_locator.length()));
  return ret;
}

int ObExprXmlConcat::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_xml_concat;
  return OB_SUCCESS;
}
} //sql
} //oceanbase
