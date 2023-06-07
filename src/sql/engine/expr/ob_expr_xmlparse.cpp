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
 * This file is for func xmlparse.
 */

#include "ob_expr_xmlparse.h"
#include "sql/engine/ob_exec_context.h"
#include "ob_expr_xml_func_helper.h"

#define USING_LOG_PREFIX SQL_ENG

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprXmlparse :: ObExprXmlparse(common::ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_XMLPARSE, N_XMLPARSE, MORE_THAN_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprXmlparse::~ObExprXmlparse() {}

int ObExprXmlparse::calc_result_typeN(ObExprResType &type,
                                      ObExprResType *types_stack,
                                      int64_t param_num,
                                      ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  if (OB_UNLIKELY(param_num != 4)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid param number", K(ret), K(param_num));
  } else if (!is_called_in_sql()) {
    ret = OB_ERR_SP_LILABEL_MISMATCH;
    LOG_WARN("expr call in pl semantics disallowed", K(ret), K(N_XMLPARSE));
    LOG_USER_ERROR(OB_ERR_SP_LILABEL_MISMATCH, static_cast<int>(strlen(N_XMLPARSE)), N_XMLPARSE);
  } else {
    // param 0 indicate type is document or content
    // param 1 is xml text for parsing
    // param 2 indicate wellformed or not
    // param 3 is output format only used with xmlagg, should be removed when xml binary ready
    ObObjType xml_type = types_stack[1].get_type();
    if (types_stack[1].get_type() == ObNullType) {
    } else if (ob_is_string_type(xml_type)) {
      if (types_stack[1].get_collation_type() == CS_TYPE_BINARY) {
        ret = OB_ERR_INVALID_XML_DATATYPE;
        LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, "CHAR", ob_obj_type_str(xml_type));
        LOG_WARN("Unsupport for string type with binary charset input.", K(ret), K(xml_type));
      } else if (types_stack[1].get_charset_type() != CHARSET_UTF8MB4) {
        types_stack[1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    } else {
      ret = OB_ERR_INVALID_XML_DATATYPE;
      LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, "CHAR", ob_obj_type_str(xml_type));
      LOG_WARN("invalid type, expect char", K(ret), K(xml_type));
    }

    for (int64_t i = 0; i < param_num && OB_SUCC(ret) && i != 1; i++) {
      if (types_stack[i].get_type() == ObNullType) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param0 type is unexpected", K(types_stack[i].get_type()));
      } else if (types_stack[i].get_type() != ObIntType) {
        types_stack[i].set_calc_type(ObIntType);
      }
    }

    if (OB_SUCC(ret)) {
      type.set_sql_udt(ObXMLSqlType);
    }
  }
  return ret;
}


int ObExprXmlparse::get_clause_opt(const ObExpr &expr,
                                   ObEvalCtx &ctx,
                                   uint8_t index,
                                   uint8_t &type,
                                   uint8_t size_para)
{
  INIT_SUCC(ret);
  int num_args = expr.arg_cnt_;
  if (OB_UNLIKELY(index >= num_args)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid args num", K(index), K(num_args));
  } else {
    ObExpr *xml_arg = expr.args_[index];
    ObObjType val_type = xml_arg->datum_meta_.type_;
    ObDatum *xml_datum = NULL;
    if (OB_FAIL(xml_arg->eval(ctx, xml_datum))) {
      LOG_WARN("eval xml arg failed", K(ret));
    } else if (val_type != ObIntType) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input type error", K(val_type));
    } else {
      int64_t option_type = xml_datum->get_int();
      if (option_type < 0 || option_type >= size_para) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input option type error", K(option_type));
      } else {
        type = static_cast<uint8_t>(option_type);
      }
    }
  }
  return ret;
}

int ObExprXmlparse::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_xmlparse;
  return OB_SUCCESS;
}

} // sql
} // oceanbase