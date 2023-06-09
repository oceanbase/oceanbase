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
 * This file is for func xmlelement.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_xml_element.h"
#include "ob_expr_xml_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprXmlElement::ObExprXmlElement(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_XML_ELEMENT, N_XML_ELEMENT, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprXmlElement::~ObExprXmlElement()
{
}

int ObExprXmlElement::calc_result_typeN(ObExprResType& type,
                                      ObExprResType* types_stack,
                                      int64_t param_num,
                                      ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num < 3)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid param number", K(ret), K(param_num));
  } else if (!is_called_in_sql()) {
    ret = OB_ERR_SP_LILABEL_MISMATCH;
    LOG_WARN("expr call in pl semantics disallowed", K(ret), K(N_XML_ELEMENT));
    LOG_USER_ERROR(OB_ERR_SP_LILABEL_MISMATCH, static_cast<int>(strlen(N_XML_ELEMENT)), N_XML_ELEMENT);
  } else {
    // check opt_escaping
    if (!ob_is_integer_type(types_stack[0].get_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid escaping opt type", K(ret), K(types_stack[0].get_type()));
    } else {
      for (int i = 2; i < param_num && OB_SUCC(ret); i++) {
        const ObObjType obj_type = types_stack[i].get_type();
        if (i == 2) {
          if (ob_is_null(obj_type)) {
            // do nothing
          } else if (obj_type == ObNumberType) {
            // do nothing
          } else if (types_stack[i].get_collation_type() == CS_TYPE_BINARY) {
            ret = OB_ERR_INVALID_XML_DATATYPE;
            LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, "Character", "-");
            LOG_WARN("Unsupport for string typee with binary charset input.", K(ret), K(obj_type));
          } else if (ob_is_string_tc(obj_type)) {
            types_stack[i].set_calc_type(ObVarcharType);
            types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
          } else {
            ret = OB_ERR_INVALID_XML_DATATYPE;
            LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, "Character", "-");
            LOG_WARN("Unsupport for string typee with binary charset input.", K(ret), K(obj_type));
          }
        } else if (i == 3 && ob_is_json(obj_type)) {
          // do nothing, result from xmlAttributes
        } else if (ob_is_string_type(obj_type)) {
          if (types_stack[i].get_collation_type() == CS_TYPE_BINARY) {
            types_stack[i].set_calc_collation_type(CS_TYPE_BINARY);
          } else if (types_stack[i].get_charset_type() != CHARSET_UTF8MB4) {
            types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
          }
        } else if (ObUserDefinedSQLType == types_stack[i].get_type()) { // xmltype
          types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        } else if (ObExtendType == types_stack[i].get_type()) {
          types_stack[i].set_calc_type(ObUserDefinedSQLType);
          types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        } else {
          types_stack[i].set_calc_type(ObVarcharType);
          types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    type.set_sql_udt(ObXMLSqlType);
  }
  return ret;
}


int ObExprXmlElement::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_xml_element;
  return OB_SUCCESS;
}

}
}