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
 * This file is for func updatexml.
 */

#include "ob_expr_update_xml.h"
#include "ob_expr_xml_func_helper.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#define USING_LOG_PREFIX SQL_ENG


using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprUpdateXml::ObExprUpdateXml(common::ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_UPDATE_XML, N_UPDATEXML, MORE_THAN_TWO, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprUpdateXml::~ObExprUpdateXml() {}

int ObExprUpdateXml::calc_result_typeN(ObExprResType &type,
                                       ObExprResType *types,
                                       int64_t param_num,
                                       common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (param_num < 3) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid param number", K(ret), K(param_num));
  } else if (!is_called_in_sql()) {
    ret = OB_ERR_SP_LILABEL_MISMATCH;
    LOG_WARN("expr call in pl semantics disallowed", K(ret), K(N_UPDATEXML));
    LOG_USER_ERROR(OB_ERR_SP_LILABEL_MISMATCH, static_cast<int>(strlen(N_UPDATEXML)), N_UPDATEXML);
  } else if (types[0].is_ext() && types[0].get_udt_id() == T_OBJ_XML) {
      types[0].get_calc_meta().set_sql_udt(ObXMLSqlType);
  } else if (!ob_is_xml_sql_type(types[0].get_type(), types[0].get_subschema_id())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "-", "-");
    LOG_WARN("inconsistent datatypes", K(ret), K(ob_obj_type_str(types[0].get_type())));
  } else {
    bool has_ns_str = (param_num - 1) % 2 == 1;
    int64_t xpath_value_end = has_ns_str ? param_num - 1 : param_num;
    for (int64_t i = 1; i < xpath_value_end && OB_SUCC(ret); i++) {
      ObObjType param_type = types[i].get_type();
      ObCollationType cs_type = types[i].get_collation_type();
      if (i % 2 == 1) {
        // xpath string
        if (param_type == ObNullType) {
        } else if (ob_is_string_type(param_type)) {
          if (types[i].get_charset_type() != CHARSET_UTF8MB4) {
            types[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
          }
        }
      } else {
        // value expr
        if (param_type == ObNullType || ob_is_xml_sql_type(param_type, types[i].get_subschema_id())) {
        } else if (types[i].is_ext() && types[i].get_udt_id() == T_OBJ_XML) {
          types[i].get_calc_meta().set_sql_udt(ObXMLSqlType);
        } else if (ob_is_clob(param_type, cs_type) || ob_is_blob(param_type, cs_type)) {
          types[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        } else {
          types[i].set_calc_type(ObVarcharType);
          types[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      }
    }
    if (has_ns_str) {
      ObObjType param_type = types[param_num - 1].get_type();
      if (param_type == ObNullType) {
      } else if (ob_is_string_type(param_type)) {
        if (types[param_num - 1].get_charset_type() != CHARSET_UTF8MB4) {
          types[param_num - 1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    type.set_sql_udt(ObXMLSqlType);
  }
  return ret;
}


int ObExprUpdateXml::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_update_xml;
  return OB_SUCCESS;
}

} // sql
} // oceanbase