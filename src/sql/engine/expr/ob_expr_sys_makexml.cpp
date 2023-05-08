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
#include "ob_expr_xml_func_helper.h"

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
