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
 * This file contains implementation for _make_xml_binary.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_priv_xml_binary.h"
#include "ob_expr_lob_utils.h"
#include "ob_expr_xml_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
// Internal expression used for generate binary xml from xmltype data
ObExprPrivXmlBinary::ObExprPrivXmlBinary(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PRIV_MAKE_XML_BINARY, N_PRIV_XML_BINARY, TWO_OR_THREE, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprPrivXmlBinary::calc_result_typeN(ObExprResType& type,
                                           ObExprResType* types_stack,
                                           int64_t param_num,
                                           ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  // 1st param is schema id for validation from column definition
  //   schema id in xml data maybe 0 or different from column definition
  // 2nd param is xmltype data (udt sql type), or varchar(should parse again)
  if (param_num != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param number", K(param_num));
  } else if (!ob_is_oracle_numeric_type(types_stack[0].get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid type", K(ret), K(types_stack[0].get_type()));
  } else if (types_stack[1].is_ext() && types_stack[1].get_udt_id() == T_OBJ_XML) {
    types_stack[1].get_calc_meta().set_sql_udt(ObXMLSqlType);
  } else if (!types_stack[1].is_xml_sql_type()
             && !types_stack[1].is_character_type() // remove later
             && !types_stack[1].is_null()) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid type", K(ret), K(types_stack[1].get_type()));
  }
  if (OB_SUCC(ret)) {
    if (types_stack[1].is_character_type() && types_stack[1].get_charset_type() != CHARSET_UTF8MB4) {
      types_stack[1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN); // remove later
    }
    type.set_type(ObLongTextType);
    type.set_collation_type(CS_TYPE_BINARY);
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObLongTextType]);
  }

  return ret;
}


int ObExprPrivXmlBinary::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSEDx(expr_cg_ctx, raw_expr);
  rt_expr.eval_func_ = eval_priv_xml_binary;

  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase