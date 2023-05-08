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
 * This file is for func extractval.
 */

#include "ob_expr_extract_value.h"
#include "ob_expr_lob_utils.h"
#include "lib/utility/utility.h"
#include "ob_expr_xml_func_helper.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

#define USING_LOG_PREFIX SQL_ENG


using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprExtractValue::ObExprExtractValue(common::ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_XML_EXTRACTVALUE, N_EXTRACTVALUE, MORE_THAN_ONE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprExtractValue::~ObExprExtractValue() {}

int ObExprExtractValue::calc_result_typeN(ObExprResType &type,
                                          ObExprResType *types,
                                          int64_t param_num,
                                          common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num != 2 && param_num != 3)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid argument number", K(ret), K(param_num));
  } else {
    ObObjType in_type = types[0].get_type();
    if (types[0].is_ext() && types[0].get_udt_id() == T_OBJ_XML) {
      types[0].get_calc_meta().set_sql_udt(ObXMLSqlType);
    } else if (!ob_is_xml_sql_type(in_type, types[0].get_subschema_id())) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "-", "-");
      LOG_WARN("inconsistent datatypes", K(ret), K(ob_obj_type_str(in_type)));
    } else {
      for (int8_t i = 1; i < param_num && OB_SUCC(ret); i++) {
        ObObjType param_type = types[i].get_type();
        if (param_type == ObNullType) {
        } else if (ob_is_string_type(param_type)) {
          if (types[i].get_charset_type() != CHARSET_UTF8MB4) {
            types[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
          }
        } else {
          types[i].set_calc_type(ObVarcharType);
          types[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      }
    }

    if (OB_SUCC(ret)) {
      type.set_type(ObVarcharType);
      type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      // length == OB_MAX_ORACLE_VARCHAR_LENGTH is not supported by generated key, use OB_MAX_VARCHAR_LENGTH_KEY instead
      type.set_length(OB_MAX_VARCHAR_LENGTH_KEY);
      type.set_length_semantics(LS_BYTE);
    }
  }
  return ret;
}


int ObExprExtractValue::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_extract_value;
  return OB_SUCCESS;
}

} // sql
} // oceanbase