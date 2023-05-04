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
 */

#define USING_LOG_PREFIX SQL_ENG
#include <string.h>
#include "objit/common/ob_item_type.h"
#include "sql/engine/expr/ob_expr_utl_i18n.h"
#include "sql/engine/expr/ob_expr_hex.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "share/object/ob_obj_cast.h"
#include "lib/oblog/ob_log.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

// UTL_I18N.STRING_TO_RAW begin
ObExprUtlI18nStringToRaw::ObExprUtlI18nStringToRaw(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_UTL_I18N_STRING_TO_RAW, N_UTL_I18N_STRING_TO_RAW, 2,
                           VALID_FOR_GENERATED_COL, false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprUtlI18nStringToRaw::~ObExprUtlI18nStringToRaw()
{
}

int ObExprUtlI18nStringToRaw::calc_result_type2(
    ObExprResType &type,
    ObExprResType &type1,
    ObExprResType &type2,
    common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  if (ob_is_null(type1.get_type())) {
    type.set_null();
  } else {
    type1.set_calc_type(common::ObVarcharType);
    type.set_raw();
    type.set_collation_level(common::CS_LEVEL_NUMERIC);
    if (ob_is_null(type2.get_type())) {
    } else {
      type2.set_calc_type(common::ObVarcharType);
    }
  }

  return ret;
}

int ObExprUtlI18nStringToRaw::calc(
    common::ObObj &result,
    const common::ObObj &obj1,
    const common::ObObj &obj2,
    common::ObCastCtx &cast_ctx,
    const ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;

  if (obj1.is_null()) {
    result.set_null();
  } else if (OB_UNLIKELY(!obj1.is_varchar())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input is not varchar", K(ret), K(obj1), K(obj2));
  } else {
    if (OB_ISNULL(cast_ctx.allocator_v2_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("allocator in cast ctx is NULL", K(ret));
    } else if (CS_TYPE_INVALID == obj1.get_collation_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("collation type of input is invalid", K(ret), K(obj1));
    } else {
      ObCollationType dest_collation = CS_TYPE_INVALID;
      ObString dest_str;
      if (obj2.is_null()) {
        dest_collation = session_info->get_nls_collation();
      } else { // obj2.is_varchar()
        dest_collation = ObCharset::get_default_collation_oracle(
            ObCharset::charset_type_by_name_oracle(obj2.get_string()));
      }
      if (OB_FAIL(ret)) {
      } else if (CS_TYPE_INVALID != dest_collation) {
        if (OB_FAIL(ObExprUtil::convert_string_collation(obj1.get_string(),
                                             obj1.get_collation_type(),
                                             dest_str,
                                             dest_collation,
                                             *cast_ctx.allocator_v2_))) {
          LOG_WARN("fail to convert string collation", K(ret), K(obj1),
                                                       K(dest_str),
                                                       K(dest_collation));
        } else {
          result.set_raw(dest_str.ptr(), dest_str.length());
        }
      } else {
        result.set_null();
      }
    }
  }

  return ret;
}


// UTL_I18N.STRING_TO_RAW end

// UTL_I18N.RAW_TO_CHAR begin
ObExprUtlI18nRawToChar::ObExprUtlI18nRawToChar(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_UTL_I18N_RAW_TO_CHAR, N_UTL_I18N_RAW_TO_CHAR, 2, VALID_FOR_GENERATED_COL,
                           false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprUtlI18nRawToChar::~ObExprUtlI18nRawToChar()
{
}

int ObExprUtlI18nRawToChar::calc_result_type2(
    ObExprResType &type,
    ObExprResType &type1,
    ObExprResType &type2,
    common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  const ObBasicSessionInfo *session_info = NULL;
  if (OB_ISNULL(session_info = type_ctx.get_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else if (ob_is_null(type1.get_type())) {
    type.set_null();
  } else if (ob_is_string_type(type1.get_type()) || ob_is_raw(type1.get_type())) {
    type.set_varchar();
    type.set_collation_level(common::CS_LEVEL_COERCIBLE);
    type.set_collation_type(session_info->get_nls_collation());
    if (ob_is_null(type2.get_type())) {
    } else {
      type2.set_calc_type(common::ObVarcharType);
    }
  } else {
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_WARN("wrong type of arguments in function", K(ret), K(type1));
    ObString func_name("RAW_TO_CHAR");
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  }

  return ret;
}

int ObExprUtlI18nRawToChar::calc(
    common::ObObj &result,
    const common::ObObj &obj1,
    const common::ObObj &obj2,
    common::ObCastCtx &cast_ctx,
    const ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;

  if (obj1.is_null()) {
    result.set_null();
  } else if (OB_UNLIKELY(!obj1.is_raw() && !obj1.is_string_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong number or types of arguments in function", K(ret), K(obj1));
  } else if (OB_ISNULL(cast_ctx.allocator_v2_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator in cast ctx is NULL", K(ret));
  } else {
    ObObj tmp_obj;
    ObString src_str;
    ObString dest_str;
    ObCollationType src_collation = CS_TYPE_INVALID;
    if (obj1.is_raw()) {
      src_str = obj1.get_string();
    } else {
      if (OB_FAIL(ObHexUtils::hextoraw(obj1, cast_ctx, tmp_obj))) {
        LOG_WARN("fail to hextoraw", K(ret), K(obj1));
      } else {
        src_str = tmp_obj.get_string();
      }
    }
    if (OB_FAIL(ret)) {
    } else if (obj2.is_null()) {
      src_collation = session_info->get_nls_collation();
    } else { // obj2.is_varchar()
      src_collation = ObCharset::get_default_collation_oracle(
          ObCharset::charset_type_by_name_oracle(obj2.get_string()));
    }
    if (OB_FAIL(ret)) {
    } else if (CS_TYPE_INVALID != src_collation) {
      if (OB_FAIL(ObExprUtil::convert_string_collation(
                  src_str,
                  src_collation,
                  dest_str,
                  session_info->get_nls_collation(),
                  *cast_ctx.allocator_v2_))) {
        LOG_WARN("fail to convert string collation", K(ret), K(obj1),
                                                     K(src_collation));
      } else {
        result.set_varchar(dest_str.ptr(), dest_str.length());
        result.set_collation_type(session_info->get_nls_collation());
      }
    } else {
      result.set_null();
    }
  }

  return ret;
}

// UTL_I18N.RAW_TO_CHAR end

}
}
