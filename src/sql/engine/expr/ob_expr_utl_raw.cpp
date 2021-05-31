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
#include "sql/parser/ob_item_type.h"
#include "sql/engine/expr/ob_expr_utl_raw.h"
#include "sql/engine/expr/ob_expr_hex.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "share/object/ob_obj_cast.h"
#include "lib/oblog/ob_log.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

ObExprUtlRawCastToRaw::ObExprUtlRawCastToRaw(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_UTL_RAW_CAST_TO_RAW, N_UTL_RAW_CAST_TO_RAW, 1)
{}

ObExprUtlRawCastToRaw::~ObExprUtlRawCastToRaw()
{}

inline int ObExprUtlRawCastToRaw::calc_result_type1(
    ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  common::ObObjType param_type = text.get_type();
  const int64_t oracle_max_avail_len = 40;

  if (!text.is_string_type()) {
    text.set_calc_type_default_varchar();
  } else {
    text.set_calc_type(common::ObVarcharType);  // need set
  }
  if (ob_is_null(param_type)) {
    type.set_null();
  } else {
    type.set_raw();
    type.set_collation_level(common::CS_LEVEL_NUMERIC);
    type.set_length(common::OB_MAX_ORACLE_RAW_PL_VAR_LENGTH);
  }

  return OB_SUCCESS;
}

int ObExprUtlRawCastToRaw::calc(ObObj& result, const ObObj& text, ObCastCtx& cast_ctx)
{
  int ret = OB_SUCCESS;

  if (text.is_null()) {
    result.set_null();
  } else if (OB_UNLIKELY(!text.is_varchar())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("text is not varchar", K(ret), K(text), K(text.get_type()), K(text.get_collation_type()));
  } else {
    const ObString str = text.get_string();
    char* buf = NULL;
    if (str.length() > OB_MAX_ORACLE_RAW_PL_VAR_LENGTH) {
      ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
      ObString err_msg("raw variable length too long");
      LOG_WARN("raw variable length too long", K(ret), K(str.length()));
      LOG_USER_ERROR(OB_ERR_NUMERIC_OR_VALUE_ERROR, err_msg.length(), err_msg.ptr());
    } else if (OB_ISNULL(cast_ctx.allocator_v2_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("allocator in cast ctx is NULL", K(ret), K(text));
    } else if (OB_ISNULL(buf = static_cast<char*>(cast_ctx.allocator_v2_->alloc(str.length())))) {
      result.set_null();
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret), "length", str.length());
    } else {
      MEMCPY(buf, str.ptr(), str.length());
      result.set_raw(buf, str.length());
      LOG_DEBUG("succ to calc", K(text), K(result));
    }
  }

  return ret;
}

int ObExprUtlRawCastToRaw::calc_result1(common::ObObj& result, const ObObj& text, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    if (OB_FAIL(calc(result, text, cast_ctx))) {
      LOG_WARN("fail to calc", K(ret), K(text));
    }
  }

  return ret;
}

ObExprUtlRawCastToVarchar2::ObExprUtlRawCastToVarchar2(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_UTL_RAW_CAST_TO_VARCHAR2, N_UTL_RAW_CAST_TO_VARCHAR2, 1)
{
  disable_operand_auto_cast();
}

ObExprUtlRawCastToVarchar2::~ObExprUtlRawCastToVarchar2()
{}

inline int ObExprUtlRawCastToVarchar2::calc_result_type1(
    ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  // calc length now...
  common::ObObjType param_type = text.get_type();
  common::ObLength length = -1;
  const ObSQLSessionInfo* session = type_ctx.get_session();
  if (ob_is_null(param_type)) {
    type.set_null();
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (ob_is_string_type(param_type) || ob_is_raw(param_type)) {
    type.set_varchar();
    type.set_collation_level(common::CS_LEVEL_COERCIBLE);
    type.set_collation_type(get_default_collation_type(type.get_type(), *type_ctx.get_session()));
    type.set_collation_type(session->get_nls_collation());
    length = text.get_length();
    if (length <= 0 || length > common::OB_MAX_VARCHAR_LENGTH) {
      length = common::OB_MAX_VARCHAR_LENGTH;
    }
    type.set_length(length);
  } else {
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_WARN("wrong type of arguments in function", K(ret), K(text), K(text.get_type()), K(text.get_collation_type()));
    ObString func_name("CAST_TO_VARCHAR2");
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  }

  return ret;
}

int ObExprUtlRawCastToVarchar2::calc(ObObj& result, const ObObj& text, ObCastCtx& cast_ctx)
{
  int ret = OB_SUCCESS;

  if (text.is_null()) {
    result.set_null();
  } else if (OB_UNLIKELY(!text.is_raw() && !text.is_string_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong number or types of arguments in function",
        K(ret),
        K(text),
        K(text.get_type()),
        K(text.get_collation_type()));
  } else {
    char* buf = NULL;
    ObString str;
    ObObj out;
    if (text.is_raw()) {
      str = text.get_string();
    } else {
      if (OB_FAIL(ObHexUtils::hextoraw(text, cast_ctx, out))) {
        LOG_WARN("fail to hextoraw", K(ret), K(text), K(text.get_type()), K(text.get_collation_type()));
      } else {
        str = out.get_string();
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(cast_ctx.allocator_v2_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("allocator in cast ctx is NULL", K(ret), K(text));
      } else if (OB_ISNULL(buf = static_cast<char*>(cast_ctx.allocator_v2_->alloc(str.length())))) {
        result.set_null();
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(ret), "length", str.length());
      } else {
        MEMCPY(buf, str.ptr(), str.length());
        result.set_varchar(buf, str.length());
        LOG_DEBUG("succ to calc", K(text), K(result));
      }
    }
  }

  return ret;
}

int ObExprUtlRawCastToVarchar2::calc_result1(common::ObObj& result, const ObObj& text, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    if (OB_FAIL(calc(result, text, cast_ctx))) {
      LOG_WARN("fail to calc", K(ret), K(text));
    } else if (OB_LIKELY(!result.is_null())) {
      result.set_collation(result_type_);
      LOG_DEBUG("succ to calc_result1", K(text), K(result));
    }
  }

  return ret;
}

ObExprUtlRawLength::ObExprUtlRawLength(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_UTL_RAW_LENGTH, N_UTL_RAW_LENGTH, 1, NOT_ROW_DIMENSION)
{}

ObExprUtlRawLength::~ObExprUtlRawLength()
{}

inline int ObExprUtlRawLength::calc_result_type1(
    ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  text.set_calc_type(common::ObRawType);  // need set
  common::ObObjType param_type = text.get_type();
  if (ob_is_null(param_type)) {
    type.set_null();
  } else if (ob_is_string_type(param_type) || ob_is_raw(param_type)) {
    type.set_int();
    type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
    type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
    type.set_calc_scale(text.get_scale());
  } else {
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_WARN("wrong type of argument in function", K(ret), K(text), K(text.get_type()), K(text.get_collation_type()));
    ObString func_name("LENGTH");
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  }

  return ret;
}

int ObExprUtlRawLength::calc_result1(common::ObObj& result, const ObObj& text, ObExprCtx& expr_ctx) const
{
  UNUSED(expr_ctx);
  int ret = OB_SUCCESS;

  if (text.is_null()) {
    result.set_null();
  } else if (OB_UNLIKELY(!text.is_raw())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong type of argument in function", K(ret), K(text), K(text.get_type()), K(text.get_collation_type()));
  } else {
    ObString str = text.get_string();
    if (str.length() > OB_MAX_ORACLE_RAW_PL_VAR_LENGTH) {
      ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
      ObString err_msg("raw variable length too long");
      LOG_WARN("raw variable length too long", K(ret), K(str.length()));
      LOG_USER_ERROR(OB_ERR_NUMERIC_OR_VALUE_ERROR, err_msg.length(), err_msg.ptr());
    } else {
      result.set_int(static_cast<int64_t>(str.length()));
      LOG_DEBUG("succ to calc", K(text), K(result));
    }
  }

  return ret;
}

int ObRawBitwiseExprOperator::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type1);
  UNUSED(type2);
  UNUSED(type_ctx);
  type.set_raw();
  return OB_SUCCESS;
}

int ObRawBitwiseExprOperator::calc_result_type2_(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  type1.set_calc_type(common::ObRawType);
  type2.set_calc_type(common::ObRawType);
  common::ObObjType param_type_1 = type1.get_type();
  common::ObObjType param_type_2 = type2.get_type();
  if (ob_is_null(param_type_1) || ob_is_null(param_type_2)) {
    type.set_null();
  } else if (((ob_is_string_type(param_type_1) || ob_is_raw(param_type_1)) &&
                 ((ob_is_string_type(param_type_2) || ob_is_raw(param_type_2))))) {
    type.set_collation_level(common::CS_LEVEL_NUMERIC);
    type.set_raw();
    type.set_length(common::OB_MAX_ORACLE_RAW_PL_VAR_LENGTH);
  } else {
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_WARN(
        "wrong type of argument in function", K(ret), K(type1), K(type1.get_type()), K(type2), K(type2.get_type()));
  }

  return ret;
}

int ObRawBitwiseExprOperator::calc_result2_(
    ObObj& result, const ObObj& obj1, const ObObj& obj2, ObCastCtx& cast_ctx, BitOperator op) const
{
  UNUSED(cast_ctx);
  int ret = OB_SUCCESS;

  if (OB_ISNULL(cast_ctx.allocator_v2_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator in cast ctx is NULL", K(ret));
  } else if (OB_UNLIKELY(op < 0 || op >= BIT_MAX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index", K(ret), K(op), K(obj1), K(obj2));
  } else if (obj1.is_null() || obj2.is_null()) {
    result.set_null();
  } else if (OB_UNLIKELY(!obj1.is_raw() || !obj2.is_raw())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("text is not raw", K(ret), K(obj1.get_type()), K(obj2.get_type()));
  } else {
    char* buf = NULL;
    ObString str_res;
    ObString str1 = obj1.get_string();
    ObString str2 = obj2.get_string();
    int32_t loop_len = -1;  // the lenth of shorter raw
    int32_t res_len = -1;   // the lenth of longer raw
    if (str1.length() < str2.length()) {
      loop_len = str1.length();
      res_len = str2.length();
    } else {
      loop_len = str2.length();
      res_len = str1.length();
    }
    if (res_len > OB_MAX_ORACLE_RAW_PL_VAR_LENGTH) {
      ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
      ObString err_msg("raw variable length too long");
      LOG_WARN("raw variable length too long", K(ret), K(res_len));
      LOG_USER_ERROR(OB_ERR_NUMERIC_OR_VALUE_ERROR, err_msg.length(), err_msg.ptr());
    } else if (OB_ISNULL(buf = static_cast<char*>(cast_ctx.allocator_v2_->alloc(res_len)))) {
      result.set_null();
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret), "length", res_len);
    } else {
      MEMCPY(buf, ((str1.length() == res_len) ? str1.ptr() : str2.ptr()), res_len);
      switch (op) {
        case BIT_AND: {
          for (int i = 0; i < loop_len; ++i) {
            buf[i] = (char)(str1[i] & str2[i]);
          }
          break;
        }
        case BIT_OR: {
          for (int i = 0; i < loop_len; ++i) {
            buf[i] = (char)(str1[i] | str2[i]);
          }
          break;
        }
        case BIT_XOR: {
          for (int i = 0; i < loop_len; ++i) {
            buf[i] = (char)(str1[i] ^ str2[i]);
          }
          break;
        }
        default:
          break;  // won't come here
      }
      result.set_raw(buf, res_len);
      LOG_DEBUG("succ to calc", K(obj1), K(obj2), K(result));
    }
  }

  return ret;
}

ObExprUtlRawBitAnd::ObExprUtlRawBitAnd(ObIAllocator& alloc)
    : ObRawBitwiseExprOperator(alloc, T_FUN_SYS_UTL_RAW_BIT_AND, N_UTL_RAW_BIT_AND, 2)
{}

ObExprUtlRawBitAnd::~ObExprUtlRawBitAnd()
{}

int ObExprUtlRawBitAnd::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObRawBitwiseExprOperator::calc_result_type2_(type, type1, type2, type_ctx))) {
    ObString func_name("BIT_AND");
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  }

  return ret;
}

int ObExprUtlRawBitAnd::calc_result2(
    common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("buffer not init", K(ret));
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    if (OB_FAIL(
            ObRawBitwiseExprOperator::calc_result2_(result, obj1, obj2, cast_ctx, ObRawBitwiseExprOperator::BIT_AND))) {
      LOG_WARN("fail to calc", K(ret), K(obj1), K(obj2));
    } else {
      LOG_DEBUG("succ to calc_result", K(obj1), K(obj2), K(result));
    }
  }

  return ret;
}

ObExprUtlRawBitOr::ObExprUtlRawBitOr(ObIAllocator& alloc)
    : ObRawBitwiseExprOperator(alloc, T_FUN_SYS_UTL_RAW_BIT_OR, N_UTL_RAW_BIT_OR, 2)
{}

ObExprUtlRawBitOr::~ObExprUtlRawBitOr()
{}

int ObExprUtlRawBitOr::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObRawBitwiseExprOperator::calc_result_type2_(type, type1, type2, type_ctx))) {
    ObString func_name("BIT_OR");
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  }

  return ret;
}

int ObExprUtlRawBitOr::calc_result2(
    common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("buffer not init", K(ret));
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    if (OB_FAIL(
            ObRawBitwiseExprOperator::calc_result2_(result, obj1, obj2, cast_ctx, ObRawBitwiseExprOperator::BIT_OR))) {
      LOG_WARN("fail to calc", K(ret), K(obj1), K(obj2));
    } else {
      LOG_DEBUG("succ to calc_result", K(obj1), K(obj2), K(result));
    }
  }

  return ret;
}

ObExprUtlRawBitXor::ObExprUtlRawBitXor(ObIAllocator& alloc)
    : ObRawBitwiseExprOperator(alloc, T_FUN_SYS_UTL_RAW_BIT_XOR, N_UTL_RAW_BIT_XOR, 2)
{}

ObExprUtlRawBitXor::~ObExprUtlRawBitXor()
{}

int ObExprUtlRawBitXor::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObRawBitwiseExprOperator::calc_result_type2_(type, type1, type2, type_ctx))) {
    ObString func_name("BIT_XOR");
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  }

  return ret;
}

int ObExprUtlRawBitXor::calc_result2(
    common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("buffer not init", K(ret));
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    if (OB_FAIL(
            ObRawBitwiseExprOperator::calc_result2_(result, obj1, obj2, cast_ctx, ObRawBitwiseExprOperator::BIT_XOR))) {
      LOG_WARN("fail to calc", K(ret), K(obj1), K(obj2));
    } else {
      LOG_DEBUG("succ to calc_result", K(obj1), K(obj2), K(result));
    }
  }

  return ret;
}

ObExprUtlRawBitComplement::ObExprUtlRawBitComplement(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_UTL_RAW_BIT_COMPLEMENT, N_UTL_RAW_BIT_COMPLEMENT, 1)
{}

ObExprUtlRawBitComplement::~ObExprUtlRawBitComplement()
{}

int ObExprUtlRawBitComplement::calc_result_type1(
    ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  type1.set_calc_type(common::ObRawType);
  common::ObObjType param_type_1 = type1.get_type();
  if (ob_is_null(param_type_1)) {
    type.set_null();
  } else if (ob_is_string_type(param_type_1) || ob_is_raw(param_type_1)) {
    type.set_collation_level(common::CS_LEVEL_NUMERIC);
    type.set_raw();
    type.set_length(common::OB_MAX_ORACLE_RAW_PL_VAR_LENGTH);
  } else {
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_WARN("wrong type of argument in function", K(ret), K(type1), K(type1.get_type()));
    ObString func_name("BIT_COMPLEMENT");
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  }

  return ret;
}

int ObExprUtlRawBitComplement::calc_result1(common::ObObj& result, const common::ObObj& obj1, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("buffer not init", K(ret));
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    if (OB_ISNULL(cast_ctx.allocator_v2_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("allocator in cast ctx is NULL", K(ret));
    } else if (obj1.is_null()) {
      result.set_null();
    } else if (OB_UNLIKELY(!obj1.is_raw())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("text is not raw", K(ret), K(obj1.get_type()));
    } else {
      char* buf = NULL;
      ObString str = obj1.get_string();
      int32_t res_len = str.length();
      if (res_len > OB_MAX_ORACLE_RAW_PL_VAR_LENGTH) {
        ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
        ObString err_msg("raw variable length too long");
        LOG_WARN("raw variable length too long", K(ret), K(res_len));
        LOG_USER_ERROR(OB_ERR_NUMERIC_OR_VALUE_ERROR, err_msg.length(), err_msg.ptr());
      } else if (OB_ISNULL(buf = static_cast<char*>(cast_ctx.allocator_v2_->alloc(res_len)))) {
        result.set_null();
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(ret), "length", res_len);
      } else {
        MEMCPY(buf, str.ptr(), res_len);
        for (int i = 0; i < res_len; ++i) {
          buf[i] = (char)(~str[i]);
        }
        result.set_raw(buf, res_len);
        LOG_DEBUG("succ to calc", K(obj1), K(result));
      }
    }
  }

  return ret;
}

ObExprUtlRawReverse::ObExprUtlRawReverse(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_UTL_RAW_REVERSE, N_UTL_RAW_REVERSE, 1)
{}

ObExprUtlRawReverse::~ObExprUtlRawReverse()
{}

int ObExprUtlRawReverse::calc_result_type1(
    ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  type1.set_calc_type(common::ObRawType);
  common::ObObjType param_type_1 = type1.get_type();

  if (ob_is_null(param_type_1)) {
    ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
    LOG_WARN("type can not be null", K(ret));
  } else if (ob_is_string_type(param_type_1) || ob_is_raw(param_type_1)) {
    type.set_collation_level(common::CS_LEVEL_NUMERIC);
    type.set_raw();
    type.set_length(common::OB_MAX_ORACLE_RAW_PL_VAR_LENGTH);
  } else {
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_WARN("wrong type of argument in function", K(ret), K(type1), K(type1.get_type()));
    ObString func_name("REVERSE");
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  }

  return ret;
}

int ObExprUtlRawReverse::calc(ObObj& result, const ObObj& text, ObCastCtx& cast_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(text.is_null())) {
    ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
    LOG_WARN("type can not be null", K(ret));
  } else if (OB_UNLIKELY(!text.is_raw())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong type of argument in function", K(ret), K(text), K(text.get_type()), K(text.get_collation_type()));
  } else {
    char* buf = NULL;
    ObString str = text.get_string();
    int32_t res_len = str.length();
    if (str.length() > OB_MAX_ORACLE_RAW_PL_VAR_LENGTH) {
      ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
      ObString err_msg("raw variable length too long");
      LOG_WARN("raw variable length too long", K(ret), K(str.length()));
      LOG_USER_ERROR(OB_ERR_NUMERIC_OR_VALUE_ERROR, err_msg.length(), err_msg.ptr());
    } else if (OB_ISNULL(buf = static_cast<char*>(cast_ctx.allocator_v2_->alloc(res_len)))) {
      result.set_null();
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret), "length", res_len);
    } else {
      MEMCPY(buf, str.ptr(), res_len);
      int32_t l_pos = 0;
      int32_t r_pos = res_len;
      while ((l_pos != r_pos) && (l_pos != --r_pos)) {
        std::swap(buf[l_pos++], buf[r_pos]);
      }
      result.set_raw(buf, res_len);
      LOG_DEBUG("succ to calc", K(text), K(result));
    }
  }

  return ret;
}

int ObExprUtlRawReverse::calc_result1(common::ObObj& result, const ObObj& text, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    if (OB_FAIL(calc(result, text, cast_ctx))) {
      LOG_WARN("fail to calc", K(ret), K(text));
    } else {
      LOG_DEBUG("succ to calc_result1", K(text), K(result));
    }
  }

  return ret;
}

ObExprUtlRawCopies::ObExprUtlRawCopies(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_UTL_RAW_COPIES, N_UTL_RAW_COPIES, 2)
{}

ObExprUtlRawCopies::~ObExprUtlRawCopies()
{}

int ObExprUtlRawCopies::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  type1.set_calc_type(common::ObRawType);
  type2.set_calc_type(common::ObIntType);
  common::ObObjType param_type_1 = type1.get_type();
  common::ObObjType param_type_2 = type2.get_type();
  if ((ob_is_string_type(param_type_1) || ob_is_raw(param_type_1)) &&
      (ob_is_string_type(param_type_2) || ob_is_numeric_type(param_type_2))) {
    type.set_collation_level(common::CS_LEVEL_NUMERIC);
    type.set_raw();
    type.set_length(common::OB_MAX_ORACLE_RAW_PL_VAR_LENGTH);
  } else {
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    ObString func_name("COPIES");
    LOG_WARN(
        "wrong type of argument in function", K(ret), K(type1), K(type1.get_type()), K(type2), K(type2.get_type()));
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  }

  return ret;
}

int ObExprUtlRawCopies::calc(
    common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObCastCtx& cast_ctx)
{
  UNUSED(cast_ctx);
  int ret = OB_SUCCESS;
  char* buf = NULL;
  ObString str = obj1.get_string();
  int32_t copy_times = obj2.get_int32();
  int32_t str_len = str.length();
  int32_t res_len = str_len * copy_times;

  if (OB_UNLIKELY(obj1.is_null())) {
    ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
    LOG_WARN("type can not be null", K(ret));
  } else if (OB_UNLIKELY(copy_times <= 0)) {
    ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
    LOG_WARN("value of argument in function is wrong", K(ret), K(copy_times));
  } else if (OB_UNLIKELY(res_len > OB_MAX_ORACLE_RAW_PL_VAR_LENGTH)) {
    ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
    ObString err_msg("raw variable length too long");
    LOG_WARN("raw variable length too long", K(ret), K(res_len));
    LOG_USER_ERROR(OB_ERR_NUMERIC_OR_VALUE_ERROR, err_msg.length(), err_msg.ptr());
  } else {
    if (OB_ISNULL(buf = static_cast<char*>(cast_ctx.allocator_v2_->alloc(res_len)))) {
      result.set_null();
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret), K(res_len));
    } else {
      int32_t buf_pos = 0;
      for (int64_t i = 0; i < copy_times; ++i) {
        MEMCPY(buf + buf_pos, str.ptr(), str_len);
        buf_pos += str_len;
      }
      result.set_raw(buf, res_len);
      LOG_DEBUG("succ to calc", K(obj1), K(obj2), K(result));
    }
  }

  return ret;
}

int ObExprUtlRawCopies::calc_result2(
    common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("buffer not init", K(ret));
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    if (OB_FAIL(calc(result, obj1, obj2, cast_ctx))) {
      LOG_WARN("fail to calc", K(ret), K(obj1), K(obj2));
    } else {
      LOG_DEBUG("succ to calc_result", K(obj1), K(obj2), K(result));
    }
  }

  return ret;
}

ObExprUtlRawCompare::ObExprUtlRawCompare(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_UTL_RAW_COMPARE, N_UTL_RAW_COMPARE, TWO_OR_THREE)
{}

ObExprUtlRawCompare::~ObExprUtlRawCompare()
{}

int ObExprUtlRawCompare::calc_result_typeN(
    ObExprResType& type, ObExprResType* types_stack, int64_t param_num, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(2 != param_num && 3 != param_num)) {
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_WARN("compare should have two or three arguments", K(param_num), K(ret));
  } else {  // (2 == param_num) || (3 == param_num)
    types_stack[0].set_calc_type(common::ObRawType);
    types_stack[1].set_calc_type(common::ObRawType);
    common::ObObjType param_type_1 = types_stack[0].get_type();
    common::ObObjType param_type_2 = types_stack[1].get_type();
    if (((ob_is_null(param_type_1) || ob_is_string_type(param_type_1) || ob_is_raw(param_type_1)) &&
            ((ob_is_null(param_type_2) || ob_is_string_type(param_type_2) || ob_is_raw(param_type_2))))) {
    } else {
      ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
      LOG_WARN("wrong type of argument in function",
          K(ret),
          K(types_stack[0]),
          K(types_stack[0].get_type()),
          K(types_stack[1]),
          K(types_stack[1].get_type()));
    }
  }
  if (OB_SUCC(ret) && (3 == param_num)) {
    types_stack[2].set_calc_type(common::ObRawType);
    common::ObObjType param_type_3 = types_stack[2].get_type();
    if (ob_is_null(param_type_3) || ob_is_string_type(param_type_3) || ob_is_raw(param_type_3)) {
    } else {
      ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
      LOG_WARN("wrong type of argument in function", K(ret), K(types_stack[2]), K(types_stack[2].get_type()));
    }
  }
  if (OB_SUCC(ret)) {
    type.set_int();
    type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
    type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
    type.set_calc_scale(NUMBER_SCALE_UNKNOWN_YET);
  } else {  // if (OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE == ret)
    ObString func_name("COMPARE");
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  }

  return ret;
}

int ObExprUtlRawCompare::calc_resultN(
    common::ObObj& result, const common::ObObj* objs_stack, int64_t param_num, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t res = 0;
  char default_pad_str = (char)(0);
  const char* tmp_buf = &default_pad_str;
  ObString pad_str(tmp_buf);
  ObString long_str;
  ObString short_str;
  char* buf = NULL;

  if (3 == param_num && !objs_stack[2].is_null()) {
    pad_str.assign_ptr(objs_stack[2].get_string().ptr(), 1);
  }
  if (objs_stack[0].is_null() && objs_stack[1].is_null()) {
    result.set_int(res);
    LOG_DEBUG("succ to calc", K(res), K(result));
  } else {
    if (objs_stack[0].is_null()) {
      long_str = objs_stack[1].get_string();
    } else if (objs_stack[1].is_null()) {
      long_str = objs_stack[0].get_string();
    } else if (objs_stack[0].get_string().length() > objs_stack[1].get_string().length()) {
      long_str = objs_stack[0].get_string();
      short_str = objs_stack[1].get_string();
    } else {
      long_str = objs_stack[1].get_string();
      short_str = objs_stack[0].get_string();
    }
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    if (long_str.length() > OB_MAX_ORACLE_RAW_PL_VAR_LENGTH) {
      ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
      ObString err_msg("raw variable length too long");
      LOG_WARN("raw variable length too long", K(ret), K(long_str.length()));
      LOG_USER_ERROR(OB_ERR_NUMERIC_OR_VALUE_ERROR, err_msg.length(), err_msg.ptr());
    } else if (OB_ISNULL(buf = static_cast<char*>(cast_ctx.allocator_v2_->alloc(long_str.length())))) {
      result.set_null();
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret), K(long_str.length()));
    } else {
      int32_t pad_times = long_str.length() - short_str.length();
      MEMCPY(buf, short_str.ptr(), short_str.length());
      for (int64_t i = 0; i < pad_times; ++i) {
        MEMCPY(buf + short_str.length() + i, pad_str.ptr(), 1);
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < long_str.length(); ++i) {
        if (buf[i] != long_str[i]) {
          res = ++i;
          break;
        }
      }
      result.set_int(res);
      LOG_DEBUG("succ to calc", K(res), K(result));
    }
  }

  return ret;
}

ObExprUtlRawSubstr::ObExprUtlRawSubstr(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_UTL_RAW_SUBSTR, N_UTL_RAW_SUBSTR, TWO_OR_THREE)
{}

ObExprUtlRawSubstr::~ObExprUtlRawSubstr()
{}

int ObExprUtlRawSubstr::calc_result_typeN(
    ObExprResType& type, ObExprResType* types_stack, int64_t param_num, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(2 != param_num && 3 != param_num)) {
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_WARN("substr should have two or three arguments", K(param_num), K(ret));
  } else {  // (2 == param_num) || (3 == param_num)
    types_stack[0].set_calc_type(common::ObRawType);
    types_stack[1].set_calc_type(common::ObNumberType);
    common::ObObjType param_type_1 = types_stack[0].get_type();
    common::ObObjType param_type_2 = types_stack[1].get_type();
    if (((ob_is_string_type(param_type_1) || ob_is_raw(param_type_1)) &&
            (ob_is_null(param_type_2) || ob_is_string_type(param_type_2) || ob_is_numeric_type(param_type_2)))) {
    } else {
      ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
      LOG_WARN("wrong type of argument in function",
          K(ret),
          K(types_stack[0]),
          K(types_stack[0].get_type()),
          K(types_stack[1]),
          K(types_stack[1].get_type()));
    }
  }
  if ((OB_SUCC(ret)) && (3 == param_num)) {
    types_stack[2].set_calc_type(common::ObNumberType);
    common::ObObjType param_type_3 = types_stack[2].get_type();
    if (ob_is_null(param_type_3) || ob_is_string_type(param_type_3) || ob_is_numeric_type(param_type_3)) {
    } else {
      ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
      LOG_WARN("wrong type of argument in function", K(ret), K(types_stack[2]), K(types_stack[2].get_type()));
    }
  }
  if (OB_SUCC(ret)) {
    type.set_collation_level(common::CS_LEVEL_NUMERIC);
    type.set_raw();
    type.set_length(common::OB_MAX_ORACLE_RAW_PL_VAR_LENGTH);
  } else {  // if (OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE == ret)
    ObString func_name("SUBSTR");
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  }

  return ret;
}

int ObExprUtlRawSubstr::calc_resultN(
    common::ObObj& result, const common::ObObj* objs_stack, int64_t param_num, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObString str;
  number::ObNumber pos_nmb;
  number::ObNumber len_nmb;
  int64_t pos = 0;
  int64_t len = -1;
  UNUSED(expr_ctx);

  if (3 == param_num && !objs_stack[2].is_null()) {
    if (OB_FAIL(objs_stack[2].get_number(len_nmb))) {
      LOG_WARN("fail to get number", K(ret));
    } else if (OB_FAIL(len_nmb.extract_valid_int64_with_trunc(len))) {
      LOG_WARN("fail to do trunc", K(ret));
    } else if (len <= 0) {
      ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
      LOG_WARN("length should be greater than zero", K(ret), K(len));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(objs_stack[1].get_number(pos_nmb))) {
      LOG_WARN("fail to get number", K(ret));
    } else if (OB_FAIL(pos_nmb.extract_valid_int64_with_trunc(pos))) {
      LOG_WARN("fail to do trunc", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    str = objs_stack[0].get_string();
    if (OB_UNLIKELY(objs_stack[0].is_null())) {
      ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
      LOG_WARN("type can not be null", K(ret));
    } else if (str.length() > OB_MAX_ORACLE_RAW_PL_VAR_LENGTH) {
      ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
      ObString err_msg("raw variable length too long");
      LOG_WARN("raw variable length too long", K(ret), K(str.length()));
      LOG_USER_ERROR(OB_ERR_NUMERIC_OR_VALUE_ERROR, err_msg.length(), err_msg.ptr());
    } else if (pos > str.length() || pos < -str.length()) {
      ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
      LOG_WARN("pos is overflow", K(ret), K(pos));
    } else if (0 == pos) {
      pos = 1;
    } else if (pos < 0) {
      pos = str.length() + pos + 1;
    }
    if (-1 == len) {
      len = str.length() - pos + 1;
    }
    if (len + (--pos) > str.length()) {
      ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
      LOG_WARN("pos + len is overflow", K(ret), K(pos), K(len), K(str.length()));
    }
  }
  if (OB_SUCC(ret)) {
    result.set_raw(str.ptr() + pos, len);
    LOG_DEBUG("succ to calc", K(result));
  }

  return ret;
}

ObExprUtlRawConcat::ObExprUtlRawConcat(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_UTL_RAW_CONCAT, N_UTL_RAW_CONCAT, PARAM_NUM_UNKNOWN)
{}

ObExprUtlRawConcat::~ObExprUtlRawConcat()
{}

int ObExprUtlRawConcat::calc_result_typeN(
    ObExprResType& type, ObExprResType* types_stack, int64_t param_num, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  int64_t null_type_num = 0;

  if (param_num > 12) {
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_WARN("compare arguments should be less than 13", K(param_num), K(ret));
  } else if (param_num == 0) {
    type.set_null();
  } else {  // param_num > 0 && param_num < 12
    for (int64_t i = 0; OB_SUCC(ret) && (i < param_num); ++i) {
      if (ob_is_null(types_stack[i].get_type())) {
        ++null_type_num;
        types_stack[i].set_calc_type(common::ObRawType);
      } else if (ob_is_string_type(types_stack[i].get_type()) || ob_is_raw(types_stack[i].get_type())) {
        types_stack[i].set_calc_type(common::ObRawType);
      } else {
        ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
        LOG_WARN("wrong type of argument in function", K(param_num), K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (null_type_num == param_num) {
      type.set_null();
    } else {
      type.set_collation_level(common::CS_LEVEL_NUMERIC);
      type.set_raw();
      type.set_length(common::OB_MAX_ORACLE_RAW_PL_VAR_LENGTH);
    }
  } else {  // if (OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE == ret)
    ObString func_name("CONCAT");
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  }

  return ret;
}

int ObExprUtlRawConcat::calc_resultN(
    common::ObObj& result, const common::ObObj* objs_stack, int64_t param_num, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  bool is_res_null = true;
  int32_t pos = 0;
  int32_t res_len = 0;
  char* buf = NULL;

  for (int64_t i = 0; i < param_num; ++i) {
    if (objs_stack[i].is_null()) {
      continue;
    } else {
      res_len += objs_stack[i].get_string().length();
      is_res_null = false;
    }
  }
  if (res_len > OB_MAX_ORACLE_RAW_PL_VAR_LENGTH) {
    ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
    ObString err_msg("raw variable length too long");
    LOG_WARN("raw variable length too long", K(ret), K(res_len));
    LOG_USER_ERROR(OB_ERR_NUMERIC_OR_VALUE_ERROR, err_msg.length(), err_msg.ptr());
  } else if (is_res_null) {
    result.set_null();
    LOG_DEBUG("succ to calc", K(result));
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    if (OB_ISNULL(buf = static_cast<char*>(cast_ctx.allocator_v2_->alloc(res_len)))) {
      result.set_null();
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret), K(res_len));
    } else {
      for (int64_t i = 0; i < param_num; ++i) {
        if (objs_stack[i].is_null()) {
          continue;
        } else {
          MEMCPY(buf + pos, objs_stack[i].get_string().ptr(), objs_stack[i].get_string().length());
          pos += objs_stack[i].get_string().length();
        }
      }
      result.set_raw(buf, res_len);
      LOG_DEBUG("succ to calc", K(result));
    }
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
