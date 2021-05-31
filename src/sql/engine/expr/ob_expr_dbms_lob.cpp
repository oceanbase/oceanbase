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
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_dbms_lob.h"
#include "sql/engine/expr/ob_expr_substr.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/parser/ob_item_type.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprDbmsLobGetLength::ObExprDbmsLobGetLength(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_DBMS_LOB_GETLENGTH, N_DBMS_LOB_GETLENGTH, 1, NOT_ROW_DIMENSION)
{}

ObExprDbmsLobGetLength::~ObExprDbmsLobGetLength()
{}

int ObExprDbmsLobGetLength::calc_result_type1(
    ObExprResType& res_type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  common::ObObjType param_type = text.get_type();
  if (ob_is_string_tc(param_type) || ob_is_clob(param_type, text.get_collation_type()) || ob_is_raw(param_type) ||
      ob_is_blob(param_type, text.get_collation_type())) {
    res_type.set_int();
    res_type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
    res_type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
    res_type.set_calc_scale(text.get_scale());
    text.set_calc_type(param_type);
    text.set_calc_collation_type(text.get_collation_type());
  } else {
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_WARN("wrong type of argument in function", K(ret), K(text), K(text.get_type()), K(text.get_collation_type()));
    ObString func_name("GETLENGTH");
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  }

  return ret;
}

int ObExprDbmsLobGetLength::calc_result1(common::ObObj& result, const ObObj& text, ObExprCtx& expr_ctx) const
{
  UNUSED(expr_ctx);
  int ret = OB_SUCCESS;

  if (text.is_null_oracle()) {
    result.set_null();
  } else {
    size_t c_len = ObCharset::strlen_char(
        text.get_collation_type(), text.get_string().ptr(), static_cast<int64_t>(text.get_string().length()));
    result.set_int(static_cast<int64_t>(c_len));
  }

  return ret;
}

ObExprDbmsLobAppend::ObExprDbmsLobAppend(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_DBMS_LOB_APPEND, N_DBMS_LOB_APPEND, 2)
{}

ObExprDbmsLobAppend::~ObExprDbmsLobAppend()
{}

int ObExprDbmsLobAppend::calc_result_type2(
    ObExprResType& res_type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  common::ObObjType param_type_1 = type1.get_type();
  common::ObObjType param_type_2 = type2.get_type();
  if ((ob_is_clob(param_type_1, type1.get_collation_type())) &&
      (ob_is_clob(param_type_2, type2.get_collation_type()) || ob_is_string_tc(param_type_2))) {
    res_type.set_clob();
    res_type.set_collation_type(type1.get_collation_type());
  } else if ((ob_is_blob(param_type_1, type1.get_collation_type())) &&
             (ob_is_blob(param_type_2, type2.get_collation_type()) || ob_is_raw(param_type_2))) {
    res_type.set_blob();
    res_type.set_collation_type(type1.get_collation_type());
  } else {
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    ObString func_name("Append");
    LOG_WARN(
        "wrong type of argument in function", K(ret), K(type1), K(type1.get_type()), K(type2), K(type2.get_type()));
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  }

  return ret;
}

int ObExprDbmsLobAppend::calc_result2(
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

int ObExprDbmsLobAppend::calc(
    common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObCastCtx& cast_ctx)
{
  UNUSED(cast_ctx);
  int ret = OB_SUCCESS;
  char* buf = NULL;
  ObString str_1 = obj1.get_string();
  ObString str_2 = obj2.get_string();
  int32_t res_len = str_1.length() + str_2.length();

  if (!obj1.is_blob() && !obj1.is_clob()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is wrong", K(ret), K(obj1.get_type()));
  } else if (OB_UNLIKELY(obj1.is_null() || obj2.is_null())) {
    // ORA-06502: PL/SQL: numeric or value error: invalid LOB locator specified:
    ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
    LOG_WARN("type can not be null", K(ret));
  } else if (OB_UNLIKELY(res_len > OB_MAX_LONGTEXT_LENGTH)) {
    ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
    ObString err_msg("lob variable length too long");
    LOG_WARN("raw variable length too long", K(ret), K(res_len));
    LOG_USER_ERROR(OB_ERR_NUMERIC_OR_VALUE_ERROR, err_msg.length(), err_msg.ptr());
  } else {
    if (0 == res_len) {
      result.set_lob_value(obj1.get_type(), buf, res_len);
      result.set_collation_type(obj1.get_collation_type());
    } else if (OB_ISNULL(buf = static_cast<char*>(cast_ctx.allocator_v2_->alloc(res_len)))) {
      result.set_null();
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret), K(res_len));
    } else {
      MEMCPY(buf, str_1.ptr(), str_1.length());
      MEMCPY(buf + str_1.length(), str_2.ptr(), str_2.length());
      result.set_lob_value(obj1.get_type(), buf, res_len);
      result.set_collation_type(obj1.get_collation_type());
      LOG_DEBUG("succ to calc", K(obj1), K(obj2), K(result));
    }
  }

  return ret;
}

ObExprDbmsLobRead::ObExprDbmsLobRead(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_DBMS_LOB_READ, N_DBMS_LOB_READ, 3)
{}

ObExprDbmsLobRead::~ObExprDbmsLobRead()
{}

int ObExprDbmsLobRead::calc_result_type3(ObExprResType& res_type, ObExprResType& lob_type, ObExprResType& amount_type,
    ObExprResType& offset_type, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  amount_type.set_calc_type(common::ObIntType);
  offset_type.set_calc_type(common::ObIntType);
  common::ObObjType param_type_1 = lob_type.get_type();
  if (ob_is_clob(param_type_1, lob_type.get_collation_type()) || ob_is_string_tc(param_type_1)) {
    res_type.set_varchar();
    res_type.set_collation_type(lob_type.get_collation_type());
  } else if (ob_is_blob(param_type_1, lob_type.get_collation_type()) || ob_is_raw(param_type_1)) {
    res_type.set_raw();
  } else {
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    ObString func_name("Read");
    LOG_WARN("wrong type of argument in function",
        K(ret),
        K(lob_type),
        K(lob_type.get_type()),
        K(amount_type),
        K(amount_type.get_type()),
        K(offset_type),
        K(offset_type.get_type()));
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  }
  return ret;
}

int ObExprDbmsLobRead::calc_result3(common::ObObj& result, const common::ObObj& lob, const common::ObObj& amount,
    const common::ObObj& offset, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("buffer not init", K(ret));
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    if (OB_FAIL(calc(result, lob, amount, offset, cast_ctx))) {
      LOG_WARN("fail to calc", K(ret), K(lob), K(amount), K(offset));
    } else {
      LOG_DEBUG("succ to calc_result", K(lob), K(amount), K(offset), K(result));
    }
  }

  return ret;
}

int ObExprDbmsLobRead::calc(common::ObObj& result, const common::ObObj& lob, const common::ObObj& amount,
    const common::ObObj& offset, ObCastCtx& cast_ctx)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  ObString lob_str = lob.get_string();
  int32_t res_len = amount.get_int();

  if (OB_ISNULL(lob_str.ptr())) {
    result.set_null();
  } else {
    int64_t lob_len = static_cast<int64_t>(
        ObCharset::strlen_char(lob.get_collation_type(), lob_str.ptr(), static_cast<int64_t>(lob_str.length())));
    if (OB_UNLIKELY(res_len < 1)) {
      ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
      // ORA-21560: argument 2 is null, invalid, or out of range
      ObString err_msg("argument 2 is null, invalid, or out of range");
      LOG_WARN("amount.get_int() < 1", K(ret), K(res_len));
      LOG_USER_ERROR(OB_ERR_NUMERIC_OR_VALUE_ERROR, err_msg.length(), err_msg.ptr());
    } else if (OB_UNLIKELY(offset.get_int() < 1)) {
      ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
      // ORA-21560: argument 3 is null, invalid, or out of range
      ObString err_msg("argument 3 is null, invalid, or out of range");
      LOG_WARN("offset.get_int() < 1", K(ret), K(offset.get_int()));
      LOG_USER_ERROR(OB_ERR_NUMERIC_OR_VALUE_ERROR, err_msg.length(), err_msg.ptr());
    } else if (OB_UNLIKELY(offset.get_int() > lob_len)) {
      ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
      // ORA-01403: no data found
      ObString err_msg("no data found");
      LOG_WARN("offset > lob_len", K(ret), K(offset.get_int()), K(lob_len));
      LOG_USER_ERROR(OB_ERR_NUMERIC_OR_VALUE_ERROR, err_msg.length(), err_msg.ptr());
    } else {
      if (offset.get_int() - 1 + res_len > lob_len) {
        res_len = lob_len - offset.get_int() + 1;
      }
      if (lob.is_blob()) {  // for blob
        if (OB_ISNULL(buf = static_cast<char*>(cast_ctx.allocator_v2_->alloc(amount.get_int())))) {
          result.set_null();
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("alloc memory failed", K(ret), K(amount.get_int()));
        } else {
          MEMCPY(buf, lob_str.ptr() + offset.get_int() - 1, res_len);
          result.set_raw(buf, res_len);
        }
      } else if (lob.is_clob()) {  // for clob
        const ObString& str_val = lob.get_varchar();
        ObCollationType cs_type = lob.get_collation_type();
        if (OB_FAIL(ObExprSubstr::calc(result, str_val, offset.get_int(), res_len, cs_type, false))) {
          LOG_WARN("failed to calc for substr", K(ret), K(str_val), K(offset.get_int()), K(res_len), K(cs_type));
        } else {
          result.set_collation_type(lob.get_collation_type());
        }
      }
    }
  }

  return ret;
}

ObExprDbmsLobConvertToBlob::ObExprDbmsLobConvertToBlob(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_DBMS_LOB_CONVERTTOBLOB, N_DBMS_LOB_CONVERTTOBLOB, 3)
{}

ObExprDbmsLobConvertToBlob::~ObExprDbmsLobConvertToBlob()
{}

int ObExprDbmsLobConvertToBlob::calc_result_type3(ObExprResType& res_type, ObExprResType& blob_type,
    ObExprResType& clob_type, ObExprResType& blob_offset_type, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  blob_offset_type.set_calc_type(common::ObIntType);
  if (!ob_is_blob(blob_type.get_type(), blob_type.get_collation_type()) ||
      (!ob_is_clob(clob_type.get_type(), clob_type.get_collation_type()) && !ob_is_string_tc(clob_type.get_type()))) {
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    ObString func_name("CONVERTTOBLOB");
    LOG_WARN("wrong type of argument in function",
        K(ret),
        K(blob_type.get_type()),
        K(blob_type.get_collation_type()),
        K(clob_type.get_type()),
        K(clob_type.get_collation_type()));
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  } else {
    res_type.set_blob();
    res_type.set_collation_type(CS_TYPE_BINARY);
  }

  return ret;
}

int ObExprDbmsLobConvertToBlob::calc_result3(common::ObObj& result, const common::ObObj& blob,
    const common::ObObj& clob, const common::ObObj& blob_offset, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("buffer not init", K(ret));
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    if (OB_FAIL(calc(result, blob, clob, blob_offset, cast_ctx))) {
      LOG_WARN("fail to calc", K(ret));
    } else {
      LOG_DEBUG("succ to calc_result", K(result));
    }
  }

  return ret;
}

int ObExprDbmsLobConvertToBlob::calc(common::ObObj& result, const common::ObObj& blob, const common::ObObj& clob,
    const common::ObObj& blob_offset, common::ObCastCtx& cast_ctx)
{
  UNUSED(cast_ctx);
  int ret = OB_SUCCESS;
  char* res_blob_buf = NULL;
  int64_t res_len = 0;
  int64_t dest_offset = blob_offset.get_int();
  const ObString blob_str = blob.get_string();
  const ObString clob_str = clob.get_string();

  if (OB_ISNULL(blob_str.ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("blob_str.ptr is null", K(ret));
  } else if (dest_offset < 1) {
    ObString err_msg("argument 4 is null, invalid, or out of range");
    LOG_WARN("dest_offset < 1", K(ret), K(dest_offset));
    LOG_USER_ERROR(OB_ERR_NUMERIC_OR_VALUE_ERROR, err_msg.length(), err_msg.ptr());
  } else if (OB_ISNULL(clob_str.ptr())) {
    result.set_lob_value(blob.get_type(), blob_str.ptr(), blob_str.length());
    result.set_collation_type(CS_TYPE_BINARY);
  } else if (dest_offset - 1 + clob_str.length() <= blob_str.length()) {
    res_len = blob_str.length();
  } else {
    res_len = dest_offset - 1 + clob_str.length();
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(clob_str.ptr())) {
    if (res_len > OB_MAX_LONGTEXT_LENGTH) {
      ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
      ObString err_msg("clob variable length too long");
      LOG_WARN("clob variable length too long", K(ret), K(res_len));
      LOG_USER_ERROR(OB_ERR_NUMERIC_OR_VALUE_ERROR, err_msg.length(), err_msg.ptr());
    } else if (OB_ISNULL(cast_ctx.allocator_v2_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("allocator in cast ctx is NULL", K(ret));
    } else if (OB_ISNULL(res_blob_buf = static_cast<char*>(cast_ctx.allocator_v2_->alloc(res_len)))) {
      result.set_null();
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret), K(res_len));
    } else {
      MEMSET(res_blob_buf, 0, res_len);
      MEMCPY(res_blob_buf, blob_str.ptr(), blob_str.length());
      MEMCPY(res_blob_buf + dest_offset - 1, clob_str.ptr(), clob_str.length());
      result.set_lob_value(blob.get_type(), res_blob_buf, res_len);
      result.set_collation_type(CS_TYPE_BINARY);
      LOG_DEBUG("succ to calc", K(ret), K(result));
    }
  }

  return ret;
}

ObExprDbmsLobCastClobToBlob::ObExprDbmsLobCastClobToBlob(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_DBMS_LOB_CAST_CLOB_TO_BLOB, N_DBMS_LOB_CAST_CLOB_TO_BLOB, 1)
{}

ObExprDbmsLobCastClobToBlob::~ObExprDbmsLobCastClobToBlob()
{}

int ObExprDbmsLobCastClobToBlob::calc_result_type1(
    ObExprResType& res_type, ObExprResType& clob_type, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  if (!ob_is_clob(clob_type.get_type(), clob_type.get_collation_type()) && !ob_is_string_tc(clob_type.get_type())) {
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    ObString func_name("CAST_CLOB_TO_BLOB");
    LOG_WARN("wrong type of argument in function", K(ret), K(clob_type.get_type()), K(clob_type.get_collation_type()));
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  } else {
    res_type.set_blob();
    res_type.set_collation_type(CS_TYPE_BINARY);
  }

  return ret;
}

int ObExprDbmsLobCastClobToBlob::calc_result1(
    common::ObObj& result, const common::ObObj& clob, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("buffer not init", K(ret));
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    if (OB_FAIL(calc(result, clob, cast_ctx))) {
      LOG_WARN("fail to calc", K(ret));
    } else {
      LOG_DEBUG("succ to calc_result", K(result));
    }
  }

  return ret;
}

int ObExprDbmsLobCastClobToBlob::calc(common::ObObj& result, const common::ObObj& clob, common::ObCastCtx& cast_ctx)
{
  UNUSED(cast_ctx);
  int ret = OB_SUCCESS;
  char* res_blob_buf = NULL;
  const ObString clob_str = clob.get_string();

  if (OB_ISNULL(clob_str.ptr())) {
    result.set_lob_value(ObLongTextType, res_blob_buf, clob_str.length());
    result.set_collation_type(CS_TYPE_BINARY);
  } else if (clob_str.length() > OB_MAX_LONGTEXT_LENGTH) {
    ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
    ObString err_msg("clob variable length too long");
    LOG_WARN("clob variable length too long", K(ret), K(clob_str.length()));
    LOG_USER_ERROR(OB_ERR_NUMERIC_OR_VALUE_ERROR, err_msg.length(), err_msg.ptr());
  } else if (OB_ISNULL(cast_ctx.allocator_v2_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator in cast ctx is NULL", K(ret));
  } else if (OB_ISNULL(res_blob_buf = static_cast<char*>(cast_ctx.allocator_v2_->alloc(clob_str.length())))) {
    result.set_null();
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", K(ret), K(clob_str.length()));
  } else {
    MEMCPY(res_blob_buf, clob_str.ptr(), clob_str.length());
    result.set_lob_value(ObLongTextType, res_blob_buf, clob_str.length());
    result.set_collation_type(CS_TYPE_BINARY);
    LOG_DEBUG("succ to calc", K(ret), K(result));
  }

  return ret;
}

ObExprDbmsLobConvertClobCharset::ObExprDbmsLobConvertClobCharset(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_DBMS_LOB_CONVERT_CLOB_CHARSET, N_DBMS_LOB_CONVERT_CLOB_CHARSET, 2)
{}

ObExprDbmsLobConvertClobCharset::~ObExprDbmsLobConvertClobCharset()
{}

int ObExprDbmsLobConvertClobCharset::calc_result_type2(
    ObExprResType& res_type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  int64_t nls_charset_id = 0;

  common::ObObjType param_type_1 = type1.get_type();
  common::ObObjType param_type_2 = type2.get_type();
  type2.set_calc_type(common::ObNumberType);
  if (OB_FAIL(type2.get_param().get_number().extract_valid_int64_with_trunc(nls_charset_id))) {
    LOG_WARN("get_trunc_int64 failed", K(ret), K(type2.get_param().get_number()), K(nls_charset_id));
  } else if ((ob_is_clob(param_type_1, type1.get_collation_type()) || ob_is_string_tc(param_type_1)) &&
             ob_is_number_tc(param_type_2)) {
    if (ob_is_clob(param_type_1, type1.get_collation_type())) {
      res_type.set_clob();
    } else {  // ob_is_string_tc(param_type_1)
      res_type.set_varchar();
    }
    switch (nls_charset_id) {
      case ObNlsCharsetId::CHARSET_AL32UTF8_ID:
        res_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported character set", K(ret), K(nls_charset_id));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "character set");
    }
  } else {
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    ObString func_name("ConvertClobCharset");
    LOG_WARN(
        "wrong type of argument in function", K(ret), K(type1), K(type1.get_type()), K(type2), K(type2.get_type()));
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  }

  return ret;
}

int ObExprDbmsLobConvertClobCharset::calc_result2(
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

int ObExprDbmsLobConvertClobCharset::calc(
    common::ObObj& result, const common::ObObj& clob, const common::ObObj& charset_id, common::ObCastCtx& cast_ctx)
{
  int ret = OB_SUCCESS;

  ObString src_clob_str = clob.get_string();
  int64_t nls_charset_id = 0;
  const int32_t CharConvertFactorNum = 2;
  char* buf = NULL;
  int32_t buf_len = src_clob_str.length() * CharConvertFactorNum;
  uint32_t res_len = 0;
  ObCollationType dest_collation = CS_TYPE_INVALID;

  if (OB_UNLIKELY(clob.is_null() || charset_id.is_null())) {
    // ORA-06502: PL/SQL: numeric or value error: invalid LOB locator specified:
    ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
    LOG_WARN("type can not be null", K(ret));
  } else if (OB_ISNULL(src_clob_str.ptr())) {
    if (clob.is_clob()) {
      result.set_lob_value(ObLongTextType, buf, static_cast<int32_t>(res_len));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("str.ptr() is null", K(ret), K(src_clob_str.ptr()));
    }
  } else if (OB_FAIL(charset_id.get_number().extract_valid_int64_with_trunc(nls_charset_id))) {
    LOG_WARN("get_trunc_int64 failed", K(ret), K(charset_id.get_number()), K(nls_charset_id));
  } else {
    switch (nls_charset_id) {
      case ObNlsCharsetId::CHARSET_AL32UTF8_ID:
        dest_collation = CS_TYPE_UTF8MB4_BIN;
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported character set", K(ret), K(nls_charset_id));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "character set");
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(buf = static_cast<char*>(cast_ctx.allocator_v2_->alloc(buf_len)))) {
        result.set_null();
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(ret), K(buf_len));
      } else if (OB_FAIL(ObCharset::charset_convert(clob.get_collation_type(),
                     src_clob_str.ptr(),
                     src_clob_str.length(),
                     dest_collation,
                     buf,
                     buf_len,
                     res_len))) {
        LOG_WARN("charset convert failed", K(ret), K(clob.get_collation_type()), K(dest_collation));
      } else {
        if (clob.is_clob()) {
          result.set_lob_value(ObLongTextType, buf, static_cast<int32_t>(res_len));
        } else {
          result.set_varchar(buf, static_cast<int32_t>(res_len));
        }
        result.set_collation_type(dest_collation);
        LOG_DEBUG("succ to calc", K(clob), K(charset_id), K(result));
      }
    }
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
