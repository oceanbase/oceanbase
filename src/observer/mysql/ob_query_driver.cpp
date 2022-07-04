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

#define USING_LOG_PREFIX SERVER

#include "ob_query_driver.h"
#include "ob_mysql_result_set.h"
#include "obmp_base.h"
#include "rpc/obmysql/packet/ompk_resheader.h"
#include "rpc/obmysql/packet/ompk_field.h"
#include "rpc/obmysql/packet/ompk_eof.h"
#include <string.h>

namespace oceanbase {
using namespace common;
using namespace sql;
using namespace obmysql;
namespace observer {

int ObQueryDriver::response_query_header(ObResultSet& result, bool has_nore_result, bool need_set_ps_out_flag)
{
  int ret = OB_SUCCESS;
  bool ac = true;
  if (result.get_field_cnt() <= 0) {
    LOG_WARN("result set column", K(result.get_field_cnt()));
    ret = OB_ERR_BAD_FIELD_ERROR;
  } else if (OB_FAIL(session_.get_autocommit(ac))) {
    LOG_WARN("fail to get autocommit", K(ret));
  }
  if (OB_SUCC(ret) && !result.get_is_com_filed_list()) {
    OMPKResheader rhp;
    rhp.set_field_count(result.get_field_cnt());
    if (OB_FAIL(sender_.response_packet(rhp))) {
      LOG_WARN("response packet fail", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    const ColumnsFieldIArray* fields = result.get_field_columns();
    if (OB_ISNULL(fields)) {
      ret = OB_INVALID_ARGUMENT;
      result.set_errcode(ret);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < result.get_field_cnt(); ++i) {
      bool is_match = true;
      ObMySQLField field;
      const ObField& ob_field = result.get_field_columns()->at(i);
      if (result.get_is_com_filed_list() && !ObCharset::is_valid_collation(ob_field.charsetnr_)) {
        ret = OB_ERR_COLLATION_MISMATCH;
        LOG_WARN("invalid charset", K(result.get_session().get_current_query_string()));
      } else if (result.get_is_com_filed_list() && !result.get_wildcard_string().empty() &&
                 OB_FAIL(like_match(ob_field.org_cname_.ptr(),
                     ob_field.org_cname_.length(),
                     0,
                     result.get_wildcard_string().ptr(),
                     result.get_wildcard_string().length(),
                     0,
                     is_match))) {
        LOG_WARN("failed to like match", K(ret));
      } else if (is_match) {
        ret = ObMySQLResultSet::to_mysql_field(ob_field, field);
        result.replace_lob_type(result.get_session(), ob_field, field);
        if (result.get_is_com_filed_list()) {
          field.default_value_ = static_cast<EMySQLFieldType>(ob_field.default_value_.get_ext());
        }
        result.set_errcode(ret);
        if (OB_SUCC(ret)) {
          OMPKField fp(field);
          if (OB_FAIL(sender_.response_packet(fp))) {
            LOG_WARN("response packet fail", K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    OMPKEOF eofp;
    eofp.set_warning_count(0);
    ObServerStatusFlags flags = eofp.get_server_status();
    flags.status_flags_.OB_SERVER_STATUS_IN_TRANS = (session_.is_server_status_in_transaction() ? 1 : 0);
    flags.status_flags_.OB_SERVER_STATUS_AUTOCOMMIT = (ac ? 1 : 0);
    flags.status_flags_.OB_SERVER_MORE_RESULTS_EXISTS = has_nore_result;
    flags.status_flags_.OB_SERVER_PS_OUT_PARAMS = need_set_ps_out_flag ? 1 : 0;
    if (!session_.is_obproxy_mode()) {
      // in java client or others, use slow query bit to indicate partition hit or not
      flags.status_flags_.OB_SERVER_QUERY_WAS_SLOW = !session_.partition_hit().get_bool();
    }
    eofp.set_server_status(flags);

    if (OB_FAIL(sender_.response_packet(eofp))) {
      LOG_WARN("response packet fail", K(ret));
    }
  }
  return ret;
}

int ObQueryDriver::convert_field_charset(ObIAllocator& allocator, const ObCollationType& from_collation,
    const ObCollationType& dest_collation, const ObString& from_string, ObString& dest_string)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int32_t buf_len = from_string.length() * 4;
  uint32_t result_len = 0;
  if (0 == buf_len) {
  } else if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(allocator.alloc(buf_len))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", K(ret));
  } else if (OB_FAIL(ObCharset::charset_convert(static_cast<ObCollationType>(from_collation),
                 from_string.ptr(),
                 from_string.length(),
                 dest_collation,
                 buf,
                 buf_len,
                 result_len))) {
    LOG_WARN("charset convert failed", K(ret), K(from_collation), K(dest_collation));
  } else {
    dest_string.assign(buf, result_len);
  }
  return ret;
}

int ObQueryDriver::convert_string_charset(const ObString& in_str, const ObCollationType in_cs_type,
    const ObCollationType out_cs_type, char* buf, int32_t buf_len, uint32_t& result_len)
{
  int ret = OB_SUCCESS;
  ret = ObCharset::charset_convert(in_cs_type, in_str.ptr(), in_str.length(), out_cs_type, buf, buf_len, result_len);
  if (OB_SUCCESS != ret) {
    int32_t str_offset = 0;
    int64_t buf_offset = 0;
    ObString question_mark = ObCharsetUtils::get_const_str(out_cs_type, '?');
    while (str_offset < in_str.length() && buf_offset + question_mark.length() <= buf_len) {
      int64_t offset = ObCharset::charpos(in_cs_type, in_str.ptr() + str_offset, in_str.length() - str_offset, 1);
      ret = ObCharset::charset_convert(in_cs_type,
          in_str.ptr() + str_offset,
          offset,
          out_cs_type,
          buf + buf_offset,
          buf_len - buf_offset,
          result_len);
      str_offset += offset;
      if (OB_SUCCESS == ret) {
        buf_offset += result_len;
      } else {
        MEMCPY(buf + buf_offset, question_mark.ptr(), question_mark.length());
        buf_offset += question_mark.length();
      }
    }
    if (str_offset < in_str.length()) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("sizeoverflow", K(ret), K(in_str), KPHEX(in_str.ptr(), in_str.length()));
    } else {
      result_len = buf_offset;
      ret = OB_SUCCESS;
      LOG_WARN("charset convert failed", K(ret), K(in_cs_type), K(out_cs_type));
    }
  }
  return ret;
}

int ObQueryDriver::convert_string_value_charset(ObObj& value, ObResultSet& result)
{
  int ret = OB_SUCCESS;
  ObCharsetType charset_type = CHARSET_INVALID;
  const ObSQLSessionInfo &my_session = result.get_session();
  ObArenaAllocator *allocator = NULL;
  if (OB_FAIL(result.get_exec_context().get_convert_charset_allocator(allocator))) {
    LOG_WARN("fail to get lob fake allocator", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob fake allocator is null.", K(ret), K(value));
  } else if (OB_FAIL(my_session.get_character_set_results(charset_type))) {
    LOG_WARN("fail to get result charset", K(ret));
  } else {
    OZ (value.convert_string_value_charset(charset_type, *allocator));
  }
  return ret;
}

int ObQueryDriver::convert_string_value_charset(ObObj& value, ObCharsetType charset_type, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  ObString str;
  value.get_string(str);
  if (ObCharset::is_valid_charset(charset_type) && CHARSET_BINARY != charset_type) {
    ObCollationType collation_type = ObCharset::get_default_collation(charset_type);
    const ObCharsetInfo* from_charset_info = ObCharset::get_charset(value.get_collation_type());
    const ObCharsetInfo* to_charset_info = ObCharset::get_charset(collation_type);
    if (OB_ISNULL(from_charset_info) || OB_ISNULL(to_charset_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("charsetinfo is null", K(ret), K(value.get_collation_type()), K(collation_type));
    } else if (CS_TYPE_INVALID == value.get_collation_type() || CS_TYPE_INVALID == collation_type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid collation", K(value.get_collation_type()), K(collation_type), K(ret));
    } else if (CS_TYPE_BINARY != value.get_collation_type() && CS_TYPE_BINARY != collation_type &&
               strcmp(from_charset_info->csname, to_charset_info->csname) != 0) {
      char* buf = NULL;
      int32_t buf_len = str.length() * 4;
      uint32_t result_len = 0;
      if (0 == buf_len) {
        // do noting
      } else if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(allocator.alloc(buf_len))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(ret), K(buf_len));
      } else if (OB_FAIL(convert_string_charset(
                     str, value.get_collation_type(), collation_type, buf, buf_len, result_len))) {
        LOG_WARN("convert string charset failed", K(ret));
      } else {
        value.set_string(value.get_type(), buf, static_cast<int32_t>(result_len));
        value.set_collation_type(collation_type);
      }
    }
  }
  return ret;
}

int ObQueryDriver::convert_lob_value_charset(common::ObObj& value, sql::ObResultSet& result)
{
  int ret = OB_SUCCESS;

  ObString str;
  ObLobLocator* lob_locator = NULL;
  ObCharsetType charset_type = CHARSET_INVALID;
  const ObSQLSessionInfo& my_session = result.get_session();
  if (OB_FAIL(value.get_lob_locator(lob_locator))) {
    LOG_WARN("get lob locator failed", K(ret));
  } else if (OB_ISNULL(lob_locator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null lob locator", K(ret));
  } else if (OB_FAIL(lob_locator->get_payload(str))) {
    LOG_WARN("get lob locator payload failed", K(ret));
  } else if (OB_FAIL(my_session.get_character_set_results(charset_type))) {
    LOG_WARN("fail to get result charset", K(ret));
  } else if (ObCharset::is_valid_charset(charset_type) && CHARSET_BINARY != charset_type) {
    ObCollationType collation_type = ObCharset::get_default_collation(charset_type);
    const ObCharsetInfo* from_charset_info = ObCharset::get_charset(value.get_collation_type());
    const ObCharsetInfo* to_charset_info = ObCharset::get_charset(collation_type);
    if (OB_ISNULL(from_charset_info) || OB_ISNULL(to_charset_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("charsetinfo is null", K(ret), K(value.get_collation_type()), K(collation_type));
    } else if (CS_TYPE_INVALID == value.get_collation_type() || CS_TYPE_INVALID == collation_type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid collation", K(value.get_collation_type()), K(collation_type), K(ret));
    } else if (CS_TYPE_BINARY != value.get_collation_type() && CS_TYPE_BINARY != collation_type &&
               strcmp(from_charset_info->csname, to_charset_info->csname) != 0) {
      char* buf = NULL;
      int32_t header_len = value.get_val_len() - lob_locator->payload_size_;
      int32_t str_len = lob_locator->payload_size_ * 4;
      uint32_t result_len = 0;
      ObArenaAllocator *allocator = NULL;
      if (OB_FAIL(result.get_exec_context().get_convert_charset_allocator(allocator))) {
        LOG_WARN("fail to get lob fake allocator", K(ret));
      } else if (OB_ISNULL(allocator)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("lob fake allocator is null.", K(ret), K(value));
      } else if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(allocator->alloc(header_len + str_len))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(ret), K(header_len), K(str_len));
      } else {
        MEMCPY(buf, lob_locator, header_len);
        if (OB_FAIL(convert_string_charset(
                str, value.get_collation_type(), collation_type, buf + header_len, str_len, result_len))) {
          LOG_WARN("convert string charset failed", K(ret));
        } else {
          ObLobLocator* result_lob = reinterpret_cast<ObLobLocator*>(buf);
          result_lob->payload_size_ = result_len;
          value.set_lob_locator(*result_lob);
          value.set_collation_type(collation_type);
        }
      }
    }
  }
  return ret;
}

int ObQueryDriver::like_match(
    const char* str, int64_t length_str, int64_t i, const char* pattern, int64_t length_pat, int64_t j, bool& is_match)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(str) || OB_ISNULL(pattern) || OB_UNLIKELY(length_str < 0 || i < 0 || length_pat < 0 || j < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(str), K(length_str), K(i), K(pattern), K(length_pat), K(j));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (i == length_str && j == length_pat) {
    is_match = true;
  } else if (i != length_str && j >= length_pat) {
    is_match = false;
  } else if (j < length_pat && pattern[j] == '%') {
    ++j;
    if (OB_FAIL(like_match(str, length_str, i, pattern, length_pat, j, is_match))) {
      LOG_WARN("failed to match", K(ret));
    } else if (!is_match && i < length_str) {
      ++i;
      --j;
      if (OB_FAIL(like_match(str, length_str, i, pattern, length_pat, j, is_match))) {
        LOG_WARN("failed to match", K(ret));
      }
    }
  } else if (i < length_str && j < length_pat && pattern[j] == '_') {
    ++i;
    ++j;
    if (OB_FAIL(like_match(str, length_str, i, pattern, length_pat, j, is_match))) {
      LOG_WARN("failed to match", K(ret));
    }
  } else if (i < length_str && j < length_pat && tolower(str[i]) == tolower(pattern[j])) {
    ++i;
    ++j;
    if (OB_FAIL(like_match(str, length_str, i, pattern, length_pat, j, is_match))) {
      LOG_WARN("failed to match", K(ret));
    }
  } else {
    is_match = false;
  }
  return ret;
}

int ObQueryDriver::convert_lob_locator_to_longtext(ObObj& value, ObResultSet& result)
{
  int ret = OB_SUCCESS;
  // if the client uses a new lob locator, return the lob locator data
  // if the client uses the old lob (no locator header, only data), the old lob is returned
  if (lib::is_oracle_mode()) {
    bool is_use_lob_locator = session_.is_client_use_lob_locator();
    if (is_use_lob_locator && value.is_lob()) {
      ObString str;
      char* buf = nullptr;
      ObLobLocator* lob_locator = nullptr;
      ObArenaAllocator* allocator = NULL;
      if (OB_FAIL(value.get_string(str))) {
        STORAGE_LOG(WARN, "Failed to get string from obj", K(ret), K(value));
      } else if (OB_FAIL(result.get_exec_context().get_convert_charset_allocator(allocator))) {
        LOG_WARN("fail to get lob fake allocator", K(ret));
      } else if (FALSE_IT(allocator->reset())) {
      } else if (OB_ISNULL(buf = static_cast<char*>(allocator->alloc(str.length() + sizeof(ObLobLocator))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate memory for lob locator", K(ret), K(str));
      } else if (FALSE_IT(lob_locator = reinterpret_cast<ObLobLocator*>(buf))) {
      } else if (OB_FAIL(lob_locator->init(str))) {
        STORAGE_LOG(WARN, "Failed to init lob locator", K(ret), K(str), KPC(lob_locator));
      } else {
        value.set_lob_locator(*lob_locator);
        LOG_TRACE("return lob locator", K(*lob_locator), K(str));
      }
    } else if (!is_use_lob_locator && value.is_lob_locator()) {
      ObString payload;
      if (OB_ISNULL(value.get_lob_locator())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(value));
      } else if (OB_FAIL(value.get_lob_locator()->get_payload(payload))) {
        LOG_WARN("fail to get payload", K(ret));
      } else {
        value.set_lob_value(ObLongTextType, payload.ptr(), static_cast<int32_t>(payload.length()));
      }
    }
    LOG_TRACE("return data", K(is_use_lob_locator), K(value), K(lbt()));
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
