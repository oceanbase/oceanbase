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

#define USING_LOG_PREFIX LIB

#include "lib/string/ob_hex_utils_base.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{

int ObHexUtilsBase::unhex(const ObString &text, ObIAllocator &alloc, ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString str_result;
  char *buf = NULL;
  const bool need_fill_zero = (1 == text.length() % 2);
  const int32_t tmp_length = text.length() / 2 + need_fill_zero;
  int32_t alloc_length = (0 == tmp_length ? 1 : tmp_length);
/*
  if (OB_ISNULL(cast_ctx.allocator_v2_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator in cast ctx is NULL", K(ret), K(text));
  } else
*/
  if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(alloc_length)))) {
    result.set_null();
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", K(alloc_length), K(ret));
  } else {
    bool all_valid_char = true;
    int32_t i = 0;
    char c1 = 0;
    char c2 = 0;
    if (text.length() > 0) {
      if (need_fill_zero) {
        c1 = '0';
        c2 = text[0];
        i = 0;
      } else {
        c1 = text[0];
        c2 = text[1];
        i = 1;
      }
    }
    while (OB_SUCC(ret) && all_valid_char && i < text.length()) {
      if (isxdigit(c1) && isxdigit(c2)) {
        buf[i / 2] = (char)((get_xdigit(c1) << 4) | get_xdigit(c2));
        if (i + 2 < text.length()) {
          c1 = text[++i];
          c2 = text[++i];
        }  else {
          break;
        }
      } else if (lib::is_oracle_mode()) {
        ret = OB_ERR_INVALID_HEX_NUMBER;
        LOG_WARN("invalid hex number", K(ret), K(c1), K(c2), K(text));
      } else {
        all_valid_char = false;
        result.set_null();
      }
    }

    if (OB_SUCC(ret) && all_valid_char) {
      str_result.assign_ptr(buf, tmp_length);
      result.set_varchar(str_result);
    }
  }
  return ret;
}

int ObHexUtilsBase::hex(const ObString &text, ObIAllocator &alloc, ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString str_result;
  char *buf = NULL;
  const int32_t alloc_length = text.empty() ? 1 : text.length() * 2;
/*
  if (OB_ISNULL(cast_ctx.allocator_v2_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator in cast ctx is NULL", K(ret), K(text));
  } else
*/
  if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(alloc_length)))) {
    result.set_null();
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", K(ret), K(alloc_length));
  } else {
    static const char *HEXCHARS = "0123456789ABCDEF";
    int32_t pos = 0;
    for (int32_t i = 0; i < text.length(); ++i) {
      buf[pos++] = HEXCHARS[text[i] >> 4 & 0xF];
      buf[pos++] = HEXCHARS[text[i] & 0xF];
    }
    str_result.assign_ptr(buf, pos);
    result.set_varchar(str_result);
    LOG_DEBUG("succ to hex", K(text), "length", text.length(), K(str_result));
  }
  return ret;
}

int ObHexUtilsBase::hex(ObString &text, ObIAllocator &alloc, const char *binary_buf, int64_t binary_len)
{
  int ret = OB_SUCCESS;
  char *text_buf = NULL;
  int64_t text_len = binary_len << 1;
  if (OB_ISNULL(binary_buf) || binary_len < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_ISNULL(text_buf = static_cast<char *>(alloc.alloc(text_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", K(ret), K(text_len));
  } else {
    static const char *HEXCHARS = "0123456789ABCDEF";
    int64_t pos = 0;
    for (int64_t i = 0; i < binary_len; ++i) {
      text_buf[pos++] = HEXCHARS[binary_buf[i] >> 4 & 0xF];
      text_buf[pos++] = HEXCHARS[binary_buf[i] & 0xF];
    }
    text.assign_ptr(text_buf, pos);
    LOG_DEBUG("succ to hex", K(text.length()), K(text));
  }
  return ret;
}

int ObHexUtilsBase::unhex(const ObString &text, ObIAllocator &alloc, char *&binary_buf, int64_t &binary_len)
{
  int ret = OB_SUCCESS;
  char *bin_buf = NULL;
  const bool need_fill_zero = (1 == text.length() % 2);
  const int32_t bin_len = text.length() / 2 + need_fill_zero;
  if (OB_ISNULL(bin_buf = static_cast<char *>(alloc.alloc(bin_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", K(bin_len), K(ret));
  } else {
    bool all_valid_char = true;
    int64_t i = 0;
    char c1 = 0;
    char c2 = 0;
    if (text.length() > 0) {
      if (need_fill_zero) {
        c1 = '0';
        c2 = text[0];
        i = 0;
      } else {
        c1 = text[0];
        c2 = text[1];
        i = 1;
      }
    }
    while (OB_SUCC(ret) && all_valid_char && i < text.length()) {
      if (isxdigit(c1) && isxdigit(c2)) {
        bin_buf[i / 2] = (char)((get_xdigit(c1) << 4) | get_xdigit(c2));
        if (i + 2 < text.length()) {
          c1 = text[++i];
          c2 = text[++i];
        } else {
          break;
        }
      } else if (lib::is_oracle_mode()) {
        ret = OB_ERR_INVALID_HEX_NUMBER;
        LOG_WARN("invalid hex number", K(ret), K(c1), K(c2), K(text));
      } else {
        all_valid_char = false;
      }
    }

    if (OB_SUCC(ret) && all_valid_char) {
      binary_buf = bin_buf;
      binary_len = bin_len;
    }
  }
  return ret;
}

} // end namespace common
} // end namespace oceanbase

