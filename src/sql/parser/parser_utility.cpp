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

#include "sql/parser/parser_utility.h"
#include "lib/utility/ob_macro_utils.h"

#include <cstddef>

int parser_to_hex_cstr(
    const void *in_data,
    const int64_t data_length,
    char *buff,
    const int64_t buff_size)
{
  int ret = 0;
  int64_t pos = 0;
  int64_t cstr_pos = 0;
  if (0 != (ret = parser_to_hex_cstr_(in_data, data_length, buff, buff_size, &pos, &cstr_pos))) {
  }
  return ret;
}

static const char *HEXCHARS = "0123456789ABCDEF";

int parser_to_hex_cstr_(
    const void *in_data,
    const int64_t data_length,
    char *buf,
    const int64_t buf_len,
    int64_t *pos,
    int64_t *cstr_pos)
{
  int ret = 0;
  *cstr_pos = *pos;
  if ((OB_UNLIKELY(data_length > 0) && OB_ISNULL(in_data))
      || OB_ISNULL(buf)
      || OB_UNLIKELY(*pos < 0)
      || OB_UNLIKELY(buf_len - *pos) < 1) {
    ret = -1;
    // LOG_WARN("Invalid argument", KP(in_data), KP(buf), K(data_length), K(buf_len), K(ret));
  } else if ((buf_len - *pos) < (data_length * 2 + 1)) {
    buf[*pos++] = '\0';
    ret = -2;
    // LOG_WARN("size is overflow", K(buf_len), K(data_length), K(buf_len), K(pos), K(ret));
  } else {
    unsigned const char *p = (unsigned const char *)in_data;
    char *dst = static_cast<char *>(buf + *pos);
    for (int64_t i = 0; i < data_length; ++i) {
      *dst++ = HEXCHARS[*p >> 4 & 0xF];
      *dst++ = HEXCHARS[*p & 0xF];
      p++;
    }
    *dst = '\0';
    *pos += (data_length * 2 + 1);
  }
  return ret;
}