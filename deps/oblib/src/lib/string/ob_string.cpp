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

#include "lib/string/ob_string.h"
#include "lib/utility/serialization.h"
#include "common/data_buffer.h"

using namespace oceanbase;
using namespace common;

int ObString::clone(const char *rv, const int32_t len, ObDataBuffer &buf, bool add_separator)
{
  int ret = OB_SUCCESS;
  if (len > buf.get_remain()) {
    ret = OB_BUF_NOT_ENOUGH;
    LIB_LOG(WARN, "buffer not enough", K(ret), K(len), "remain", buf.get_remain());
  } else {
    assign_buffer(buf.get_cur_pos(), static_cast<obstr_size_t>(len));
    if (len > 0) {
      const obstr_size_t writed_length = write(rv, len);
      if (writed_length == len) {
        buf.get_position() += writed_length;
      } else {
        ret = OB_ERROR;
        LIB_LOG(WARN, "write string failed", K(ret), K(writed_length), K(len));
      }
    }
    if (OB_SUCC(ret) && add_separator) {
      if (buf.get_remain() > 0) {
        // temporary use this to separate string
        *(buf.get_cur_pos()) = '\0';
        buf.get_position() ++;
      } else {
        ret = OB_BUF_NOT_ENOUGH;
        LIB_LOG(WARN, "buffer not enough", K(ret), "remain", buf.get_remain());
      }
    }
  }
  return ret;
}

int ObString::clone(const ObString &rv, ObDataBuffer &buf)
{
  int ret = OB_SUCCESS;
  if (rv.length() > buf.get_remain()) {
    ret = OB_BUF_NOT_ENOUGH;
    LIB_LOG(WARN, "buffer not enough", K(ret), "need", rv.length(), "remain", buf.get_remain());
  } else {
    assign_buffer(buf.get_data() + buf.get_position(), static_cast<obstr_size_t>(buf.get_remain()));
    const obstr_size_t writed_length = write(rv.ptr(), rv.length());
    if (writed_length == rv.length()) {
      buf.get_position() += writed_length;
    } else {
      ret = OB_ERROR;
      LIB_LOG(WARN, "write string failed", K(ret), K(writed_length), "size", rv.length());
    }
  }
  return ret;
}

DEFINE_SERIALIZE(ObString)
{
  int ret = OB_SUCCESS;
  const int64_t serialize_size = get_serialize_size();
  //Null ObString is allowed
  if (OB_ISNULL(buf) || OB_UNLIKELY(serialize_size > buf_len - pos)) {
    ret = OB_SIZE_OVERFLOW;
    LIB_LOG(WARN, "size overflow", K(ret),
        KP(buf), K(serialize_size), "remain", buf_len - pos);
  } else if (OB_FAIL(serialization::encode_vstr(buf, buf_len, pos, ptr_, data_length_))) {
    LIB_LOG(WARN, "string serialize failed", K(ret));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObString)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  const int64_t MINIMAL_NEEDED_SIZE = 2; //at least need two bytes
  if (OB_ISNULL(buf) || OB_UNLIKELY((data_len - pos) < MINIMAL_NEEDED_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument", K(ret), KP(buf), "remain", data_len - pos);
  } else {
    if (0 == buffer_size_) {
      ptr_ = const_cast<char *>(serialization::decode_vstr(buf, data_len, pos, &len));
      if (OB_ISNULL(ptr_)) {
        ret = OB_ERROR;
        LIB_LOG(WARN, "decode NULL string", K(ret));
      }
    } else {
      //copy to ptr_
      const int64_t str_len = serialization::decoded_length_vstr(buf, data_len, pos);
      if (str_len < 0 || buffer_size_ < str_len || (data_len - pos) < str_len) {
        ret = OB_BUF_NOT_ENOUGH;
        LIB_LOG(WARN, "string buffer not enough",
            K(ret), K_(buffer_size), K(str_len), "remain", data_len - pos);
      } else if (NULL == serialization::decode_vstr(buf, data_len, pos, ptr_, buffer_size_, &len)) {
        ret = OB_ERROR;
        LIB_LOG(WARN, "fail to decode_vstr", K(str_len), K(pos), K(data_len), K(buffer_size_));
      }
    }
    if (OB_SUCC(ret)) {
      data_length_ = static_cast<obstr_size_t>(len);
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObString)
{
  return serialization::encoded_length_vstr(data_length_);
}
