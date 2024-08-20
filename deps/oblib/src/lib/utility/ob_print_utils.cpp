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
#include "lib/utility/ob_print_utils.h"
#include "lib/string/ob_string.h"
#include "lib/thread_local/ob_tsi_factory.h"
namespace oceanbase
{
namespace common
{

char get_xdigit(const char c1)
{
  char ret_char = 0;
  if (c1 >= 'a' && c1 <= 'f') {
    ret_char = (char)(c1 - 'a' + 10);
  } else if (c1 >= 'A' && c1 <= 'F') {
    ret_char = (char)(c1 - 'A' + 10);
  } else {
    ret_char = (char)(c1 - '0');
  }
  return (ret_char & 0x0F);
}

int to_hex_cstr(
    const void *in_data,
    const int64_t data_length,
    char *buff,
    const int64_t buff_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t cstr_pos = 0;
  if (OB_FAIL(to_hex_cstr(in_data, data_length, buff, buff_size, pos, cstr_pos))) {
    LOG_WARN("Failed to transform hex to cstr", K(ret));
  }
  return ret;
}

int hex_to_cstr(const void *in_data,
                const int64_t data_length,
                char *buff,
                const int64_t buff_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(hex_to_cstr(in_data, data_length, buff, buff_size, pos))) {
    LOG_WARN("fail to hex to cstr", K(ret), K(in_data));
  }
  return ret;
}

int hex_to_cstr(const void *in_data,
                const int64_t data_length,
                char *buff,
                const int64_t buff_size,
                int64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;
  if (OB_ISNULL(in_data) || 0 != data_length % 2
      || OB_ISNULL(buff) || buff_size < data_length / 2 + 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(in_data), K(data_length), KP(buff), K(buff_size));
  } else  {
    for (int64_t i = 0; OB_SUCC(ret) && i < data_length; i += 2) {
      const char &c1 = static_cast<const char*>(in_data)[i];
      const char &c2 = static_cast<const char*>(in_data)[i + 1];
      if (!isxdigit(c1) || !isxdigit(c2)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid input string", K(ret), K(c1), K(c2));
      } else {
        buff[i / 2] = (char)((get_xdigit(c1) << 4) | get_xdigit(c2));
      }
    }
  }
  if (OB_SUCC(ret)) {
    pos = data_length / 2;
    buff[data_length / 2] = '\0';
  }
  return ret;
}

static const char *HEXCHARS = "0123456789ABCDEF";
int to_hex_cstr(
    const void *in_data,
    const int64_t data_length,
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    int64_t &cstr_pos)
{
  int ret = OB_SUCCESS;
  cstr_pos = pos;
  if ((OB_UNLIKELY(data_length > 0) && OB_ISNULL(in_data))
      || OB_ISNULL(buf)
      || OB_UNLIKELY(pos < 0)
      || OB_UNLIKELY(buf_len - pos) < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KP(in_data), KP(buf), K(data_length), K(buf_len), K(ret));
  } else if ((buf_len - pos) < (data_length * 2 + 1)) {
    buf[pos++] = '\0';
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size is overflow", K(buf_len), K(data_length), K(buf_len), K(pos), K(ret));
  } else {
    unsigned const char *p = (unsigned const char *)in_data;
    char *dst = static_cast<char *>(buf + pos);
    for (int64_t i = 0; i < data_length; ++i) {
      *dst++ = HEXCHARS[*p >> 4 & 0xF];
      *dst++ = HEXCHARS[*p & 0xF];
      p++;
    }
    *dst = '\0';
    pos += (data_length * 2 + 1);
  }
  return ret;
}

int hex_print(const void* in_buf, const int64_t in_len,
              char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if ((OB_UNLIKELY(in_len > 0) && OB_ISNULL(in_buf))
      || OB_ISNULL(buf)
      || OB_UNLIKELY(buf_len < 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(in_len), KP(in_buf), KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_UNLIKELY(buf_len < pos) || OB_UNLIKELY((buf_len - pos) < in_len * 2)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size is overflow", K(in_len), K(buf_len), K(pos), K(ret));
  } else {
    unsigned const char *p = static_cast<unsigned const char *>(in_buf);
    for (int64_t i = 0; i < in_len; ++i) {
      buf[pos++] = HEXCHARS[*p >> 4 & 0xF];
      buf[pos++] = HEXCHARS[*p & 0xF];
      p++;
    } // end for
  }
  return ret;
}

//used for logtool
int is_printable(const char *buf, int64_t buf_len, bool &is_printable)
{
  int ret = OB_SUCCESS;
  is_printable = true;
  for (int64_t i=0; OB_SUCC(ret) && is_printable && i< buf_len; ++i) {
    if (!std::isprint(buf[i]) || std::isspace(buf[i])) {
      is_printable = false;
    }
  }
  return ret;
}

int32_t str_to_hex(const void *in_data, const int32_t data_length, void *buff,
                   const int32_t buff_size)
{
  unsigned const char *p = NULL;
  unsigned char *o = NULL;
  unsigned char c = 0;
  int32_t i = 0;
  if (NULL != in_data && NULL != buff && buff_size >= data_length / 2) {
    p = (unsigned const char *)in_data;
    o = (unsigned char *)buff;
    c = 0;
    for (i = 0; i < data_length; i++) {
      c = static_cast<unsigned char>(c << 4);
      if (*(p + i) > 'F' ||
          (*(p + i) < 'A' && *(p + i) > '9') ||
          *(p + i) < '0') {
        break;
      }
      if (*(p + i) >= 'A') {
        c = static_cast<unsigned char>(c + (*(p + i) - 'A' + 10));
      } else {
        c = static_cast<unsigned char>(c + (*(p + i) - '0'));
      }
      if (i % 2 == 1) {
        *(o + i / 2) = c;
        c = 0;
      }
    }
  } else {}
  return i;
}

int bit_to_char_array(uint64_t value, int32_t bit_len, char *buf, int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int32_t MAX_LEN = (OB_MAX_BIT_LENGTH + 7) / 8;
  const int32_t bytes_length = (bit_len + 7) / 8;
  char tmp_buf [MAX_LEN];
  static_assert(MAX_LEN == sizeof(uint64_t), "max len not match");

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(bit_len <= 0 )
      || OB_UNLIKELY(bit_len > OB_MAX_BIT_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(bit_len), K(OB_MAX_BIT_LENGTH));
  } else if (OB_UNLIKELY(pos + MAX_LEN > len)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("buffer size overflow", K(len), K(pos), K(MAX_LEN), K(ret));
  } else {
    for (int64_t i = 0; i < sizeof(uint64_t); i++) {
      *((reinterpret_cast<uint8_t *>(tmp_buf)) + i) = static_cast<uint8_t>(value >> (8 * (7 - i)));
    }
    MEMCPY(buf + pos, tmp_buf + MAX_LEN - bytes_length, bytes_length);
    pos += bytes_length;
  }

  return ret;
}

int64_t ObHexStringWrap::to_string(char *buf, const int64_t len) const
{
  int64_t pos = 0;
  (void)hex_print(str_.ptr(), str_.length(), buf, len, pos);
  return pos;
}

////////////////////////////////////////////////////////////////
template <>
int64_t to_string<int64_t>(const int64_t &v, char *buffer, const int64_t buffer_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(databuff_printf(buffer, buffer_size, pos, "%ld", v))) {
  } else {}
  return pos;
}
template <>
int64_t to_string<uint64_t>(const uint64_t &v, char *buffer, const int64_t buffer_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(databuff_printf(buffer, buffer_size, pos, "%lu", v))) {
  } else {}
  return pos;
}

template <>
int64_t to_string<float>(const float &v, char *buffer, const int64_t buffer_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(databuff_printf(buffer, buffer_size, pos, "%.9e", v))) {
  } else {}
  return pos;
}

template <>
int64_t to_string<double>(const double &v, char *buffer, const int64_t buffer_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(databuff_printf(buffer, buffer_size, pos, "%.18le", v))) {
  } else {}
  return pos;
}

template <>
int64_t to_string<bool>(const bool &v, char *buffer, const int64_t buffer_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char *str = v ? "True" : "False";
  if (OB_FAIL(databuff_printf(buffer, buffer_size, pos, "%s", str))) {
  } else {}
  return pos;
}

template <>
const char *to_cstring<const char *>(const char *const &str)
{
  return str;
}

template <>
const char *to_cstring<int64_t>(const int64_t &v)
{
  return to_cstring<int64_t>(v, BoolType<false>());
}

////////////////////////////////////////////////////////////////

int databuff_printf(char *buf, const int64_t buf_len, const char *fmt, ...)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  va_list args;
  va_start(args, fmt);
  if (OB_FAIL(databuff_vprintf(buf, buf_len, pos, fmt, args))) {
  } else {}
  va_end(args);
  return ret;
}

int databuff_printf(char *buf, const int64_t buf_len, int64_t &pos, const char *fmt, ...)
{
  int ret = OB_SUCCESS;
  va_list args;
  va_start(args, fmt);
  if (OB_FAIL(databuff_vprintf(buf, buf_len, pos, fmt, args))) {
  } else {}
  va_end(args);
  return ret;
}

int databuff_printf(char *&buf, int64_t &buf_len, int64_t &pos,
                    ObIAllocator &alloc, const char *fmt, ...)
{
  int ret = OB_SUCCESS;
  va_list args;
  int64_t saved_pos = pos;

  while (OB_SUCC(ret) && pos == saved_pos) {
    va_start(args, fmt);
    if (OB_FAIL(databuff_vprintf(buf, buf_len, pos, fmt, args))) {
      if (OB_SIZE_OVERFLOW == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(multiple_extend_buf(buf, buf_len, alloc))) {
          LOG_WARN("failed to auto extend stmt buf", K(ret));
        } else {
          pos = saved_pos;
        }
      } else {
        LOG_WARN("failed to printf buf", K(ret));
      }
    } else {
      va_end(args);
      break;
    }
    va_end(args);
  }

  return ret;
}

int multiple_extend_buf(char *&buf, int64_t &buf_len, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  int16_t BUF_BLOCK = 1024L;
  int64_t alloc_size = (OB_NOT_NULL(buf)) ? (buf_len << 1) : BUF_BLOCK;
  char *alloc_buf = static_cast<char *>(alloc.alloc(alloc_size));
  if (OB_ISNULL(alloc_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to extend stmt buf", K(ret), K(alloc_size), KP(alloc_buf));
  } else {
    if (OB_NOT_NULL(buf)) {
      MEMCPY(alloc_buf, buf, buf_len);
      alloc.free(buf);
    }
    buf = alloc_buf;
    buf_len = alloc_size;
  }
  return ret;
}

int databuff_vprintf(char *buf, const int64_t buf_len, int64_t &pos, const char *fmt, va_list args)
{
  int ret = OB_SUCCESS;
  if (NULL != buf && 0 <= pos && pos < buf_len) {
    int len = vsnprintf(buf + pos, buf_len - pos, fmt, args);
    if (len < 0) {
      ret = OB_ERR_UNEXPECTED;
    } else if (len < buf_len - pos) {
      pos += len;
    } else {
      pos = buf_len - 1;  //skip '\0' written by vsnprintf
      ret = OB_SIZE_OVERFLOW;
    }
  } else {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}

int databuff_memcpy(char *buf, const int64_t buf_len, int64_t &pos, const int64_t src_len,
                    const char *src)
{
  int ret = OB_SUCCESS;
  if (NULL != buf && 0 <= pos && 0 <= src_len && pos < buf_len - src_len) {
    MEMCPY(buf + pos, src, src_len);
    pos += src_len;
  } else {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}

int bit_print(uint64_t bit_val, char *buffer, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  char tmp_buf[OB_MAX_BIT_LENGTH];
  int64_t tmp_pos = OB_MAX_BIT_LENGTH;
  int64_t tmp_buf_len = 0;
  if (OB_ISNULL(buffer) || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buffer), K(pos), K(ret));
  } else if (0 == bit_val) {
    if (OB_FAIL(databuff_printf(buffer, buf_len, pos, "0"))) {
      LOG_WARN("fail to print buffer", K(ret), KCSTRING(buffer), K(buf_len), K(pos));
    }
  } else {
    do {
      tmp_buf[--tmp_pos] = (bit_val & 1ULL) ? '1' : '0';
    } while((bit_val >>= 1) != 0);

    tmp_buf_len = OB_MAX_BIT_LENGTH - tmp_pos;
    if (OB_UNLIKELY(buf_len - pos < tmp_buf_len)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("buffer size if not enough", K(ret), K(buf_len), K(pos), K(tmp_pos));
    } else {
      MEMCPY(buffer + pos, tmp_buf + tmp_pos, tmp_buf_len);
      pos += tmp_buf_len;
    }
  }

  return ret;
}

template<>
int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const ObString &obj)
{
  int ret = OB_SUCCESS;
  if (obj.length() >= 2 && obj.ptr()[0] == '"' && obj.ptr()[obj.length() - 1] == '"') {
    //ignore "" for select "cccc1"
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\"%.*s\"",
        std::min(static_cast<int32_t>(buf_len - pos), obj.length() - 2), obj.ptr() + 1))) {
    } else {}
  } else {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\"%.*s\"",
        std::min(static_cast<int32_t>(buf_len - pos), obj.length()), obj.ptr()))) {
    } else {}
  }
  return ret;
}

template<>
int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                  const bool with_comma, const ObString &obj)
{
  int ret = OB_SUCCESS;
  if (obj.length() >= 2 && obj.ptr()[0] == '"' && obj.ptr()[obj.length() - 1] == '"') {
    //ignore "" for select "cccc1"
    ret = databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:\"%.*s\""),
        key, std::min(static_cast<int32_t>(buf_len - pos), obj.length() - 2), obj.ptr() + 1);
  } else {
    ret = databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:\"%.*s\""),
        key, std::min(static_cast<int32_t>(buf_len - pos), obj.length()), obj.ptr());
  }
  return ret;
}

int64_t CStringBufMgr::acquire(char *&buffer)
{
  int64_t buf_len = 0;
  if (0 == level_) {
    buffer = local_buf_ + pos_;
    buf_len = BUF_SIZE - pos_;
  } else {
    BufNode *node = NULL;
    node = list_.head_;
    if (NULL != node) {
      list_.head_ = node->pre_;
    }
    if ((NULL != node)
      || (NULL != (node = OB_NEW(BufNode, ObModIds::OB_THREAD_BUFFER)))) {
      buffer = node->buf_ + node->pos_;
      node->pre_ = curr_;
      curr_ = node;
      buf_len = BUF_SIZE - node->pos_;
    } else {
      buffer = NULL;
    }
  }
  return buf_len;
}

void CStringBufMgr::try_clear_list()
{
  if (0 == level_) {
    while (NULL != list_.head_) {
      BufNode *node = list_.head_;
      list_.head_ = node->pre_;
      OB_DELETE(BufNode, ObModIds::OB_THREAD_BUFFER, node);
    }
  }
}

} // end namespace common
} // end namespace oceanbase
