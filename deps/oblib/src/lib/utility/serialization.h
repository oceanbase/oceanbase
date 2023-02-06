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

#ifndef OCEANBASE_COMMON_SERIALIZATION_H_
#define OCEANBASE_COMMON_SERIALIZATION_H_
#include <cstdint>
#include <cstring>
#include <cstdio>
#include <utility>
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace common
{
namespace serialization
{
const uint64_t OB_MAX_V1B = (__UINT64_C(1) << 7) - 1;
const uint64_t OB_MAX_V2B = (__UINT64_C(1) << 14) - 1;
const uint64_t OB_MAX_V3B = (__UINT64_C(1) << 21) - 1;
const uint64_t OB_MAX_V4B = (__UINT64_C(1) << 28) - 1;
const uint64_t OB_MAX_V5B = (__UINT64_C(1) << 35) - 1;
const uint64_t OB_MAX_V6B = (__UINT64_C(1) << 42) - 1;
const uint64_t OB_MAX_V7B = (__UINT64_C(1) << 49) - 1;
const uint64_t OB_MAX_V8B = (__UINT64_C(1) << 56) - 1;
const uint64_t OB_MAX_V9B = (__UINT64_C(1) << 63) - 1;


const uint64_t OB_MAX_1B = (__UINT64_C(1) << 8) - 1;
const uint64_t OB_MAX_2B = (__UINT64_C(1) << 16) - 1;
const uint64_t OB_MAX_3B = (__UINT64_C(1) << 24) - 1;
const uint64_t OB_MAX_4B = (__UINT64_C(1) << 32) - 1;
const uint64_t OB_MAX_5B = (__UINT64_C(1) << 40) - 1;
const uint64_t OB_MAX_6B = (__UINT64_C(1) << 48) - 1;
const uint64_t OB_MAX_7B = (__UINT64_C(1) << 56) - 1;
const uint64_t OB_MAX_8B = UINT64_MAX;



const uint64_t OB_MAX_INT_1B = (__UINT64_C(23));
const uint64_t OB_MAX_INT_2B = (__UINT64_C(1) << 8) - 1;
const uint64_t OB_MAX_INT_3B = (__UINT64_C(1) << 16) - 1;
const uint64_t OB_MAX_INT_4B = (__UINT64_C(1) << 24) - 1;
const uint64_t OB_MAX_INT_5B = (__UINT64_C(1) << 32) - 1;
const uint64_t OB_MAX_INT_7B = (__UINT64_C(1) << 48) - 1;
const uint64_t OB_MAX_INT_9B = UINT64_MAX;


const int64_t OB_MAX_1B_STR_LEN = (__INT64_C(55));
const int64_t OB_MAX_2B_STR_LEN = (__INT64_C(1) << 8) - 1;
const int64_t OB_MAX_3B_STR_LEN = (__INT64_C(1) << 16) - 1;
const int64_t OB_MAX_4B_STR_LEN = (__INT64_C(1) << 24) - 1;
const int64_t OB_MAX_5B_STR_LEN = (__INT64_C(1) << 32) - 1;

const int8_t OB_INT_TYPE_BIT_POS = 7;
const int8_t OB_INT_OPERATION_BIT_POS = 6;
const int8_t OB_INT_SIGN_BIT_POS = 5;
const int8_t OB_DATETIME_OPERATION_BIT = 3;
const int8_t OB_DATETIME_SIGN_BIT = 2;
const int8_t OB_FLOAT_OPERATION_BIT_POS = 0;
const int8_t OB_DECIMAL_OPERATION_BIT_POS = 7;

const int8_t OB_INT_VALUE_MASK = 0x1f;
const int8_t OB_VARCHAR_LEN_MASK = 0x3f;
const int8_t OB_DATETIME_LEN_MASK =  0x3;


const int8_t OB_VARCHAR_TYPE = static_cast<int8_t>((0x1 << 7));
const int8_t OB_SEQ_TYPE = static_cast<int8_t>(0xc0);
const int8_t OB_DATETIME_TYPE = static_cast<int8_t>(0xd0);
const int8_t OB_PRECISE_DATETIME_TYPE = static_cast<int8_t>(0xe0);
const int8_t OB_MODIFYTIME_TYPE = static_cast<int8_t>(0xf0);
const int8_t OB_CREATETIME_TYPE = static_cast<int8_t>(0xf4);
const int8_t OB_FLOAT_TYPE = static_cast<int8_t>(0xf8);
const int8_t OB_DOUBLE_TYPE = static_cast<int8_t>(0xfa);
const int8_t OB_NULL_TYPE =  static_cast<int8_t>(0xfc);
const int8_t OB_BOOL_TYPE =  static_cast<int8_t>(0xfd);
const int8_t OB_EXTEND_TYPE = static_cast<int8_t>(0xfe);
const int8_t OB_DECIMAL_TYPE = static_cast<int8_t>(0xff);
const int8_t OB_NUMBER_TYPE = OB_DECIMAL_TYPE; // 2014number
const int8_t OB_SERIALIZE_SIZE_NEED_BYTES = 5; // 4 bytes encode serialize_size and 1 byte encode flag
inline void set_bit(int8_t &v, int8_t pos)
{
  v |= static_cast<int8_t>(1 << pos);
}

inline void clear_bit(int8_t &v, int8_t pos)
{
  v &= static_cast<int8_t>(~(1 << pos));
}

inline bool test_bit(int8_t v, int8_t pos)
{
  return (v & (1 << pos));
}

/**
 * @brief Encode a byte into given buffer.
 *
 * @param buf address of destination buffer
 * @param val the byte
 *
 */

inline int64_t encoded_length_i8(int8_t val)
{
  return static_cast<int64_t>(sizeof(val));
}

inline int encode_i8(char *buf, const int64_t buf_len, int64_t &pos, int8_t val)
{
  int ret = ((NULL != buf) &&
             ((buf_len - pos) >= static_cast<int64_t>(sizeof(val)))) ? OB_SUCCESS : OB_SIZE_OVERFLOW;
  if (OB_SUCC(ret)) {
    *(buf + pos++) = val;
  }
  return ret;
}

inline int decode_i8(const char *buf, const int64_t data_len, int64_t &pos, int8_t *val)
{
  int ret = (NULL != buf && data_len - pos >= 1) ? OB_SUCCESS : OB_DESERIALIZE_ERROR;
  if (OB_SUCC(ret)) {
    *val = *(buf + pos++);
  }
  return ret;
}

/**
 * @brief Enocde a 16-bit integer in big-endian order
 *
 * @param buf address of destination buffer
 * @param val value to encode
 */
inline int64_t encoded_length_i16(int16_t val)
{
  return static_cast<int64_t>(sizeof(val));
}

inline int encode_i16(char *buf, const int64_t buf_len, int64_t &pos, int16_t val)
{
  int ret = ((NULL != buf) &&
             ((buf_len - pos) >= static_cast<int64_t>(sizeof(val)))) ? OB_SUCCESS : OB_SIZE_OVERFLOW;
  if (OB_SUCC(ret)) {
    *(buf + pos++) = static_cast<char>((((val) >> 8)) & 0xff);
    *(buf + pos++) = static_cast<char>((val) & 0xff);
  }
  return ret;
}

inline int decode_i16(const char *buf, const int64_t data_len, int64_t &pos, int16_t *val)
{
  int ret = (NULL != buf && data_len - pos  >= 2) ? OB_SUCCESS : OB_DESERIALIZE_ERROR;
  if (OB_SUCC(ret)) {
    *val = static_cast<int16_t>(((*(buf + pos++)) & 0xff) << 8);
    *val = static_cast<int16_t>(*val | (*(buf + pos++) & 0xff));
  }
  return ret;
}

inline int64_t encoded_length_i32(int32_t val)
{
  return static_cast<int64_t>(sizeof(val));
}

inline int encode_i32(char *buf, const int64_t buf_len, int64_t &pos, int32_t val)
{
  int ret = ((NULL != buf) &&
             ((buf_len - pos) >= static_cast<int64_t>(sizeof(val)))) ? OB_SUCCESS : OB_SIZE_OVERFLOW;
  if (OB_SUCC(ret)) {
    *(buf + pos++) = static_cast<char>(((val) >> 24) & 0xff);
    *(buf + pos++) = static_cast<char>(((val) >> 16) & 0xff);
    *(buf + pos++) = static_cast<char>(((val) >> 8) & 0xff);
    *(buf + pos++) = static_cast<char>((val) & 0xff);
  }
  return ret;
}

inline int decode_i32(const char *buf, const int64_t data_len, int64_t &pos, int32_t *val)
{

  int ret = (NULL != buf && data_len - pos  >= 4) ? OB_SUCCESS : OB_DESERIALIZE_ERROR;
  if (OB_SUCC(ret)) {
    *val = ((static_cast<int32_t>(*(buf + pos++))) & 0xff) << 24;
    *val |= ((static_cast<int32_t>(*(buf + pos++))) & 0xff) << 16;
    *val |= ((static_cast<int32_t>(*(buf + pos++))) & 0xff) << 8;
    *val |= ((static_cast<int32_t>(*(buf + pos++))) & 0xff);
  }
  return ret;
}

inline int64_t encoded_length_i64(int64_t val)
{
  return static_cast<int64_t>(sizeof(val));
}

inline int encode_i64(char *buf, const int64_t buf_len, int64_t &pos, int64_t val)
{
  int ret = ((NULL != buf) &&
             ((buf_len - pos) >= static_cast<int64_t>(sizeof(val)))) ? OB_SUCCESS : OB_SIZE_OVERFLOW;
  if (OB_SUCC(ret)) {
    *(buf + pos++) = static_cast<char>(((val) >> 56) & 0xff);
    *(buf + pos++) = static_cast<char>(((val) >> 48) & 0xff);
    *(buf + pos++) = static_cast<char>(((val) >> 40) & 0xff);
    *(buf + pos++) = static_cast<char>(((val) >> 32) & 0xff);
    *(buf + pos++) = static_cast<char>(((val) >> 24) & 0xff);
    *(buf + pos++) = static_cast<char>(((val) >> 16) & 0xff);
    *(buf + pos++) = static_cast<char>(((val) >> 8) & 0xff);
    *(buf + pos++) = static_cast<char>((val) & 0xff);
  }
  return ret;
}

inline int decode_i64(const char *buf, const int64_t data_len, int64_t &pos, int64_t *val)
{
  int ret = (NULL != buf && data_len - pos  >= 8) ? OB_SUCCESS : OB_DESERIALIZE_ERROR;
  if (OB_SUCC(ret)) {
    *val = ((static_cast<int64_t>((*(buf + pos++))) & 0xff)) << 56;
    *val |= ((static_cast<int64_t>((*(buf + pos++))) & 0xff)) << 48;
    *val |= ((static_cast<int64_t>((*(buf + pos++))) & 0xff)) << 40;
    *val |= ((static_cast<int64_t>((*(buf + pos++))) & 0xff)) << 32;
    *val |= ((static_cast<int64_t>((*(buf + pos++))) & 0xff)) << 24;
    *val |= ((static_cast<int64_t>((*(buf + pos++))) & 0xff)) << 16;
    *val |= ((static_cast<int64_t>((*(buf + pos++))) & 0xff)) << 8;
    *val |= ((static_cast<int64_t>((*(buf + pos++))) & 0xff));
  }
  return ret;
}

inline int64_t encoded_length_bool(bool val)
{
  UNUSED(val);
  return static_cast<int64_t>(1);
}

inline int encode_bool(char *buf, const int64_t buf_len, int64_t &pos, bool val)
{
  int ret = ((NULL != buf) &&
             ((buf_len - pos) >= static_cast<int64_t>(sizeof(val)))) ? OB_SUCCESS : OB_SIZE_OVERFLOW;
  if (OB_SUCC(ret)) {
    *(buf + pos++) = (val) ? 1 : 0;
  }
  return ret;
}

inline int decode_bool(const char *buf, const int64_t buf_len, int64_t &pos, bool *val)
{
  int ret = OB_DESERIALIZE_ERROR;
  int8_t v = 0;
  if ((ret = decode_i8(buf, buf_len, pos, &v)) == 0) {
    *val = (v != 0);
  }
  return ret;
}

inline int64_t encoded_length_vi64(int64_t val)
{
  uint64_t __v = static_cast<uint64_t>(val);
  int64_t need_bytes = 0;
  if (__v <= OB_MAX_V1B) {
    need_bytes = 1;
  } else if (__v <= OB_MAX_V2B) {
    need_bytes = 2;
  } else if (__v <= OB_MAX_V3B) {
    need_bytes = 3;
  } else if (__v <= OB_MAX_V4B) {
    need_bytes = 4;
  } else if (__v <= OB_MAX_V5B) {
    need_bytes = 5;
  } else if (__v <= OB_MAX_V6B) {
    need_bytes = 6;
  } else if (__v <= OB_MAX_V7B) {
    need_bytes = 7;
  } else if (__v <= OB_MAX_V8B) {
    need_bytes = 8;
  } else if (__v <= OB_MAX_V9B) {
    need_bytes = 9;
  } else {
    need_bytes = 10;
  }
  return need_bytes;
}

/**
 * @brief Encode a integer (up to 64bit) in variable length encoding
 *
 * @param buf pointer to the destination buffer
 * @param end the end pointer to the destination buffer
 * @param val value to encode
 *
 * @return true - success, false - failed
 */
inline int encode_vi64(char *buf, const int64_t buf_len, int64_t &pos, int64_t val)
{
  uint64_t __v = static_cast<uint64_t>(val);
  int ret = ((NULL != buf) &&
             ((buf_len - pos) >= encoded_length_vi64(__v))) ? OB_SUCCESS : OB_SIZE_OVERFLOW;
  if (OB_SUCC(ret)) {
    while (__v > OB_MAX_V1B) {
      *(buf + pos++) = static_cast<int8_t>((__v) | 0x80);
      __v >>= 7;
    }
    if (__v <= OB_MAX_V1B) {
      *(buf + pos++) = static_cast<int8_t>((__v) & 0x7f);
    }
  }
  return ret;
}

/* compatible with decode_vi64 */
inline int encode_fixed_bytes_i64(char *buf, const int64_t buf_len, int64_t &pos, int64_t val)
{
  uint64_t __v = static_cast<uint64_t>(val);
  int ret = ((NULL != buf) &&
             ((buf_len - pos) >= OB_SERIALIZE_SIZE_NEED_BYTES)) ? OB_SUCCESS : OB_SIZE_OVERFLOW;
  if (OB_SUCC(ret)) {
    int n = OB_SERIALIZE_SIZE_NEED_BYTES;
    while (n--) {
      if (n > 0) {
        *(buf + pos++) = static_cast<int8_t>((__v) | 0x80);
        __v >>= 7;
      } else {
        *(buf + pos++) = static_cast<int8_t>((__v) & 0x7f);
      }
    }
  }
  return ret;
}

inline int decode_vi64(const char *buf, const int64_t data_len, int64_t &pos, int64_t *val)
{
  uint64_t __v = 0;
  uint32_t shift = 0;
  int64_t tmp_pos = pos;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && ((*(buf + tmp_pos)) & 0x80)) {
    if (data_len - tmp_pos < 1) {
      ret = OB_DESERIALIZE_ERROR;
      break;
    }
    __v |= (static_cast<uint64_t>(*(buf + tmp_pos++)) & 0x7f) << shift;
    shift += 7;
  }
  if (OB_SUCC(ret)) {
    if (data_len - tmp_pos < 1) {
      ret = OB_DESERIALIZE_ERROR;
    } else {
      __v |= ((static_cast<uint64_t>(*(buf + tmp_pos++)) & 0x7f) << shift);
      *val = static_cast<int64_t>(__v);
      pos = tmp_pos;
    }
  }
  return ret;
}

inline int64_t encoded_length_vi32(int32_t val)
{
  uint32_t __v = static_cast<uint32_t>(val);
  int64_t need_bytes = 0;
  if (__v <= OB_MAX_V1B) {
    need_bytes = 1;
  } else if (__v <= OB_MAX_V2B) {
    need_bytes = 2;
  } else if (__v <= OB_MAX_V3B) {
    need_bytes = 3;
  } else if (__v <= OB_MAX_V4B) {
    need_bytes = 4;
  } else {
    need_bytes = 5;
  }
  return need_bytes;

}

/**
 * @brief Encode a integer (up to 32bit) in variable length encoding
 *
 * @param buf pointer to the destination buffer
 * @param end the end pointer to the destination buffer
 * @param val value to encode
 *
 * @return true - success, false - failed
 */

inline int encode_vi32(char *buf, const int64_t buf_len, int64_t &pos, int32_t val)
{
  uint32_t __v = static_cast<uint32_t>(val);
  int ret = ((NULL != buf) &&
             ((buf_len - pos) >= encoded_length_vi32(val))) ? OB_SUCCESS : OB_SIZE_OVERFLOW;
  if (OB_SUCC(ret)) {
    while (__v > OB_MAX_V1B) {
      *(buf + pos++) = static_cast<int8_t>((__v) | 0x80);
      __v >>= 7;
    }
    if (__v <= OB_MAX_V1B) {
      *(buf + pos++) = static_cast<int8_t>((__v) & 0x7f);
    }
  }
  return ret;

}

inline int decode_vi32(const char *buf, const int64_t data_len, int64_t &pos, int32_t *val)
{
  uint32_t __v = 0;
  uint32_t shift = 0;
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  while (OB_SUCC(ret) && ((*(buf + tmp_pos)) & 0x80)) {
    if (data_len - tmp_pos < 1) {
      ret = OB_DESERIALIZE_ERROR;
      break;
    }
    __v |= (static_cast<uint32_t>(*(buf + tmp_pos++)) & 0x7f) << shift;
    shift += 7;
  }
  if (OB_SUCC(ret)) {
    if (data_len - tmp_pos < 1) {
      ret = OB_DESERIALIZE_ERROR;
    } else {
      __v |= (static_cast<uint32_t>(*(buf + tmp_pos++)) & 0x7f) << shift;
      *val = static_cast<int32_t>(__v);
      pos = tmp_pos;
    }
  }
  return ret;
}

inline int64_t encoded_length_float(float val)
{
  int32_t tmp = 0;
  MEMCPY(&tmp, &val, sizeof(tmp));
  return encoded_length_vi32(tmp);
}

inline int encode_float(char *buf, const int64_t buf_len, int64_t &pos, float val)
{
  int32_t tmp = 0;
  MEMCPY(&tmp, &val, sizeof(tmp));
  return encode_vi32(buf, buf_len, pos, tmp);
}

inline int decode_float(const char *buf, const int64_t data_len, int64_t &pos, float *val)
{
  int32_t tmp = 0;
  int ret = OB_SUCCESS;
  if ((ret = decode_vi32(buf, data_len, pos, &tmp)) == 0) {
    MEMCPY(val, &tmp, sizeof(*val));
  }
  return ret;
}

inline int64_t encoded_length_double(double val)
{
  int64_t tmp = 0;
  MEMCPY(&tmp, &val, sizeof(tmp));
  return encoded_length_vi64(tmp);
}

inline int encode_double(char *buf, const int64_t buf_len, int64_t &pos, double val)
{
  int64_t tmp = 0;
  MEMCPY(&tmp, &val, sizeof(tmp));
  return encode_vi64(buf, buf_len, pos, tmp);
}

inline int decode_double(const char *buf, const int64_t data_len, int64_t &pos, double *val)
{
  int64_t tmp = 0;
  int ret = OB_SUCCESS;
  if ((ret = decode_vi64(buf, data_len, pos, &tmp)) == 0) {
    MEMCPY(val, &tmp, sizeof(*val));
  }
  return ret;
}

/**
 * @brief Computes the encoded length of vstr(int64,data,null)
 *
 * @param len string length
 *
 * @return the encoded length of str
 */
inline int64_t encoded_length_vstr(int64_t len)
{
  return encoded_length_vi64(len) + len + 1;
}

inline int64_t encoded_length_vstr(const char *str)
{
  return encoded_length_vstr(str ? static_cast<int64_t>(strlen(str) + 1) : 0);
}

/**
 * @brief get the decoded length of data len of vstr
 * won't change the pos
 * @return the length of data
 */
inline int64_t decoded_length_vstr(const char *buf, const int64_t data_len, int64_t pos)
{
  int64_t len = -1;
  int64_t tmp_pos = pos;
  if (NULL == buf || data_len < 0 || pos < 0) {
    len = -1;
  } else if (decode_vi64(buf, data_len, tmp_pos, &len) != 0) {
    len = -1;
  }
  return len;
}

/**
 * @brief Encode a buf as vstr(int64,data,null)
 *
 * @param buf pointer to the destination buffer
 * @param vbuf pointer to the start of the input buffer
 * @param len length of the input buffer
 */
inline int encode_vstr(char *buf, const int64_t buf_len, int64_t &pos, const void *vbuf,
                       int64_t len)
{
  int ret = ((NULL != buf) && (len >= 0)
             && ((buf_len - pos) >= static_cast<int32_t>(encoded_length_vstr(len))))
            ?  OB_SUCCESS : OB_SIZE_OVERFLOW;
  if (OB_SUCC(ret)) {
    /**
     * even through it's a null string, we can serialize it with
     * lenght 0, and following a '\0'
     */
    ret = encode_vi64(buf, buf_len, pos, len);
    if (OB_SUCCESS == ret && len > 0 && NULL != vbuf) {
      MEMCPY(buf + pos, vbuf, len);
      pos += len;
    }
    *(buf + pos++) = 0;
  }
  return ret;
}

inline int encode_vstr(char *buf, const int64_t buf_len, int64_t &pos, const char *s)
{
  return encode_vstr(buf, buf_len, pos, s, s ? strlen(s) + 1 : 0);
}

inline const char *decode_vstr(const char *buf, const int64_t data_len, int64_t &pos, int64_t *lenp)
{
  const char *str = 0;
  int64_t tmp_len = 0;
  int64_t tmp_pos = pos;

  if ((NULL == buf) || (data_len < 0) || (pos < 0) || (NULL == lenp)) {
    //just _return_;
  } else if (decode_vi64(buf, data_len, tmp_pos, &tmp_len) != OB_SUCCESS) {
    *lenp = -1;
  } else if (tmp_len >= 0) {
    if (data_len - tmp_pos >= tmp_len) {
      str = buf + tmp_pos;
      *lenp = tmp_len++;
      tmp_pos += tmp_len;
      pos = tmp_pos;
    } else {
      *lenp = -1;
    }
  }
  return str;
}

inline const char *decode_vstr(const char *buf, const int64_t data_len, int64_t &pos,
                               char *dest, int64_t buf_len, int64_t *lenp)
{
  const char *str = 0;
  int64_t tmp_len = 0;
  int64_t tmp_pos = pos;
  if ((NULL == buf) || (data_len < 0) || (pos < 0) || (0 == dest) || buf_len < 0 || (NULL == lenp)) {
    //just _return_;
  } else if (decode_vi64(buf, data_len, tmp_pos, &tmp_len) != 0 || tmp_len > buf_len) {
    *lenp = -1;
  } else if (tmp_len >= 0) {
    if (data_len - tmp_pos >= tmp_len) {
      str = buf + tmp_pos;
      *lenp = tmp_len++;
      MEMCPY(dest, str, *lenp);
      tmp_pos += tmp_len;
      pos = tmp_pos;
    } else {
      *lenp = -1;
    }
  }
  return str;
}

/**
 * @brief encode ObBool type of ObObject into given buffer
 *
 * @param buf pointer to the destination buffer
 * @param buf_len the length of the destination buffer
 * @param pos the current position of the destination buffer
 *
 * @return on success, OB_SUCCESS is returned, on error,OB_SIZE_OVERFLOW is returned.
 */
inline int64_t encoded_length_bool_type(bool val)
{
  return static_cast<int64_t>(1 + encoded_length_bool(val));
}

inline int encode_bool_type(char *buf, int64_t buf_len, int64_t &pos, const bool val)
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS != encode_i8(buf, buf_len, pos, OB_BOOL_TYPE)) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_SUCCESS != encode_bool(buf, buf_len, pos, val)) {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}

inline int decode_bool_type(const char *buf, const int64_t buf_len, int8_t first_byte, int64_t &pos,
                            bool &val)
{
  UNUSED(first_byte);

  int ret = OB_DESERIALIZE_ERROR;

  if (NULL == buf || buf_len - pos < static_cast<int64_t>(sizeof(val))) {
    ret = OB_DESERIALIZE_ERROR;
  } else {
    bool tmp = false;
    if (OB_SUCCESS != (ret = decode_bool(buf, buf_len, pos, &tmp))) {
      ret = OB_DESERIALIZE_ERROR;
    } else {
      MEMCPY(&val, &tmp, sizeof(val));
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

/**
 * @brief encode ObNull type of ObObject into given buffer
 *
 * @param buf pointer to the destination buffer
 * @param buf_len the length of the destination buffer
 * @param pos the current position of the destination buffer
 *
 * @return on success,OB_SUCCESS is returned, on error,OB_SIZE_OVERFLOW is returned.
 */

inline int64_t encoded_length_null()
{
  return static_cast<int64_t>(1);
}

inline int encode_null(char *buf, int64_t buf_len, int64_t &pos)
{
  return encode_i8(buf, buf_len, pos, OB_NULL_TYPE);
}

inline uint64_t safe_int64_abs(int64_t val)
{
  int64_t __v = val;
  int64_t tmp = __v >> 63;
  __v ^= tmp;
  __v -= tmp;
  return static_cast<uint64_t>(__v);
}


/**
 * @brief encode a uint64_t into given buffer with given length
 *
 * @param buf       pointer to the destination buffer
 * @param buf_len   the length of buf
 * @param pos       the current position of the buffer
 * @param val       the value to encode
 * @param val_len   the encoded length of val in bytes
 *
 * @return  on success, OB_SUCCESS is returned
 */
inline int __encode_uint_with_given_length(char *buf, const int64_t buf_len,
                                           int64_t &pos,
                                           const uint64_t val, int64_t val_len)
{
  int ret = OB_SUCCESS;
  if ((NULL == buf) || (val_len <= 0) || ((buf_len - pos) < val_len)) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    uint64_t __v = val;
    int64_t tmp_pos = pos;
    for (int64_t n = 0; n < val_len; ++n) {
      int8_t t = static_cast<int8_t>((__v >>(n << 3)) & 0xff);
      ret = encode_i8(buf, buf_len, tmp_pos, t);
      if (OB_FAIL(ret)) {
        break;
      }
    }
    if (OB_SUCC(ret)) {
      pos = tmp_pos;
    }
  }
  return ret;
}

inline int __decode_uint_with_given_length(const char *buf, const int64_t data_len,
                                           int64_t &pos,
                                           uint64_t &val, int64_t val_len)
{
  int ret = OB_SUCCESS;
  if ((NULL == buf) || (data_len - pos < val_len)) {
    ret = OB_DESERIALIZE_ERROR;
  } else {
    int8_t tmp = 0;
    uint64_t __v = 0;
    int64_t tmp_pos = pos;
    for (int64_t n = 0; n < val_len; ++n) {
      if (OB_SUCCESS != (ret = decode_i8(buf, data_len, tmp_pos, &tmp))) {
        ret = OB_DESERIALIZE_ERROR;
        break;
      } else {
        __v |= (static_cast<uint64_t>((static_cast<uint8_t>(tmp))) << (n << 3));
      }
    }

    if (OB_SUCC(ret)) {
      val = __v;
      pos = tmp_pos;
    }
  }
  return ret;
}

/**
 * @brief Computes the encoded length of int(type,val)
 *
 * @param val the int value to be encoded
 *
 * @return the encoded length of int
 */
inline int64_t encoded_length_int(int64_t val)
{
  uint64_t __v = safe_int64_abs(val);
  int64_t len = 0;
  if (__v <= OB_MAX_INT_1B) {
    len = 1;
  } else if (__v <= OB_MAX_INT_2B) {
    len = 2;
  } else if (__v <= OB_MAX_INT_3B) {
    len = 3;
  } else if (__v <= OB_MAX_INT_4B) {
    len = 4;
  } else if (__v <= OB_MAX_INT_5B) {
    len = 5;
  } else if (__v <= OB_MAX_INT_7B) {
    len = 7;
  } else { /*if(__v < OB_MAX_INT_9B)*/
    len = 9;
  }
  return len;
}

inline int fast_encode(char *buf, int64_t &pos, int64_t val, bool is_add = false)
{
  int ret = OB_SUCCESS;
  int8_t first_byte = 0;
  if (val < 0) {
    set_bit(first_byte, OB_INT_SIGN_BIT_POS);
    val = -val;
    //val = safe_int64_abs(val);
  }
  if (is_add) {
    set_bit(first_byte, OB_INT_OPERATION_BIT_POS);
  }
  if ((uint64_t)val <= OB_MAX_INT_1B) {
    first_byte |= static_cast<int8_t>(val);
    buf[pos++] = first_byte;
  } else {
    first_byte |= static_cast<int8_t> (OB_MAX_INT_1B);
    if ((uint64_t)val <= OB_MAX_INT_4B) {
      if ((uint64_t)val <= OB_MAX_INT_2B) {
        first_byte = static_cast<int8_t>(first_byte + 1);
        buf[pos++] = first_byte;
        goto G_INT2B;
      } else {
        if ((uint64_t)val <= OB_MAX_INT_3B) {
          first_byte = static_cast<int8_t>(first_byte + 2);
          buf[pos++] = first_byte;
          goto G_INT3B;
        } else {
          first_byte = static_cast<int8_t>(first_byte + 3);
          buf[pos++] = first_byte;
          goto G_INT4B;
        }
      }
    } else {
      if ((uint64_t)val <= OB_MAX_INT_5B) {
        first_byte = static_cast<int8_t>(first_byte + 4);
        buf[pos++] = first_byte;
        goto G_INT5B;
      } else {
        if ((uint64_t)val <= OB_MAX_INT_7B) {
          first_byte = static_cast<int8_t>(first_byte + 5);
          buf[pos++] = first_byte;
          goto G_INT7B;
        } else {
          first_byte = static_cast<int8_t>(first_byte + 7);
          buf[pos++] = first_byte;
          goto G_INT9B;
        }
      }
    }

G_INT9B:
    buf[pos++] = (int8_t)(val & 0xFF);
    val >>= 8;
    buf[pos++] = (int8_t)(val & 0xFF);
    val >>= 8;
G_INT7B:
    buf[pos++] = (int8_t)(val & 0xFF);
    val >>= 8;
    buf[pos++] = (int8_t)(val & 0xFF);
    val >>= 8;
G_INT5B:
    buf[pos++] = (int8_t)(val & 0xFF);
    val >>= 8;
G_INT4B:
    buf[pos++] = (int8_t)(val & 0xFF);
    val >>= 8;
G_INT3B:
    buf[pos++] = (int8_t)(val & 0xFF);
    val >>= 8;
G_INT2B:
    buf[pos++] = (int8_t)(val & 0xFF);
  }

  return ret;
}

/**
 * @brief encoded the ObInt type of ObObject into given buffer
 *
 * @param buf pointer to the destination buffer
 * @param buf_len the length of buf
 * @param pos the current position of the buffer
 * @param val the value of int
 * @param is_add operation type
 *
 * @return on success,OB_SUCCESS is returned,on error,OB_SIZE_OVERFLOW is returned.
 */
inline int encode_int_safe(char *buf, const int64_t buf_len, int64_t &pos, int64_t val,
                           bool is_add = false)
{
  int ret = OB_SIZE_OVERFLOW;
  int64_t len = encoded_length_int(val);
  if ((NULL == buf) || ((buf_len - pos) < len)) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    int8_t first_byte = 0;
    if (val < 0) {
      set_bit(first_byte, OB_INT_SIGN_BIT_POS);
    }

    if (is_add) {
      set_bit(first_byte, OB_INT_OPERATION_BIT_POS);
    }
    uint64_t __v = safe_int64_abs(val);

    if (__v <= OB_MAX_INT_1B) {
      first_byte = static_cast<int8_t>(first_byte | __v);
    } else {
      //24: 1 byte
      //25: 2 bytes
      //26: 3 bytes
      //27: 4 bytes
      //28: 6 bytes
      //29: 8 bytes
      //30,31 reserved
      int64_t tmp_len = len;
      if (7 == tmp_len || 9 == tmp_len) {
        tmp_len -= 1;
      }
      first_byte = static_cast<int8_t>(first_byte | ((tmp_len - 1) + OB_MAX_INT_1B));
    }

    if (OB_SUCCESS != (ret = encode_i8(buf, buf_len, pos, first_byte))) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      if ((len - 1) > 0) {
        ret = __encode_uint_with_given_length(buf, buf_len, pos, __v, len - 1);
      }
    }
  }
  return ret;
}

inline int encode_int(char *buf, const int64_t buf_len, int64_t &pos, int64_t val,
                      bool is_add = false)
{
  if (buf_len - pos >= 10) {
    return fast_encode(buf, pos, val, is_add);
  } else {
    return encode_int_safe(buf, buf_len, pos, val, is_add);
  }
}

inline int fast_decode(const char *buf, int8_t first_byte, int64_t &pos,
                       int64_t &val, bool &is_add)
{
  int ret = OB_SUCCESS;

  bool is_neg = test_bit(first_byte, OB_INT_SIGN_BIT_POS);
  is_add = test_bit(first_byte, OB_INT_OPERATION_BIT_POS);
  int8_t len_or_value = first_byte & OB_INT_VALUE_MASK;

  if (len_or_value <= static_cast<int8_t>(OB_MAX_INT_1B)) {
    val = is_neg ? static_cast<int64_t>(-len_or_value) : static_cast<int64_t>(len_or_value);
  } else {
    val = 0;
    int64_t npos = 0;
    //int64_t len = len_or_value - OB_MAX_INT_1B;
    if ((uint8_t)len_or_value <= OB_MAX_INT_1B + 3) {
      if (len_or_value == OB_MAX_INT_1B + 1) {
        pos ++;
        npos = pos;
        goto D_INT2B;
      } else if (len_or_value == OB_MAX_INT_1B + 2) {
        pos += 2;
        npos = pos;
        goto D_INT3B;
      } else {
        pos += 3;
        npos = pos;
        goto D_INT4B;
      }
    } else {
      if (len_or_value == OB_MAX_INT_1B + 4) {
        pos += 4;
        npos = pos;
        goto D_INT5B;
      } else if (len_or_value == OB_MAX_INT_1B + 5) {
        pos += 6;
        npos = pos;
        goto D_INT7B;
      } else {
        pos += 8;
        npos = pos;
        goto D_INT9B;
      }
    }

D_INT9B:
    val |= (uint8_t)buf[--npos];
    val <<= 8;
    val |= (uint8_t)buf[--npos];
    val <<= 8;
D_INT7B:
    val |= (uint8_t)buf[--npos];
    val <<= 8;
    val |= (uint8_t)buf[--npos];
    val <<= 8;
D_INT5B:
    val |= (uint8_t)buf[--npos];
    val <<= 8;
D_INT4B:
    val |= (uint8_t)buf[--npos];
    val <<= 8;
D_INT3B:
    val |= (uint8_t)buf[--npos];
    val <<= 8;
D_INT2B:
    val |= (uint8_t)buf[--npos];

    if (is_neg) {
      val = -val;
    }
  }

  return ret;
}

inline int decode_int_safe(const char *buf, const int64_t data_len,
                           int8_t first_byte, int64_t &pos, int64_t &val, bool &is_add)
{
  int ret = OB_DESERIALIZE_ERROR;
  if ((NULL == buf) || (data_len - pos < 0)) {
    ret = OB_DESERIALIZE_ERROR;
  } else {
    bool is_neg = test_bit(first_byte, OB_INT_SIGN_BIT_POS);
    is_add = test_bit(first_byte, OB_INT_OPERATION_BIT_POS);
    int8_t len_or_value = first_byte & OB_INT_VALUE_MASK;
    int64_t __v = 0;

    if (len_or_value <= static_cast<int8_t>(OB_MAX_INT_1B)) {
      __v = is_neg ? static_cast<int64_t>(-len_or_value) : static_cast<int64_t>(len_or_value);
      ret = OB_SUCCESS;
      val = __v;
    } else {
      int64_t len = len_or_value - OB_MAX_INT_1B;
      if (5 == len || 7 == len) {
        len += 1;
      }
      uint64_t uv = 0;
      ret = __decode_uint_with_given_length(buf, data_len, pos, uv, len);
      if (OB_SUCC(ret)) {
        __v = static_cast<int64_t>(uv);
        val = is_neg ? -__v : __v;
      }
    }
  }
  return ret;
}

inline int decode_int(const char *buf, const int64_t data_len, int8_t first_byte,
                      int64_t &pos, int64_t &val, bool &is_add)
{
  (void)(data_len);
  if (data_len - pos >= 9) {
    return fast_decode(buf, first_byte, pos, val, is_add);
  } else {
    return decode_int_safe(buf, data_len, first_byte, pos, val, is_add);
  }
}

inline int64_t encoded_length_str_len(int64_t len)
{
  int64_t ret = -1;
  if (len < 0) {
    ret = -1;
  } else {
    if (len <= OB_MAX_1B_STR_LEN) {
      ret = 1;
    } else if (len <= OB_MAX_2B_STR_LEN) {
      ret = 2;
    } else if (len <= OB_MAX_3B_STR_LEN) {
      ret = 3;
    } else if (len <= OB_MAX_4B_STR_LEN) {
      ret = 4;
    } else if (len <= OB_MAX_5B_STR_LEN) {
      ret = 5;
    } else {
      ret = -1;
    }
  }
  return ret;
}

inline int64_t encoded_length_str(int64_t len)
{
  return encoded_length_str_len(len) + len;
}

inline int encode_str(char *buf, const int64_t buf_len, int64_t &pos, const void *vbuf, int64_t len)
{
  int ret = OB_SIZE_OVERFLOW;
  int64_t len_size = encoded_length_str_len(len);
  if ((NULL == buf) || (len_size < 0) || (buf_len - pos < len_size + len) || pos < 0
      || (len < 0)) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    int8_t first_byte = OB_VARCHAR_TYPE;

    if (1 == len_size) {
      first_byte = static_cast<int8_t>(first_byte | (len & 0xff));
    } else {
      first_byte = static_cast<int8_t>(first_byte | (len_size  - 1 + OB_MAX_1B_STR_LEN));
    }

    if (OB_SUCCESS != (ret = encode_i8(buf, buf_len, pos, first_byte))) {
      ret = OB_SIZE_OVERFLOW;
    } else if (len_size > 1) {
      for (int n = 0; n < len_size - 1; ++n) {
        if (OB_SUCCESS != (ret = encode_i8(buf, buf_len, pos, static_cast<int8_t>(len >>(n << 3))))) {
          break;
        }
      }
    }

    if (OB_SUCCESS == ret && (NULL != vbuf) && (len > 0)) {
      MEMCPY(buf + pos, vbuf, len);
      pos += len;
    }
  }
  return ret;
}

inline const char *decode_str(const char *buf, const int64_t data_len, int8_t first_byte,
                              int64_t &pos, int32_t &lenp)
{
  const char *str = NULL;
  int ret = OB_DESERIALIZE_ERROR;
  if ((NULL == buf) || data_len < 0) {
    str = NULL;
  } else {
    int8_t len_or_value = first_byte & OB_VARCHAR_LEN_MASK;
    int64_t str_len = 0;
    if (len_or_value <= OB_MAX_1B_STR_LEN) {
      //OK ,we have already got the length of str
      str_len = static_cast<int64_t>(len_or_value);
      ret = OB_SUCCESS;
    } else {
      int8_t tmp = 0;
      for (int n = 0; n < len_or_value - OB_MAX_1B_STR_LEN; ++n) {
        if (OB_SUCCESS != (ret = decode_i8(buf, data_len, pos, &tmp))) {
          str_len = -1;
          break;
        } else {
          str_len |= (static_cast<int64_t>((static_cast<uint8_t>(tmp))) << (n << 3));
        }
      }
    }

    if ((OB_SUCC(ret)) && (str_len >= 0) && (data_len - pos >= str_len)) {
      str = buf + pos;
      pos += str_len;
      lenp = static_cast<int32_t>(str_len);
    } else {
      lenp = -1;
      str = NULL;
    }

  }
  return str;
}

inline int64_t encoded_length_float_type()
{
  return static_cast<int64_t>(sizeof(float) + 1);
}

inline int encode_float_type(char *buf, const int64_t buf_len, int64_t &pos, const float val,
                      const bool is_add)
{
  int32_t tmp = 0;
  MEMCPY(&tmp, &val, sizeof(tmp));
  int ret = OB_SIZE_OVERFLOW;
  int8_t first_byte = OB_FLOAT_TYPE;

  if (is_add) {
    set_bit(first_byte, OB_FLOAT_OPERATION_BIT_POS);
  }

  if (OB_SUCCESS != (ret = encode_i8(buf, buf_len, pos, first_byte))) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    ret = encode_i32(buf, buf_len, pos, tmp);
  }

  return ret;
}

inline int decode_float_type(const char *buf, const int64_t data_len, int8_t first_byte,
                      int64_t &pos, float &val, bool &is_add)
{
  int ret = OB_DESERIALIZE_ERROR;
  if (NULL == buf || data_len - pos < static_cast<int64_t>(sizeof(float))) {
    ret = OB_DESERIALIZE_ERROR;
  } else {
    int32_t tmp = 0;
    if (OB_SUCCESS != (ret = decode_i32(buf, data_len, pos, &tmp))) {
      ret = OB_DESERIALIZE_ERROR;
    } else {
      MEMCPY(&val, &tmp, sizeof(val));
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret)) {
      is_add = serialization::test_bit(first_byte, OB_FLOAT_OPERATION_BIT_POS);
    }
  }
  return ret;
}

inline int64_t encoded_length_double_type()
{
  return static_cast<int64_t>(sizeof(double) + 1);
}

inline int64_t encode_length_number_type(const ObNumberDesc &desc)
{
  return (encoded_length_i32(desc.desc_) + sizeof(uint32_t) * desc.len_);
}

inline int decode_number_type(const char *buf, const int64_t buf_len, int64_t &pos, ObNumberDesc &desc,
                              uint32_t *&digits)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (OB_SUCCESS == ret) {
    ret = decode_i32(buf, buf_len, tmp_pos, (int32_t *)(&desc.desc_));
  }
  if (OB_SUCCESS == ret) {
    if (0 == desc.len_) {
      digits = NULL;
    } else if (buf_len < (tmp_pos + (int64_t)sizeof(uint32_t) * desc.len_)) {
      ret = OB_DESERIALIZE_ERROR;
    } else {
      digits = (uint32_t *)(buf + tmp_pos);
      tmp_pos += (sizeof(uint32_t) * desc.len_);
    }
  }
  if (OB_SUCCESS == ret) {
    pos = tmp_pos;
  }
  return ret;
}

inline int encode_number_type(char *buf,
                              const int64_t buf_len,
                              int64_t &pos,
                              const ObNumberDesc &desc,
                              const uint32_t *digits)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  //if (OB_SUCCESS == ret) {
  //  int8_t first_byte = OB_NUMBER_TYPE;
  //  ret = encode_i8(buf, buf_len, tmp_pos, first_byte);
  //}
  if (OB_SUCCESS == ret) {
    ret = encode_i32(buf, buf_len, tmp_pos, (int32_t)desc.desc_);
  }
  if (OB_SUCCESS == ret) {
    if (buf_len < (tmp_pos + (int64_t)sizeof(uint32_t) * desc.len_)) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      if (0 < desc.len_) {
        MEMCPY(buf + tmp_pos, digits, sizeof(uint32_t) * desc.len_);
        tmp_pos += (sizeof(uint32_t) * desc.len_);
      }
    }
  }
  if (OB_SUCCESS == ret) {
    pos = tmp_pos;
  }
  return ret;
}

inline int64_t encode_length_otimestamp_tz_type()
{
  return (sizeof(int64_t) + sizeof(uint32_t));
}

inline int decode_otimestamp_tz_type(const char *buf, const int64_t buf_len, int64_t &pos,
    int64_t &time_us, uint32_t &tz_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(decode_i64(buf, buf_len, pos, &time_us))) {
  } else if (OB_FAIL(decode_i32(buf, buf_len, pos, (int32_t *)(&tz_ctx)))) {
  }
  return ret;
}

inline int encode_otimestamp_tz_type(char *buf, const int64_t buf_len, int64_t &pos,
    const int64_t time_us, const uint32_t tz_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_i64(buf, buf_len, pos, time_us))) {
  } else if (OB_FAIL(encode_i32(buf, buf_len, pos, tz_ctx))) {
  }
  return ret;
}

inline int64_t encode_length_otimestamp_type()
{
  return (sizeof(int64_t) + sizeof(uint16_t));
}

inline int decode_otimestamp_type(const char *buf, const int64_t buf_len, int64_t &pos,
    int64_t &time_us, uint16_t &time_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(decode_i64(buf, buf_len, pos, &time_us))) {
  } else if (OB_FAIL(decode_i16(buf, buf_len, pos, (int16_t *)(&time_desc)))) {
  }
  return ret;
}

inline int encode_otimestamp_type(char *buf, const int64_t buf_len, int64_t &pos,
    const int64_t time_us, const uint16_t time_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_i64(buf, buf_len, pos, time_us))) {
  } else if (OB_FAIL(encode_i16(buf, buf_len, pos, time_desc))) {
  }
  return ret;
}


inline int encode_decimal_type(char *buf, const int64_t buf_len, int64_t &pos, bool is_add,
                               int8_t precision, int8_t scale, int8_t vscale, int8_t nwords, const uint32_t *words)
{
  int ret = OB_SUCCESS;
  if (buf == NULL || buf_len <= 0 || pos >= buf_len || NULL == words) {
    ret = OB_INVALID_ARGUMENT;
  }
  int8_t first_byte = OB_DECIMAL_TYPE;
  if (OB_SUCCESS == ret) {
    ret = encode_i8(buf, buf_len, pos, first_byte);
  }
  if (OB_SUCCESS == ret) {
    int8_t op = is_add ? 1 : 0;
    ret = encode_i8(buf, buf_len, pos, op);
  }
  if (OB_SUCCESS == ret) {
    ret = encode_i8(buf, buf_len, pos, precision);
  }
  if (OB_SUCCESS == ret) {
    ret = encode_i8(buf, buf_len, pos, scale);
  }
  if (OB_SUCCESS == ret) {
    ret = encode_i8(buf, buf_len, pos, vscale);
  }
  if (OB_SUCCESS == ret) {
    ret = encode_i8(buf, buf_len, pos, nwords);
  }
  if (OB_SUCCESS == ret) {
    for (int8_t i = 0; i < nwords; ++i) {
      if (OB_SUCCESS != (ret = encode_vi32(buf, buf_len, pos, static_cast<int32_t>(words[i])))) {
        break;
      }
    }
  }
  if (OB_SUCCESS != ret) {
    _OB_LOG(WARN, "fail to encode decimal. ret = %d", ret);
  }
  return ret;
}

int decode_decimal_type(const char *buf, const int64_t buf_len, int64_t &pos, bool &is_add,
                               int8_t &precision, int8_t &scale, int8_t &vscale, int8_t &nwords, uint32_t *words);

inline int64_t encoded_length_decimal_type(int8_t nwords, const uint32_t *words)
{
  int64_t len = 6;
  if (OB_UNLIKELY(NULL == words)) {
    _OB_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "null decimal words");
  } else {
    for (int8_t i = 0; i < nwords; ++i) {
      len += static_cast<int32_t>(encoded_length_vi32(words[i]));
    }
  }
  return len;
}

inline int encode_double_type(char *buf, const int64_t buf_len, int64_t &pos, double val,
                              const bool is_add)
{
  int64_t tmp = 0;
  MEMCPY(&tmp, &val, sizeof(tmp));

  int ret = OB_SIZE_OVERFLOW;

  int8_t first_byte = OB_DOUBLE_TYPE;

  if (is_add) {
    set_bit(first_byte, OB_FLOAT_OPERATION_BIT_POS);
  }

  if (OB_SUCCESS != (ret = encode_i8(buf, buf_len, pos, first_byte))) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    ret = encode_i64(buf, buf_len, pos, tmp);
  }

  return ret;
}

inline int decode_double_type(const char *buf, const int64_t data_len, int8_t first_byte,
                              int64_t &pos, double &val, bool &is_add)
{
  int ret = OB_DESERIALIZE_ERROR;
  if (NULL == buf || data_len - pos < static_cast<int64_t>(sizeof(double))) {
    ret = OB_DESERIALIZE_ERROR;
  } else {
    int64_t tmp = 0;
    if (OB_SUCCESS != (ret = decode_i64(buf, data_len, pos, &tmp))) {
      ret = OB_DESERIALIZE_ERROR;
    } else {
      MEMCPY(&val, &tmp, sizeof(val));
      is_add = test_bit(first_byte, OB_FLOAT_OPERATION_BIT_POS);
    }
  }
  return ret;
}

inline int64_t encoded_length_datetime(int64_t value)
{
  int64_t len = 0;
  uint64_t tmp_value = safe_int64_abs(value);
  if (tmp_value <= OB_MAX_4B) {
    len = 5;
  } else if (tmp_value <= OB_MAX_6B) {
    len = 7;
  } else {
    len = 9;
  }
  return len;
}

inline int64_t encoded_length_precise_datetime(int64_t value)
{
  return encoded_length_datetime(value);
}

inline int __encode_time_type(char *buf, const int64_t buf_len, int8_t first_byte, int64_t &pos,
                              int64_t val)
{
  int64_t len = encoded_length_datetime(val);
  int64_t tmp_pos = pos;
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len - pos < len) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    if (7 == len) {
      first_byte |= 1;
    } else if (9 == len) {
      first_byte |= 2;
    }

    if (OB_SUCCESS == (ret = encode_i8(buf, buf_len, tmp_pos, first_byte))) {
      uint64_t __v = safe_int64_abs(val);
      ret = __encode_uint_with_given_length(buf, buf_len, tmp_pos, __v, len - 1);
    }
    if (OB_SUCC(ret)) {
      pos = tmp_pos;
    }
  }
  return ret;
}

inline int __decode_time_type(const char *buf, const int64_t data_len, int8_t first_byte,
                              int64_t &pos, int64_t &val)
{
  int ret = OB_DESERIALIZE_ERROR;
  if ((NULL == buf) || (data_len - pos < 0)) {
    ret = OB_DESERIALIZE_ERROR;
  } else {
    int8_t len_mark = first_byte & OB_DATETIME_LEN_MASK;
    int64_t len = 0;
    if (0 == len_mark) {
      len = 4;
    } else if (1 == len_mark) {
      len = 6;
    } else if (2 == len_mark) {
      len = 8;
    }

    if (0 == len) {
      ret = OB_DESERIALIZE_ERROR;
    } else {
      uint64_t uv = 0;
      ret = __decode_uint_with_given_length(buf, data_len, pos, uv, len);
      if (OB_SUCC(ret)) {
        val = static_cast<int64_t>(uv);
      }
    }
  }
  return ret;
}

inline int encode_datetime_type(char *buf, const int64_t buf_len, int64_t &pos,
                                const ObDateTime &val, const bool is_add)
{
  int ret = OB_SIZE_OVERFLOW;
  if (NULL == buf || buf_len - pos <= 0) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    int8_t first_byte = OB_DATETIME_TYPE;

    if (val < 0) {
      set_bit(first_byte, OB_DATETIME_SIGN_BIT);
    }

    if (is_add) {
      set_bit(first_byte, OB_DATETIME_OPERATION_BIT);
    }

    ret = __encode_time_type(buf, buf_len, first_byte, pos, val);

  }
  return ret;
}

inline int decode_datetime_type(const char *buf, const int64_t data_len, int8_t first_byte,
                                int64_t &pos, int64_t &val, bool &is_add)
{
  int ret = OB_DESERIALIZE_ERROR;
  if ((NULL == buf) || (data_len - pos < 0)) {
    ret = OB_DESERIALIZE_ERROR;
  } else {
    bool is_neg = test_bit(first_byte, OB_DATETIME_SIGN_BIT);
    is_add = test_bit(first_byte, OB_DATETIME_OPERATION_BIT);

    int64_t __v = 0;

    if (OB_SUCCESS == (ret = __decode_time_type(buf, data_len, first_byte, pos, __v))) {
      val = is_neg ? -__v : __v;
    }
  }
  return ret;
}

inline int encode_precise_datetime_type(char *buf, const int64_t buf_len, int64_t &pos,
                                        const ObPreciseDateTime &val, const bool is_add)
{
  int ret = OB_SIZE_OVERFLOW;
  if (NULL == buf || buf_len - pos <= 0) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    int8_t first_byte = OB_PRECISE_DATETIME_TYPE;

    if (val < 0) {
      set_bit(first_byte, OB_DATETIME_SIGN_BIT);
    }

    if (is_add) {
      set_bit(first_byte, OB_DATETIME_OPERATION_BIT);
    }

    ret = __encode_time_type(buf, buf_len, first_byte, pos, val);

  }
  return ret;
}

inline int decode_precise_datetime_type(const char *buf, const int64_t data_len, int8_t first_byte,
                                        int64_t &pos, int64_t &val, bool &is_add)
{
  int ret = OB_DESERIALIZE_ERROR;
  if ((NULL == buf) || (data_len - pos < 0)) {
    ret = OB_DESERIALIZE_ERROR;
  } else {
    bool is_neg = test_bit(first_byte, OB_DATETIME_SIGN_BIT);
    is_add = test_bit(first_byte, OB_DATETIME_OPERATION_BIT);

    int64_t __v = 0;

    if (OB_SUCCESS == (ret = __decode_time_type(buf, data_len, first_byte, pos, __v))) {
      val = is_neg ? -__v : __v;
    }
  }
  return ret;
}

inline int64_t encoded_length_modifytime(int64_t value)
{
  return encoded_length_datetime(value);
}

inline int encode_modifytime_type(char *buf, const int64_t buf_len, int64_t &pos,
                                  const ObModifyTime &val)
{
  int ret = OB_SIZE_OVERFLOW;
  if (NULL == buf || buf_len - pos <= 0 || val < 0) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    int8_t first_byte = OB_MODIFYTIME_TYPE;
    ret = __encode_time_type(buf, buf_len, first_byte, pos, val);
  }
  return ret;
}

inline int decode_modifytime_type(const char *buf, const int64_t data_len, int8_t first_byte,
                                  int64_t &pos, int64_t &val)
{
  int ret = OB_DESERIALIZE_ERROR;
  if ((NULL == buf) || (data_len - pos <= 0)) {
    ret = OB_DESERIALIZE_ERROR;
  } else {
    ret = __decode_time_type(buf, data_len, first_byte, pos, val);
  }
  return ret;
}

inline int64_t encoded_length_createtime(const int64_t value)
{
  return encoded_length_datetime(value);
}

inline int encode_createtime_type(char *buf, const int64_t buf_len, int64_t &pos,
                                  const ObCreateTime &val)
{
  int ret = OB_SIZE_OVERFLOW;
  if (NULL == buf || buf_len - pos <= 0 || val < 0) {
    ret = OB_SIZE_OVERFLOW;
    _OB_LOG(WARN, "fail to encode_createtime_type: buf[%p], buf_len[%ld], pos[%ld] val[%ld]",
              buf, buf_len, pos, val);
  } else {
    int8_t first_byte = OB_CREATETIME_TYPE;
    ret = __encode_time_type(buf, buf_len, first_byte, pos, val);
    if (OB_FAIL(ret)) {
      _OB_LOG(WARN, "fail to __encode_time_type: ret = %d", ret);
    }
  }
  return ret;
}

inline int decode_createtime_type(const char *buf, const int64_t data_len, int8_t first_byte,
                                  int64_t &pos, int64_t &val)
{
  int ret = OB_DESERIALIZE_ERROR;
  if ((NULL == buf) || (data_len - pos <= 0)) {
    ret = OB_DESERIALIZE_ERROR;
  } else {
    ret = __decode_time_type(buf, data_len, first_byte, pos, val);
  }
  return ret;
}

inline int64_t encoded_length_extend(const int64_t value)
{
  return encoded_length_vi64(value) + 1;
}

inline int encode_extend_type(char *buf, const int64_t buf_len, int64_t &pos, const int64_t val)
{
  int ret = OB_SUCCESS;
  int64_t len = encoded_length_extend(val);
  if (NULL == buf || buf_len < 0 || (buf_len - pos < len)) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    int64_t tmp_pos = pos;
    if (OB_SUCCESS == (ret = encode_i8(buf, buf_len, tmp_pos, OB_EXTEND_TYPE))) {
      ret = encode_vi64(buf, buf_len, tmp_pos, val);
    }

    if (OB_SUCC(ret)) {
      pos = tmp_pos;
    }
  }
  return ret;
}

template<bool is_enum, typename T>
struct EnumEncoder { };

template<typename T>
struct EnumEncoder<true, T>
{
  static int encode(char *buf, const int64_t buf_len, int64_t &pos, const T &val)
  {
    return encode_vi32(buf, buf_len, pos, static_cast<int32_t>(val));
  }

  static int decode(const char *buf, const int64_t data_len, int64_t &pos, T &val)
  {
    int32_t ival = 0;
    int ret = OB_SUCCESS;

    ret = decode_vi32(buf, data_len, pos, &ival);
    val = static_cast<T>(ival);
    return ret;
  }

  static int64_t encoded_length(const T &val)
  {
    return encoded_length_vi32(static_cast<int32_t>(val));
  }
};

template<typename T>
struct EnumEncoder<false, T>
{
  static int encode(char *buf, const int64_t buf_len, int64_t &pos, const T &val)
  {
    return val.serialize(buf, buf_len, pos);
  }

  static int decode(const char *buf, const int64_t data_len, int64_t &pos, T &val)
  {
    return val.deserialize(buf, data_len, pos);
  }

  static int64_t encoded_length(const T &val)
  {
    return val.get_serialize_size();
  }
};

////////////////////////////////////////////////////////////////
template <typename T>
int encode(char *buf, const int64_t buf_len, int64_t &pos, const T &val)
{
  return EnumEncoder<__is_enum(T), T>::encode(buf, buf_len, pos, val);
}

inline int encode(char *buf, const int64_t buf_len, int64_t &pos, int64_t val)
{
  return encode_vi64(buf, buf_len, pos, val);
}
inline int encode(char *buf, const int64_t buf_len, int64_t &pos, uint64_t val)
{
  return encode_vi64(buf, buf_len, pos, static_cast<int64_t>(val));
}
inline int encode(char *buf, const int64_t buf_len, int64_t &pos, int32_t val)
{
  return encode_vi32(buf, buf_len, pos, val);
}
inline int encode(char *buf, const int64_t buf_len, int64_t &pos, uint32_t val)
{
  return encode_vi32(buf, buf_len, pos, static_cast<int32_t>(val));
}
inline int encode(char *buf, const int64_t buf_len, int64_t &pos, int16_t val)
{
  return encode_vi32(buf, buf_len, pos, static_cast<int32_t>(val));
}
inline int encode(char *buf, const int64_t buf_len, int64_t &pos, uint16_t val)
{
  return encode_vi32(buf, buf_len, pos, static_cast<int32_t>(val));
}
inline int encode(char *buf, const int64_t buf_len, int64_t &pos, int8_t val)
{
  return encode_i8(buf, buf_len, pos, val);
}
inline int encode(char *buf, const int64_t buf_len, int64_t &pos, uint8_t val)
{
  return encode_i8(buf, buf_len, pos, static_cast<int8_t>(val));
}
inline int encode(char *buf, const int64_t buf_len, int64_t &pos, char val)
{
  return encode_i8(buf, buf_len, pos, static_cast<int8_t>(val));
}
inline int encode(char *buf, const int64_t buf_len, int64_t &pos, bool val)
{
  return encode_bool(buf, buf_len, pos, val);
}
inline int encode(char *buf, const int64_t buf_len, int64_t &pos, float val)
{
  return encode_float(buf, buf_len, pos, val);
}
inline int encode(char *buf, const int64_t buf_len, int64_t &pos, double val)
{
  return encode_double(buf, buf_len, pos, val);
}

// encode null terminated char array
template <int64_t CNT>
int encode(char *buf, const int64_t buf_len, int64_t &pos, const char (&val)[CNT])
{
  int ret = OB_SUCCESS;
  size_t size = strlen(val) + 1;

  if (OB_FAIL(encode_vi64(buf, buf_len, pos, size))) {
  } else {
    if (buf_len - pos >= static_cast<int64_t>(size)) {
      MEMCPY(buf + pos, val, size);
      pos += size;
    } else {
      ret = OB_BUF_NOT_ENOUGH;
    }
  }
  return ret;
}

template <class T, int64_t CNT>
int encode(char *buf, const int64_t buf_len, int64_t &pos, const T (&val)[CNT])
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(encode_vi64(buf, buf_len, pos, CNT))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < CNT; i++) {
      ret = encode(buf, buf_len, pos, val[i]);
    }
  }
  return ret;
}

template <typename T1, typename T2>
int encode(char *buf, const int64_t buf_len, int64_t &pos, const std::pair<T1, T2> &kv_pair)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode(buf, buf_len, pos, kv_pair.first))) {
  } else if (OB_FAIL(encode(buf, buf_len, pos, kv_pair.second))) {
  }
  return ret;
}

////////////////
template<typename T>
int decode(const char *buf, const int64_t data_len, int64_t &pos, T &val)
{
  return EnumEncoder<__is_enum(T), T>::decode(buf, data_len, pos, val);
}

inline int decode(const char *buf, const int64_t data_len, int64_t &pos, int8_t &val)
{
  val = 0;
  return decode_i8(buf, data_len, pos, &val);
}
inline int decode(const char *buf, const int64_t data_len, int64_t &pos, uint8_t &val)
{
  val = 0;
  return decode_i8(buf, data_len, pos, reinterpret_cast<int8_t*>(&val));
}
inline int decode(const char *buf, const int64_t data_len, int64_t &pos, char &val)
{
  val = 0;
  return decode_i8(buf, data_len, pos, reinterpret_cast<int8_t*>(&val));
}
inline int decode(const char *buf, const int64_t data_len, int64_t &pos, int16_t &val)
{
  int32_t v = 0;
  int ret = decode_vi32(buf, data_len, pos, &v);
  val = static_cast<int16_t>(v);
  return ret;
}
inline int decode(const char *buf, const int64_t data_len, int64_t &pos, uint16_t &val)
{
  int32_t v = 0;
  int ret = decode_vi32(buf, data_len, pos, &v);
  val = static_cast<uint16_t>(v);
  return ret;
}
inline int decode(const char *buf, const int64_t data_len, int64_t &pos, int32_t &val)
{
  val = 0;
  return decode_vi32(buf, data_len, pos, &val);
}
inline int decode(const char *buf, const int64_t data_len, int64_t &pos, uint32_t &val)
{
  val = 0;
  return decode_vi32(buf, data_len, pos, reinterpret_cast<int32_t *>(&val));
}
inline int decode(const char *buf, const int64_t data_len, int64_t &pos, int64_t &val)
{
  val = 0;
  return decode_vi64(buf, data_len, pos, &val);
}
inline int decode(const char *buf, const int64_t data_len, int64_t &pos, uint64_t &val)
{
  val = 0;
  return decode_vi64(buf, data_len, pos, reinterpret_cast<int64_t *>(&val));
}
inline int decode(const char *buf, const int64_t data_len, int64_t &pos, bool &val)
{
  val = 0;
  return decode_bool(buf, data_len, pos, &val);
}
inline int decode(const char *buf, const int64_t data_len, int64_t &pos, float &val)
{
  val = 0;
  return decode_float(buf, data_len, pos, &val);
}
inline int decode(const char *buf, const int64_t data_len, int64_t &pos, double &val)
{
  val = 0.0;
  return decode_double(buf, data_len, pos, &val);
}

// decode null terminated char array
template<int64_t CNT>
int decode(const char *buf, const int64_t data_len, int64_t &pos, char (&val)[CNT])
{
  int ret = OB_SUCCESS;
  int64_t rsize = 0;

  if (OB_FAIL(decode(buf, data_len, pos, rsize))) {
  } else {
    if (CNT >= rsize && data_len - pos >= rsize) {
      MEMCPY(val, buf + pos, rsize);
      pos += rsize;
    } else {
      ret = OB_DESERIALIZE_ERROR;
    }
  }
  return ret;
}

template<class T, int64_t CNT>
int decode(const char *buf, const int64_t data_len, int64_t &pos, T (&val)[CNT])
{
  int ret = OB_SUCCESS;
  int64_t rsize = 0;

  if (OB_FAIL(decode_vi64(buf, data_len, pos, &rsize))) {
  } else if (CNT != rsize) {
    ret = OB_DESERIALIZE_ERROR;
    LIB_LOG(ERROR, "array size isn't same", K(ret), K(rsize), "CNT", CNT);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < CNT; i++) {
      ret = decode(buf, data_len, pos, val[i]);
    }
  }
  return ret;
}

template <typename T1, typename T2>
int decode(const char *buf, const int64_t data_len, int64_t &pos, std::pair<T1, T2> &kv_pair)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(decode(buf, data_len, pos, kv_pair.first))) {
  } else if (OB_FAIL(decode(buf, data_len, pos, kv_pair.second))) {
  }
  return ret;
}
////////////////
template <typename T>
int64_t encoded_length(const T &val)
{
  return EnumEncoder<__is_enum(T), T>::encoded_length(val);
}

inline int64_t encoded_length(int64_t val)
{
  return encoded_length_vi64(val);
}
inline int64_t encoded_length(uint64_t val)
{
  return encoded_length_vi64(static_cast<int64_t>(val));
}
inline int64_t encoded_length(int32_t val)
{
  return encoded_length_vi32(val);
}
inline int64_t encoded_length(uint32_t val)
{
  return encoded_length_vi32(static_cast<int32_t>(val));
}
inline int64_t encoded_length(int16_t val)
{
  return encoded_length_vi32(val);
}
inline int64_t encoded_length(uint16_t val)
{
  return encoded_length_vi32(static_cast<int32_t>(val));
}
inline int64_t encoded_length(int8_t)
{
  return 1;
}
inline int64_t encoded_length(uint8_t)
{
  return 1;
}
inline int64_t encoded_length(char)
{
  return 1;
}
inline int64_t encoded_length(bool val)
{
  return encoded_length_bool(val);
}
inline int64_t encoded_length(float val)
{
  return encoded_length_float(val);
}
inline int64_t encoded_length(double val)
{
  return encoded_length_double(val);
}

// encode null terminated char array
template <int64_t CNT>
int64_t encoded_length(const char (&val)[CNT])
{
  size_t size = strlen(val) + 1;
  size += encoded_length_vi64(size);
  return size;
}

template <class T, int64_t CNT>
int64_t encoded_length(const T (&val)[CNT])
{
  int64_t size = encoded_length_vi64(CNT);
  for (int64_t i = 0; i < CNT; i++) {
    size += encoded_length(val[i]);
  }
  return size;
}

template <typename T1, typename T2>
int64_t encoded_length(const std::pair<T1, T2> &kv_pair)
{
  int64_t len = 0;
  len += encoded_length(kv_pair.first);
  len += encoded_length(kv_pair.second);
  return len;
}

} /* serialization */
} /* common */
} /* oceanbase*/

#endif //OCEANBASE_COMMON_SERIALIZATION_H
