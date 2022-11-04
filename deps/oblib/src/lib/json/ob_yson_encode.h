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

#ifndef _OB_YSON_ENCODE_H
#define _OB_YSON_ENCODE_H 1

#include "lib/string/ob_string.h"          // for ObString
#include "lib/utility/ob_macro_utils.h"  // for LST_DO_
#include "lib/utility/ob_template_utils.h"  // for BoolType<>
#include "lib/container/ob_array_wrap.h"    // for ObArrayWrap, ObIArrayWrap
#include "lib/profile/ob_trace_id_adaptor.h" // for ObTraceIdAdaptor
#include <utility>                       // for std::pair

namespace oceanbase
{
namespace yson
{
using common::OB_SUCCESS;
using common::OB_INVALID_DATA;
using common::OB_BUF_NOT_ENOUGH;
using common::OB_INVALID_ARGUMENT;

static const uint8_t YSON_TYPE_INT32 = 0x01;
static const uint8_t YSON_TYPE_INT64 = 0x02;
static const uint8_t YSON_TYPE_STRING = 0x03;
static const uint8_t YSON_TYPE_OBJECT = 0x04;
static const uint8_t YSON_TYPE_BOOLEAN = 0x05;
static const uint8_t YSON_TYPE_UINT32 = 0x06;
static const uint8_t YSON_TYPE_UINT64 = 0x07;
static const uint8_t YSON_TYPE_ARRAY = 0x08;
static const uint8_t YSON_TYPE_INT8 = 0x09;
static const uint8_t YSON_TYPE_POINTER = 0xa;
static const uint8_t YSON_TYPE_TRACE_ID = 0x10;

typedef uint16_t ElementKeyType;
////////////////////////////////////////////////////////////////
// utility macros
#define YSON_ELEMENT_TYPE_LEN static_cast<int64_t>(sizeof(uint8_t))
#define YSON_KEY_LEN static_cast<int64_t>(sizeof(ElementKeyType))
#define YSON_LEAST_OBJECT_LEN static_cast<int64_t>(sizeof(int32_t))
#define YSON_INT32_LEN static_cast<int64_t>(sizeof(int32_t))
#define YSON_INT64_LEN static_cast<int64_t>(sizeof(int64_t))
#define YSON_LEAST_STRING_LEN static_cast<int64_t>(sizeof(int32_t))
#define YSON_BOOLEAN_LEN static_cast<int64_t>(sizeof(bool))
#define YSON_INT8_LEN static_cast<int8_t>(sizeof(int8_t))
#define YSON_POINTER_LEN static_cast<int8_t>(sizeof(void*))
// encoder
template<class T>
    inline int databuff_encode_element(char *buf, const int64_t buf_len, int64_t &pos, ElementKeyType key, const T &obj, common::TrueType);
template<class T>
    inline int databuff_encode_element(char *buf, const int64_t buf_len, int64_t &pos, ElementKeyType key, const T &obj, common::FalseType);

template<class T>
    inline int databuff_encode_element(char *buf, const int64_t buf_len, int64_t &pos, ElementKeyType key, const T &obj)
{
  return databuff_encode_element(buf, buf_len, pos, key, obj, common::BoolType<__is_enum(T)>());
}

template<class T>
    inline int databuff_encode_element(char *buf, const int64_t buf_len, int64_t &pos, ElementKeyType key, const T &obj, common::TrueType)
{
  int64_t enum_val = static_cast<int64_t>(obj);
  return databuff_encode_element(buf, buf_len, pos, key, enum_val);
}

template<class T>
    inline int databuff_encode_element(char *buf, const int64_t buf_len, int64_t &pos, ElementKeyType key, const T &obj, common::FalseType)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len-pos < YSON_ELEMENT_TYPE_LEN + YSON_KEY_LEN + YSON_LEAST_OBJECT_LEN)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    *((uint8_t*)(buf+pos)) = YSON_TYPE_OBJECT;
    pos++;
    *((ElementKeyType*)(buf+pos)) = key;
    pos+=sizeof(key);
    int32_t obj_size_pos = static_cast<int32_t>(pos);
    pos+=sizeof(int32_t);
    ret = obj.to_yson(buf, buf_len, pos);
    if (OB_SUCC(ret)) {
      *((int32_t*)(buf+obj_size_pos)) = static_cast<int32_t>((pos-(obj_size_pos+sizeof(int32_t))));
    } else {
      pos = obj_size_pos - 1 - sizeof(key);  // revert pos for safety
    }
  }
  return ret;
}
template<typename T1, typename T2>
    inline int databuff_encode_element(char *buf, const int64_t buf_len, int64_t &pos, ElementKeyType key, const std::pair<T1,T2> &obj);

template<>
    inline int databuff_encode_element(char *buf, const int64_t buf_len, int64_t &pos, ElementKeyType key, const int8_t &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len-pos < YSON_ELEMENT_TYPE_LEN + YSON_KEY_LEN + YSON_INT8_LEN)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    *((uint8_t*)(buf+pos)) = YSON_TYPE_INT8;
    pos++;
    *((ElementKeyType*)(buf+pos)) = key;
    pos+=sizeof(key);
    *((uint8_t*)(buf+pos)) = value;
    pos+=sizeof(value);
  }
  return ret;
}

template<>
    inline int databuff_encode_element(char *buf, const int64_t buf_len, int64_t &pos, ElementKeyType key, const bool &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len-pos < YSON_ELEMENT_TYPE_LEN + YSON_KEY_LEN + YSON_BOOLEAN_LEN)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    *((uint8_t*)(buf+pos)) = YSON_TYPE_BOOLEAN;
    pos++;
    *((ElementKeyType*)(buf+pos)) = key;
    pos+=sizeof(key);
    *((int8_t*)(buf+pos)) = value;
    pos+=sizeof(value);
  }
  return ret;
}

template<>
    inline int databuff_encode_element(char *buf, const int64_t buf_len, int64_t &pos, ElementKeyType key, const int32_t &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len-pos < YSON_ELEMENT_TYPE_LEN + YSON_KEY_LEN + YSON_INT32_LEN)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    *((uint8_t*)(buf+pos)) = YSON_TYPE_INT32;
    pos++;
    *((ElementKeyType*)(buf+pos)) = key;
    pos+=sizeof(key);
    *((int32_t*)(buf+pos)) = value;
    pos+=sizeof(value);
  }
  return ret;
}

template<>
    inline int databuff_encode_element(char *buf, const int64_t buf_len, int64_t &pos, ElementKeyType key, const int64_t &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len-pos < YSON_ELEMENT_TYPE_LEN + YSON_KEY_LEN + YSON_INT64_LEN)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    *((uint8_t*)(buf+pos)) = YSON_TYPE_INT64;
    pos++;
    *((ElementKeyType*)(buf+pos)) = key;
    pos+=sizeof(key);
    *((int64_t*)(buf+pos)) = value;
    pos+=sizeof(value);
  }
  return ret;
}

template<>
    inline int databuff_encode_element(char *buf, const int64_t buf_len, int64_t &pos, ElementKeyType key, void *const &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len-pos < YSON_ELEMENT_TYPE_LEN + YSON_KEY_LEN + YSON_POINTER_LEN)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    *((uint8_t*)(buf+pos)) = YSON_TYPE_POINTER;
    pos++;
    *((ElementKeyType*)(buf+pos)) = key;
    pos+=sizeof(key);
    *((int64_t*)(buf+pos)) = (int64_t)value;
    pos+=sizeof(value);
  }
  return ret;
}

template<>
    inline int databuff_encode_element(char *buf, const int64_t buf_len, int64_t &pos, ElementKeyType key, const common::ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(value.length() < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(buf_len-pos < YSON_ELEMENT_TYPE_LEN + YSON_KEY_LEN + YSON_LEAST_STRING_LEN + value.length())) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    *((int8_t*)(buf+pos)) = YSON_TYPE_STRING;
    pos++;
    *((ElementKeyType*)(buf+pos)) = key;
    pos+=sizeof(key);
    *((int32_t*)(buf+pos)) = value.length();
    pos+=sizeof(int32_t);
    memcpy(buf+pos, value.ptr(), value.length());
    pos+=value.length();
  }
  return ret;
}

template<>
    inline int databuff_encode_element(char *buf, const int64_t buf_len, int64_t &pos, ElementKeyType key, const char * const &value)
{
  common::ObString str = common::ObString::make_string(value);
  return databuff_encode_element(buf, buf_len, pos, key, str);
}

template<>
    inline int databuff_encode_element(char *buf, const int64_t buf_len, int64_t &pos, ElementKeyType key, const uint32_t &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len-pos < YSON_ELEMENT_TYPE_LEN + YSON_KEY_LEN + YSON_INT32_LEN)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    *((uint8_t*)(buf+pos)) = YSON_TYPE_UINT32;
    pos++;
    *((ElementKeyType*)(buf+pos)) = key;
    pos+=sizeof(key);
    *((uint32_t*)(buf+pos)) = value;
    pos+=sizeof(value);
  }
  return ret;
}

template<>
    inline int databuff_encode_element(char *buf, const int64_t buf_len, int64_t &pos, ElementKeyType key, const uint64_t &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len-pos < YSON_ELEMENT_TYPE_LEN + YSON_KEY_LEN + YSON_INT64_LEN)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    *((uint8_t*)(buf+pos)) = YSON_TYPE_UINT64;
    pos++;
    *((ElementKeyType*)(buf+pos)) = key;
    pos+=sizeof(key);
    *((uint64_t*)(buf+pos)) = value;
    pos+=sizeof(value);
  }
  return ret;
}

template<class T>
    inline int databuff_encode_array_element(char *buf, const int64_t buf_len, int64_t &pos, ElementKeyType key, const common::ObIArrayWrap<T> &array);

template<class T>
    inline int databuff_encode_element(char *buf, const int64_t buf_len, int64_t &pos, ElementKeyType key, const common::ObArrayWrap<T> &obj)
{
  return databuff_encode_array_element(buf, buf_len, pos, key, obj);
}

template<>
    inline int databuff_encode_element(char *buf, const int64_t buf_len, int64_t &pos, ElementKeyType key, const common::ObTraceIdAdaptor &value)
{
  int ret = OB_SUCCESS;
  const int64_t TRACE_ID_LENGTH = sizeof(value);
  if (OB_UNLIKELY(buf_len-pos < YSON_ELEMENT_TYPE_LEN + YSON_KEY_LEN + TRACE_ID_LENGTH)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    *((int8_t*)(buf+pos)) = YSON_TYPE_TRACE_ID;
    pos++;
    *((ElementKeyType*)(buf+pos)) = key;
    pos += sizeof(key);
    *((uint64_t*)(buf+pos)) = value.get()[0];
    pos += sizeof(uint64_t);
    *((uint64_t*)(buf+pos)) = value.get()[1];
    pos += sizeof(uint64_t);
    *((uint64_t*)(buf+pos)) = value.get()[2];
    pos += sizeof(uint64_t);
    *((uint64_t*)(buf+pos)) = value.get()[3];
    pos += sizeof(uint64_t);
  }
  return ret;
}

////////////////////////////////////////////////////////////////
// decoder
template<class T>
    inline int databuff_decode_element_value(const char *buf, const int64_t buf_len, int64_t &pos, T &obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len-pos < YSON_LEAST_OBJECT_LEN)) {
    ret = OB_INVALID_DATA;
  } else {
    int32_t obj_len = *((int32_t*)(buf+pos));
    pos+=sizeof(obj_len);
    if (buf_len-pos < obj_len) {
      ret = OB_INVALID_DATA;
    } else {
      ret = obj.from_yson(buf, buf_len, pos);
    }
  }
  return ret;
}
template<>
    inline int databuff_decode_element_value(const char *buf, const int64_t buf_len, int64_t &pos, int64_t &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len-pos < YSON_INT64_LEN)) {
    ret = OB_INVALID_DATA;
  } else {
    value = *((int64_t*)(buf+pos));
    pos+=sizeof(value);
  }
  return ret;
}
template<>
    inline int databuff_decode_element_value(const char *buf, const int64_t buf_len, int64_t &pos, int32_t &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len-pos < YSON_INT32_LEN)) {
    ret = OB_INVALID_DATA;
  } else {
    value = *((int32_t*)(buf+pos));
    pos+=sizeof(value);
  }
  return ret;
}
template<>
inline int databuff_decode_element_value(const char *buf, const int64_t buf_len, int64_t &pos, void *&value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len-pos < YSON_POINTER_LEN)) {
    ret = OB_INVALID_DATA;
  } else {
    value = (void*)*((int64_t*)(buf+pos));
    pos+=sizeof(value);
  }
  return ret;
}
template<>
    inline int databuff_decode_element_value(const char *buf, const int64_t buf_len, int64_t &pos, bool &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len-pos < YSON_BOOLEAN_LEN)) {
    ret = OB_INVALID_DATA;
  } else {
    value = *((int8_t*)(buf+pos));
    pos+=sizeof(value);
  }
  return ret;
}
// shallow copy
template<>
    inline int databuff_decode_element_value(const char *buf, const int64_t buf_len, int64_t &pos, common::ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len-pos < YSON_LEAST_STRING_LEN)) {
    ret = OB_INVALID_DATA;
  } else {
    int32_t str_len = *((int32_t*)(buf+pos));
    pos+=sizeof(str_len);
    if (buf_len-pos < str_len) {
      ret = OB_INVALID_DATA;
    } else {
      value.assign_ptr((char*)buf+pos, str_len);
      pos += str_len;
    }
  }
  return ret;
}
template<>
    inline int databuff_decode_element_value(const char *buf, const int64_t buf_len, int64_t &pos, uint64_t &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len-pos < YSON_INT64_LEN)) {
    ret = OB_INVALID_DATA;
  } else {
    value = *((uint64_t*)(buf+pos));
    pos+=sizeof(value);
  }
  return ret;
}
template<>
    inline int databuff_decode_element_value(const char *buf, const int64_t buf_len, int64_t &pos, uint32_t &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len-pos < YSON_INT32_LEN)) {
    ret = OB_INVALID_DATA;
  } else {
    value = *((uint32_t*)(buf+pos));
    pos+=sizeof(value);
  }
  return ret;
}
template<>
    inline int databuff_decode_element_value(const char *buf, const int64_t buf_len, int64_t &pos, common::ObTraceIdAdaptor &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len-pos < sizeof(uint64_t) * 4)) {
    ret = OB_INVALID_DATA;
  } else {
    value.set(((uint64_t*)(buf+pos)));
    pos += sizeof(uint64_t) * 4;
  }
  return ret;
}

////////////////////////////////////////////////////////////////
// define template <...> databuff_encode_elements(buf, buf_len, pos, ...)
#define TO_YSON_TEMPLATE_TYPE(N) CAT(typename T, N)
#define TO_YSON_ARG_PAIR(N) ElementKeyType CAT(key, N), const CAT(T, N) &CAT(obj, N)
#define TO_YSON_ENCODE_ONE(N) if (OB_SUCC(ret)) {       \
    ret = databuff_encode_element(buf, buf_len, pos, CAT(key, N), CAT(obj,N)); \
  }

#define TO_YSON_ELEMENTS(N)                                             \
  template < LST_DO_(N, TO_YSON_TEMPLATE_TYPE, (,), PROC_ONE, ONE_TO_HUNDRED) > \
  int databuff_encode_elements(char *buf, const int64_t buf_len, int64_t& pos, \
                               LST_DO_(N, TO_YSON_ARG_PAIR, (,), PROC_ONE, ONE_TO_HUNDRED) \
                               )                                        \
  {                                                                     \
    int ret = OB_SUCCESS;                            \
    LST_DO_(N, TO_YSON_ENCODE_ONE, (), PROC_ONE, ONE_TO_HUNDRED); \
    return ret;                                                         \
  }

inline int databuff_encode_elements(char *buf, const int64_t buf_len, int64_t& pos)
{
    int ret = common::OB_SUCCESS;
    UNUSED(buf);
    UNUSED(buf_len);
    UNUSED(pos);
    return ret;
}


TO_YSON_ELEMENTS(1);
TO_YSON_ELEMENTS(2);
TO_YSON_ELEMENTS(3);
TO_YSON_ELEMENTS(4);
TO_YSON_ELEMENTS(5);
TO_YSON_ELEMENTS(6);
TO_YSON_ELEMENTS(7);
TO_YSON_ELEMENTS(8);
TO_YSON_ELEMENTS(9);
TO_YSON_ELEMENTS(10);
TO_YSON_ELEMENTS(11);
TO_YSON_ELEMENTS(12);
TO_YSON_ELEMENTS(13);
TO_YSON_ELEMENTS(14);
TO_YSON_ELEMENTS(15);
TO_YSON_ELEMENTS(16);
TO_YSON_ELEMENTS(17);
TO_YSON_ELEMENTS(18);
TO_YSON_ELEMENTS(19);
TO_YSON_ELEMENTS(20);
TO_YSON_ELEMENTS(21);
TO_YSON_ELEMENTS(22);
TO_YSON_ELEMENTS(23);
TO_YSON_ELEMENTS(24);
TO_YSON_ELEMENTS(25);
TO_YSON_ELEMENTS(26);
TO_YSON_ELEMENTS(27);
TO_YSON_ELEMENTS(28);
TO_YSON_ELEMENTS(29);
TO_YSON_ELEMENTS(30);
TO_YSON_ELEMENTS(31);
TO_YSON_ELEMENTS(32);

// special databuff_encode_element
template<typename T1, typename T2>
    inline int databuff_encode_element(char *buf, const int64_t buf_len, int64_t &pos, ElementKeyType key, const std::pair<T1,T2> &obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len-pos < YSON_ELEMENT_TYPE_LEN + YSON_KEY_LEN + YSON_LEAST_OBJECT_LEN)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    *((uint8_t*)(buf+pos)) = YSON_TYPE_OBJECT;
    pos++;
    *((ElementKeyType*)(buf+pos)) = key;
    pos+=sizeof(key);
    int32_t obj_size_pos = static_cast<int32_t>(pos);
    pos+=sizeof(int32_t);
    static ElementKeyType key_first = 2;  // @see ob_name_id_def.h
    static ElementKeyType key_second = 3;
    ret = oceanbase::yson::databuff_encode_elements(buf, buf_len, pos, key_first, obj.first,
      key_second, obj.second);
    if (OB_SUCC(ret)) {
      *((int32_t*)(buf+obj_size_pos)) = static_cast<int32_t>((pos-(obj_size_pos+sizeof(int32_t))));
    } else {
      pos = obj_size_pos - 1 - sizeof(key);  // revert pos for safety
    }
  }
  return ret;
}

template<class T>
    inline int databuff_encode_array_element(char *buf, const int64_t buf_len, int64_t &pos, ElementKeyType key, const common::ObIArrayWrap<T> &array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len-pos < YSON_ELEMENT_TYPE_LEN + YSON_KEY_LEN + YSON_LEAST_OBJECT_LEN)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    *((uint8_t*)(buf+pos)) = YSON_TYPE_ARRAY;
    pos++;
    *((ElementKeyType*)(buf+pos)) = key;
    pos+=sizeof(key);
    int32_t obj_size_pos = static_cast<int32_t>(pos);
    pos+=sizeof(int32_t);
    for (int64_t i = 0; OB_SUCC(ret) && i < array.count(); ++i) {
      ret = oceanbase::yson::databuff_encode_element(buf, buf_len, pos, static_cast<ElementKeyType>(i), array.at(i));
    } // end for
    if (OB_SUCC(ret)) {
      *((int32_t*)(buf+obj_size_pos)) = static_cast<int32_t>((pos-(obj_size_pos+sizeof(int32_t))));
    } else {
      pos = obj_size_pos - 1 - sizeof(key);  // revert pos for safety
    }
  }
  return ret;
}

} // end namespace yson
}  // oceanbase

#endif /* _OB_YSON_ENCODE_H */
