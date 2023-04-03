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

#ifndef _OB_SERIALIZATION_HELPER_H
#define _OB_SERIALIZATION_HELPER_H 1

#include "lib/utility/serialization.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_template_utils.h"

namespace oceanbase
{
namespace common
{

template <typename T>
struct DefaultItemEncode
{
  static int encode_item(char *buf, const int64_t buf_len, int64_t &pos, const T &item)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid args", KP(buf));
    } else {
      ret = encode_item_enum(buf, buf_len, pos, item, BoolType<__is_enum(T)>());
    }
    return ret;
  }
  static int decode_item(const char *buf, const int64_t data_len, int64_t &pos, T &item)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid args", KP(buf));
    } else {
      ret = decode_item_enum(buf, data_len, pos, item, BoolType<__is_enum(T)>());
    }
    return ret;
  }
  static int64_t encoded_length_item(const T &item)
  {
    return encoded_length_item_enum(item, BoolType<__is_enum(T)>());
  }

private:
  // for class, struct, union
  static int encode_item_enum(char *buf, const int64_t buf_len, int64_t &pos,
                              const T &item, FalseType)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid args", KP(buf));
    } else {
      ret = item.serialize(buf, buf_len, pos);
    }
    return ret;
  }
  static int decode_item_enum(const char *buf, const int64_t data_len, int64_t &pos,
                              T &item, FalseType)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid args", KP(buf));
    } else {
      ret = item.deserialize(buf, data_len, pos);
    }
    return ret;
  }
  static int64_t encoded_length_item_enum(const T &item, FalseType)
  {
    return item.get_serialize_size();
  }

  // for enum
  // only 4 byte enum supported.
  static int encode_item_enum(char *buf, const int64_t buf_len, int64_t &pos,
                              const T &item, TrueType)
  {
    int ret = OB_SUCCESS;
    STATIC_ASSERT(sizeof(int32_t) == sizeof(item), "type length mismatch");
    if (OB_ISNULL(buf)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid args", KP(buf));
    } else {
      ret = serialization::encode_vi32(buf, buf_len, pos, static_cast<int32_t>(item));
    }
    return ret;
  }
  static int decode_item_enum(const char *buf, const int64_t data_len, int64_t &pos,
                              T &item, TrueType)
  {
    int ret = OB_SUCCESS;
    int32_t v = 0;
    STATIC_ASSERT(sizeof(v) == sizeof(item), "type length mismatch");
    if (OB_ISNULL(buf)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid args", KP(buf));
    } else if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, &v))) {
      COMMON_LOG(WARN, "fail to decode_vi32", K(ret));
    } else {
      item = static_cast<T>(v);
    }
    return ret;
  }
  static int64_t encoded_length_item_enum(const T &item, TrueType)
  {
    STATIC_ASSERT(sizeof(int32_t) == sizeof(item), "type length mismatch");
    return serialization::encoded_length_vi32(static_cast<int32_t>(item));
  }
};

template<typename T>
int encode_item(char *buf, const int64_t buf_len, int64_t &pos, const T &item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid args", KP(buf));
  } else {
    ret = DefaultItemEncode<T>::encode_item(buf, buf_len, pos, item);
  }
  return ret;
}
template<typename T>
int decode_item(const char *buf, const int64_t data_len, int64_t &pos, T &item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid args", KP(buf));
  } else {
    ret = DefaultItemEncode<T>::decode_item(buf, data_len, pos, item);
  }
  return ret;
}
template<typename T>
int64_t encoded_length_item(const T &item)
{
  return DefaultItemEncode<T>::encoded_length_item(item);
}

#define DECLARE_ENCODE_ITEM(type)                                       \
  template<>                                                            \
  struct DefaultItemEncode<type>                                        \
  {                                                                     \
    static int encode_item(char* buf, const int64_t buf_len, int64_t& pos, const  type &item); \
    static int decode_item(const char* buf, const int64_t data_len, int64_t& pos, type &item); \
    static int64_t encoded_length_item(const type &item);               \
  };                                                                    \
  int encode_item(char* buf, const int64_t buf_len, int64_t& pos, const  type &item); \
  int decode_item(const char* buf, const int64_t data_len, int64_t& pos, type &item); \
  int64_t encoded_length_item(const type &item);                        \


DECLARE_ENCODE_ITEM(int64_t);
DECLARE_ENCODE_ITEM(uint64_t);
DECLARE_ENCODE_ITEM(int32_t);
DECLARE_ENCODE_ITEM(int16_t);
DECLARE_ENCODE_ITEM(int8_t);
DECLARE_ENCODE_ITEM(bool);
DECLARE_ENCODE_ITEM(double);

template<int64_t SIZE>
int encode_item(char *buf, const int64_t buf_len, int64_t &pos, const char(&item)[SIZE])
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid args", KP(buf));
  } else {
    ret = serialization::encode_vstr(buf, buf_len, pos, item);
  }
  return ret;
}
template<int64_t SIZE>
int decode_item(const char *buf, const int64_t data_len, int64_t &pos, char(&item)[SIZE])
{
  int ret = OB_SUCCESS;
  int64_t ret_str_length;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid args", KP(buf));
  } else {
    ret = serialization::decode_vstr(buf, data_len, pos, item, SIZE, &ret_str_length);
  }
  return ret;
}
template<int64_t SIZE>
int64_t encoded_length_item(const char(&item)[SIZE])
{
  return serialization::encoded_length(item);
}

}
}

#endif /* _OB_SERIALIZATION_HELPER_H */
