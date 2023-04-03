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

#include "lib/utility/ob_serialization_helper.h"

namespace oceanbase
{
namespace common
{

#define DEFINE_ENCODE_ITEM(type, snippet, type2) \
  int DefaultItemEncode<type>::encode_item(char* buf, const int64_t buf_len, int64_t& pos, const type &item) \
  { \
    int ret = OB_SUCCESS; \
    if (OB_ISNULL(buf)) { \
      ret = OB_INVALID_ARGUMENT; \
      COMMON_LOG(WARN, "invalid args", KP(buf)); \
    } else { \
      ret =  serialization::encode_##snippet (buf, buf_len, pos, item); \
    } \
    return ret; \
  } \
  \
  int DefaultItemEncode<type>::decode_item(const char* buf, const int64_t data_len, int64_t& pos, type &item) \
  { \
    int ret = OB_SUCCESS; \
    if (OB_ISNULL(buf)) { \
      ret = OB_INVALID_ARGUMENT; \
      COMMON_LOG(WARN, "invalid args", KP(buf)); \
    } else { \
      ret = serialization::decode_##snippet (buf, data_len, pos, reinterpret_cast<type2 *>(&item)); \
    } \
    return ret; \
  } \
  \
  int64_t DefaultItemEncode<type>::encoded_length_item(const type &item) \
  { \
    return serialization::encoded_length_##snippet (item); \
  } \
  int encode_item(char* buf, const int64_t buf_len, int64_t& pos, const type &item) \
  { \
    int ret = OB_SUCCESS; \
    if (OB_ISNULL(buf)) { \
      ret = OB_INVALID_ARGUMENT; \
      COMMON_LOG(WARN, "invalid args", KP(buf)); \
    } else { \
      ret = serialization::encode_##snippet (buf, buf_len, pos, item); \
    } \
    return ret; \
  } \
  \
  int decode_item(const char* buf, const int64_t data_len, int64_t& pos, type &item) \
  { \
    int ret = OB_SUCCESS; \
    if (OB_ISNULL(buf)) { \
      ret = OB_INVALID_ARGUMENT; \
      COMMON_LOG(WARN, "invalid args", KP(buf)); \
    } else { \
      ret = serialization::decode_##snippet (buf, data_len, pos, reinterpret_cast<type2 *>(&item)); \
    } \
    return ret; \
  } \
  \
  int64_t encoded_length_item(const type &item) \
  { \
    return serialization::encoded_length_##snippet (item); \
  }

DEFINE_ENCODE_ITEM(int64_t, vi64, int64_t)
DEFINE_ENCODE_ITEM(uint64_t, vi64, int64_t)
DEFINE_ENCODE_ITEM(int32_t, vi32, int32_t)
DEFINE_ENCODE_ITEM(int16_t, i16, int16_t)
DEFINE_ENCODE_ITEM(int8_t, i8, int8_t)
DEFINE_ENCODE_ITEM(bool, bool, bool)
DEFINE_ENCODE_ITEM(double, double, double)

}
}
