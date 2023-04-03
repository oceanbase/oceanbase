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
#include "lib/json/ob_yson.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/ob_name_id_def.h"
using namespace oceanbase::common;

namespace oceanbase
{
namespace yson
{
inline int databuff_decode_key(const char *buf, const int64_t buf_len, int64_t &pos, ElementKeyType &key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len-pos < YSON_KEY_LEN)) {
    LOG_WARN("expected int32 key but we have no more data");
    ret = OB_INVALID_DATA;
  } else {
    key = *((ElementKeyType*)(buf+pos));
    pos += sizeof(ElementKeyType);
  }
  return ret;
}

inline int databuff_print_key(char *buf, const int64_t buf_len, int64_t &pos, ElementKeyType key, bool in_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(in_array))
  {
    // do nothing, do not print the array element's key as index
  } else {
    const char* key_name = oceanbase::name::get_name(key);
    if (OB_UNLIKELY(NULL == key_name)) {
      ret = databuff_printf(buf, buf_len, pos, "%hu:", key);
    } else {
      ret = databuff_printf(buf, buf_len, pos, "%s:", key_name);
    }
  }
  return ret;
}

template<class T>
    inline int databuff_decode_print_element(char *buf, const int64_t buf_len, int64_t &pos,
                                             ElementKeyType &key, T &obj,
                                             const char *yson_buf, const int64_t yson_buf_len, int64_t &yson_pos,
                                             bool in_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_decode_key(yson_buf, yson_buf_len, yson_pos, key))) {
    LOG_WARN("failed to decode key", K(ret), K(yson_buf_len), K(yson_pos));
  } else if (OB_FAIL(databuff_decode_element_value(yson_buf, yson_buf_len, yson_pos, obj))) {
    LOG_WARN("failed to decode value", K(ret), K(yson_buf_len), K(yson_pos), K(key));
  } else if (OB_FAIL(databuff_print_key(buf, buf_len, pos, key, in_array))) {
    LOG_WARN("failed to print key", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(databuff_print_obj(buf, buf_len, pos, obj))) {
    LOG_WARN("failed to print obj", K(ret), K(buf_len), K(pos), K(key));
  }
  return ret;
}

template<>
    inline int databuff_decode_print_element(char *buf, const int64_t buf_len, int64_t &pos,
                                             ElementKeyType &key, uint32_t &obj,
                                             const char *yson_buf, const int64_t yson_buf_len, int64_t &yson_pos,
                                             bool in_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_decode_key(yson_buf, yson_buf_len, yson_pos, key))) {
    LOG_WARN("failed to decode key", K(ret), K(yson_buf_len), K(yson_pos));
  } else if (OB_FAIL(databuff_decode_element_value(yson_buf, yson_buf_len, yson_pos, obj))) {
    LOG_WARN("failed to decode value", K(ret), K(yson_buf_len), K(yson_pos), K(key));
  } else if (OB_FAIL(databuff_print_key(buf, buf_len, pos, key, in_array))) {
    LOG_WARN("failed to print key", K(ret), K(buf_len), K(pos));
  } else {
    // special case IP, improve the design later
    if (key == OB_ID(ip)) {
      unsigned char *bytes = (unsigned char *) &obj;
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\"%d.%d.%d.%d\"",
                                  bytes[3], bytes[2], bytes[1], bytes[0]))) {
        LOG_WARN("failed to print obj", K(ret), K(buf_len), K(pos), K(key));
      }
    } else {
      if (OB_FAIL(databuff_print_obj(buf, buf_len, pos, obj))) {
        LOG_WARN("failed to print obj", K(ret), K(buf_len), K(pos), K(key));
      }
    }
  }
  return ret;
}

int databuff_print_elements(char *buf, const int64_t buf_len, int64_t &pos,
                            const char *yson_buf, const int64_t yson_buf_len,
                            bool in_array /*= false*/)
{
  int ret = OB_SUCCESS;
  int64_t yson_pos = 0;
  ElementKeyType key = 0;
  union
  {
    int64_t i64;
    int32_t i32;
    uint64_t u64;
    uint32_t u32;
    bool b;
    void* ptr;
  };
  ObString str;
  ObTraceIdAdaptor trace_id;
  bool is_first = true;
  while (OB_SUCC(ret) && yson_pos >= 0 && yson_pos < yson_buf_len)
  {
    if (OB_LIKELY(!is_first)) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
        break;
      }
    }
    uint8_t yson_type = *(yson_buf+yson_pos);
    switch(yson_type)
    {
      case YSON_TYPE_INT32:
        yson_pos++;
        ret = databuff_decode_print_element(buf, buf_len, pos, key, i32, yson_buf, yson_buf_len, yson_pos, in_array);
        break;
      case YSON_TYPE_INT64:
        yson_pos++;
        ret = databuff_decode_print_element(buf, buf_len, pos, key, i64, yson_buf, yson_buf_len, yson_pos, in_array);
        break;
      case YSON_TYPE_STRING:
        yson_pos++;
        ret = databuff_decode_print_element(buf, buf_len, pos, key, str, yson_buf, yson_buf_len, yson_pos, in_array);
        break;
      case YSON_TYPE_OBJECT:
        yson_pos++;
        if (OB_UNLIKELY(yson_buf_len - yson_pos < YSON_KEY_LEN+YSON_LEAST_OBJECT_LEN)) {
          LOG_WARN("expected int64 element but we have no more data");
          ret = OB_INVALID_DATA;
        } else {
          key = *((ElementKeyType*)(yson_buf+yson_pos));
          ret = databuff_print_key(buf, buf_len, pos, key, in_array);
          yson_pos += sizeof(ElementKeyType);
          i32 = *((int32_t*)(yson_buf+yson_pos));
          yson_pos += sizeof(int32_t);
          if (OB_UNLIKELY(i32 < 0) || OB_UNLIKELY(yson_buf_len - yson_pos < i32)) {
            ret = OB_INVALID_DATA;
            int32_t v = i32;
            LOG_WARN("YSON object data corrupted", K(ret), "i32", v, "remain", yson_buf_len-yson_pos);
          } else {
            if(OB_FAIL(databuff_printf(buf, buf_len, pos, "{"))) {
            } else if (OB_FAIL(databuff_print_elements(buf, buf_len, pos, yson_buf+yson_pos, i32, false))) {
            } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "}"))) {
            } else {
              yson_pos += i32;
            }
          }
        }
        break;
      case YSON_TYPE_BOOLEAN:
        yson_pos++;
        ret = databuff_decode_print_element(buf, buf_len, pos, key, b, yson_buf, yson_buf_len, yson_pos, in_array);
        break;
      case YSON_TYPE_UINT32:
        yson_pos++;
        ret = databuff_decode_print_element(buf, buf_len, pos, key, u32, yson_buf, yson_buf_len, yson_pos, in_array);
        break;
      case YSON_TYPE_UINT64:
        yson_pos++;
        ret = databuff_decode_print_element(buf, buf_len, pos, key, u64, yson_buf, yson_buf_len, yson_pos, in_array);
        break;
      case YSON_TYPE_POINTER:
        yson_pos++;
        ret = databuff_decode_print_element(buf, buf_len, pos, key, ptr, yson_buf, yson_buf_len, yson_pos, in_array);
        break;
      case YSON_TYPE_ARRAY:
        yson_pos++;
        if (OB_UNLIKELY(yson_buf_len - yson_pos < YSON_KEY_LEN+YSON_LEAST_OBJECT_LEN)) {
          LOG_WARN("expected int64 element but we have no more data");
          ret = OB_INVALID_DATA;
        } else {
          key = *((ElementKeyType*)(yson_buf+yson_pos));
          ret = databuff_print_key(buf, buf_len, pos, key, in_array);
          yson_pos += sizeof(ElementKeyType);
          i32 = *((int32_t*)(yson_buf+yson_pos));
          yson_pos += sizeof(int32_t);
          if (OB_UNLIKELY(i32 < 0) || OB_UNLIKELY(yson_buf_len - yson_pos < i32)) {
            ret = OB_INVALID_DATA;
            int32_t v = i32;
            LOG_WARN("YSON object data corrupted", K(ret), "i32", v, "remain", yson_buf_len-yson_pos);
          } else {
            if(OB_FAIL(databuff_printf(buf, buf_len, pos, "["))) {
            } else if (OB_FAIL(databuff_print_elements(buf, buf_len, pos, yson_buf+yson_pos, i32, true))) {
            } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "]"))) {
            } else {
              yson_pos += i32;
            }
          }
        }
        break;
      case YSON_TYPE_TRACE_ID:
        yson_pos++;
        ret = databuff_decode_print_element(buf, buf_len, pos, key, trace_id, yson_buf, yson_buf_len, yson_pos, in_array);
        break;
      default:
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid YSON element type", K(ret), K(yson_type), K(yson_pos));
        break;
    }  // end switch
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to print YSON element", K(ret), K(yson_type));
    }
    if (OB_UNLIKELY(is_first)) {
      is_first = false;
    }
  }  // end while
  return ret;
}

} // end namespace yson
} // end namespace oceanbase
