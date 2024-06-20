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

#define USING_LOG_PREFIX RPC_OBMYSQL

#include <math.h>
#include "rpc/obmysql/ob_mysql_util.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/charset/ob_dtoa.h"
#include "lib/utility/ob_fast_convert.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/rowid/ob_urowid.h"
#include "common/object/ob_object.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/geo/ob_geo_bin.h"
#include "lib/xml/ob_xml_util.h"
#include "lib/xml/ob_xml_bin.h"
using namespace oceanbase::common;

namespace oceanbase
{
namespace obmysql
{
const uint64_t ObMySQLUtil::NULL_ = UINT64_MAX;
// @todo
//TODO avoid coredump if field_index is too large
//http://dev.mysql.com/doc/internals/en/prepared-statements.html#null-bitmap
//offset is 2
void ObMySQLUtil::update_null_bitmap(char *&bitmap, int64_t field_index)
{
  int byte_pos = static_cast<int>((field_index + 2) / 8);
  int bit_pos  = static_cast<int>((field_index + 2) % 8);
  bitmap[byte_pos] |= static_cast<char>(1 << bit_pos);
}

//called by handle COM_STMT_EXECUTE offset is 0
int ObMySQLUtil::store_length(char *buf, int64_t len, uint64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (len < 0 || pos < 0 || len <= pos) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input buf", K(ret), KP(buf));
  } else {
    int64_t remain = len - pos;
    if (OB_SUCC(ret)) {
      if (length < (uint64_t) 251 && remain >= 1) {
        ret = store_int1(buf, len, (uint8_t) length, pos);
      }
      /* 251 is reserved for NULL */
      else if (length < (uint64_t) 0X10000 && remain >= 3) {
        ret = store_int1(buf, len, static_cast<int8_t>(252), pos);
        if (OB_SUCC(ret)) {
          ret = store_int2(buf, len, (uint16_t) length, pos);
          if (OB_FAIL(ret)) {
            pos--;
          }
        }
      } else if (length < (uint64_t) 0X1000000 && remain >= 4) {
        ret = store_int1(buf, len, (uint8_t) 253, pos);
        if (OB_SUCC(ret)) {
          ret = store_int3(buf, len, (uint32_t) length, pos);
          if (OB_FAIL(ret)) {
            pos--;
          }
        }
      } else if (length < UINT64_MAX && remain >= 9) {
        ret = store_int1(buf, len, (uint8_t) 254, pos);
        if (OB_SUCC(ret)) {
          ret = store_int8(buf, len, (uint64_t) length, pos);
          if (OB_FAIL(ret)) {
            pos--;
          }
        }
      } else if (length == UINT64_MAX) { /* NULL_ == UINT64_MAX */
        ret = store_null(buf, len, pos);
      } else {
        ret = OB_SIZE_OVERFLOW;
      }
    }
  }
  return ret;
}

int ObMySQLUtil::get_length(const char *&pos, uint64_t &length)
{
  uint8_t sentinel = 0;
  uint16_t s2 = 0;
  uint32_t s4 = 0;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input buf", KP(pos), K(ret));
  } else {
    get_uint1(pos, sentinel);
    if (sentinel < 251) {
      length = sentinel;
    } else if (sentinel == 251) {
      length = NULL_;
    } else if (sentinel == 252) {
      get_uint2(pos, s2);
      length = s2;
    } else if (sentinel == 253) {
      get_uint3(pos, s4);
      length = s4;
    } else if (sentinel == 254) {
      get_uint8(pos, length);
    } else {
      // 255??? won't get here.
      pos--;                  // roll back
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

int ObMySQLUtil::get_length(const char *&pos, uint64_t &length, uint64_t &pos_inc_len)
{
  int ret = OB_SUCCESS;
  const char *tmp_pos = pos;
  if (OB_FAIL(get_length(pos, length))) {
    LOG_WARN("fail to get length", K(ret));
  } else {
    pos_inc_len = pos - tmp_pos;
  }
  return ret;
}

uint64_t ObMySQLUtil::get_number_store_len(const uint64_t num)
{
  uint64_t len = 0;
  if (num < 251) {
    len = 1;
  } else if (num < (1 << 16)) {
    len = 3;
  } else if (num < (1 << 24)) {
    len = 4;
  } else if (num < UINT64_MAX) {
    len = 9;
  } else if (num == UINT64_MAX) {
    // NULL_ == UINT64_MAX
    //it is represents a NULL in a ProtocolText::ResultsetRow.
    len = 1;
  }
  return len;
}

int ObMySQLUtil::store_str(char *buf, int64_t len, const char *str, int64_t &pos)
{
  uint64_t length = strlen(str);
  return store_str_v(buf, len, str, length, pos);
}

int ObMySQLUtil::store_str_v(char *buf, int64_t len, const char *str,
                             const uint64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t pos_bk = pos;

  if (OB_ISNULL(buf)) { // str could be null
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", KP(buf), K(ret));
  } else {
    if (OB_FAIL(store_length(buf, len, length, pos))) {
    } else if (len >= pos && length <= static_cast<uint64_t>(len - pos)) {
      if ((0 == length ) || (length > 0 && NULL != str)) {
        MEMCPY(buf + pos, str, length);
        pos += length;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid args", "str", ObString(length, str), K(length));
      }
    } else {
      LOG_INFO("=========== store_str_v ====", K(len), K(length), K(pos), K(pos_bk));
      pos = pos_bk;        // roll back
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ObMySQLUtil::store_obstr(char *buf, int64_t len, ObString str, int64_t &pos)
{
  return store_str_v(buf, len, str.ptr(), str.length(), pos);
}

int ObMySQLUtil::store_obstr_with_pre_space(char *buf, int64_t len, ObString str, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t pos_bk = pos;
  if (OB_ISNULL(buf)) { // str could be null
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", KP(buf), K(ret));
  } else if (str.empty()) {
    ret = store_length(buf, len, 0, pos);
  } else {
    int64_t length = str.length() + 1;
    if (OB_FAIL(store_length(buf, len, length, pos))) {
    } else if (len >= pos && length <= static_cast<uint64_t>(len - pos)) {
      buf[pos] = ' ';
      MEMCPY(buf + pos + 1, str.ptr(), str.length());
      pos += length;
    } else {
      pos = pos_bk;        // roll back
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ObMySQLUtil::store_str_zt(char *buf, int64_t len, const char *str, int64_t &pos)
{
  uint64_t length = strlen(str);
  return store_str_vzt(buf, len, str, length, pos);
}

int ObMySQLUtil::store_str_nzt(char *buf, int64_t len, const char *str, int64_t &pos)
{
  return store_str_vnzt(buf, len, str, strlen(str), pos);
}

int ObMySQLUtil::store_str_vnzt(char *buf, int64_t len, const char *str, int64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", KP(buf));
  } else {
    if (length >= 0) {
      if (len > 0 && pos > 0 && len > pos && len - pos >= length) {
        if ((0 == length) || (length > 0 && NULL != str)) {
          MEMCPY(buf + pos, str, length);
          pos += length;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid args", "str", ObString(length, str), K(length));
        }
      } else {
        ret = OB_SIZE_OVERFLOW;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid input length", K(ret), K(length));
    }
  }
  return ret;
}


int ObMySQLUtil::store_str_vzt(char *buf, int64_t len, const char *str,
                               const uint64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf))  {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), KP(buf));
  } else {
    if (len > 0 && pos > 0 && len > pos && static_cast<uint64_t>(len - pos) > length) {
      if ((0 == length) || (length > 0 && NULL != str)) {
        MEMCPY(buf + pos, str, length);
        pos += length;
        buf[pos++] = '\0';
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid args", "str", ObString(length, str), K(length));
      }
    } else {
      ret = OB_SIZE_OVERFLOW;
      //LOG_WARN("Store string fail, buffer over flow!", K(len), K(pos), K(length), K(ret));
    }
  }
  return ret;
}

int ObMySQLUtil::store_obstr_zt(char *buf, int64_t len, ObString str, int64_t &pos)
{
  return store_str_vzt(buf, len, str.ptr(), str.length(), pos);
}

int ObMySQLUtil::store_obstr_nzt(char *buf, int64_t len, ObString str, int64_t &pos)
{
  return store_str_vnzt(buf, len, str.ptr(), str.length(), pos);
}

int ObMySQLUtil::store_obstr_nzt_with_pre_space(char *buf, int64_t len, ObString str, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t length = str.length() + 1;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", KP(buf));
  } else if (OB_UNLIKELY(str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(str), K(ret), K(length));
  } else if (OB_UNLIKELY(len <= 0 || pos < 0 || len <= pos || len - pos < length)) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    buf[pos] = ' ';
    MEMCPY(buf + pos + 1, str.ptr(), str.length());
    pos += length;
  }
  return ret;

}


void ObMySQLUtil::prepend_zeros(char *buf, int64_t org_char_size, int64_t offset) {
  // memmove(buf + offset, buf, org_char_size);
  if (OB_ISNULL(buf)) {
    LOG_WARN_RET(common::OB_INVALID_ARGUMENT, "invalid buf input", KP(buf));
  } else {
    char *src_last = buf + org_char_size;
    char *dst_last = src_last + offset;
    while (org_char_size-- > 0) {
      *--dst_last = *--src_last;
    }
    while (offset-- > 0) {
      buf[offset] = '0';
    }
  }
}


int ObMySQLUtil::int_cell_str(
    char *buf, const int64_t len, int64_t val, const ObObjType obj_type,
    bool is_unsigned,
    MYSQL_PROTOCOL_TYPE type, int64_t &pos,
    bool zerofill, int32_t zflength)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input buf", KP(buf), K(ret));
  } else if (OB_UNLIKELY(zerofill && (pos + zflength + 1 > len))) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_UNLIKELY((len - pos) < (OB_LTOA10_CHAR_LEN + 9))) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    if (TEXT == type) {
//      char tmp_buff[OB_LTOA10_CHAR_LEN];
//      int64_t len_raw = ltoa10(static_cast<int64_t>(val), tmp_buff, is_unsigned ? false : true) - tmp_buff;
      uint64_t length = 0;
      int64_t zero_cnt = 0;
      ObFastFormatInt ffi(val, is_unsigned);
      if (zerofill && (zero_cnt = zflength - ffi.length()) > 0) {
        length = zflength;
      } else {
        length = static_cast<uint64_t>(ffi.length());
      }
      /* skip bytes_to_store_len bytes to store length */
      int64_t bytes_to_store_len = get_number_store_len(length);
      if (OB_UNLIKELY(pos + bytes_to_store_len + ffi.length() > len)) {
        ret = OB_SIZE_OVERFLOW;
      } else if (zero_cnt > 0 && OB_UNLIKELY(pos + bytes_to_store_len + zero_cnt + ffi.length() > len)) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        MEMCPY(buf + pos + bytes_to_store_len, ffi.ptr(), ffi.length());
        if (zero_cnt > 0) {
          /*zero_cnt > 0 indicates that zerofill is true */
          MEMSET(buf + pos + bytes_to_store_len, '0', zero_cnt);
          MEMCPY(buf + pos + bytes_to_store_len + zero_cnt, ffi.ptr(), ffi.length());
        } else {
          MEMCPY(buf + pos + bytes_to_store_len, ffi.ptr(), ffi.length());
        }
        ret = ObMySQLUtil::store_length(buf, pos + bytes_to_store_len, length, pos);
        pos += length;
      }
    } else {
      switch (obj_type) {
        case ObTinyIntType:
        case ObUTinyIntType: {
          ret = ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(val), pos);
          break;
        }
        case ObSmallIntType:
        case ObUSmallIntType: {
          ret = ObMySQLUtil::store_int2(buf, len, static_cast<int16_t>(val), pos);
          break;
        }
        case ObMediumIntType:
        case ObUMediumIntType:
        case ObInt32Type:
        case ObUInt32Type: {
          ret = ObMySQLUtil::store_int4(buf, len, static_cast<int32_t>(val), pos);
          break;
        }
        case ObIntType:
        case ObUInt64Type: {
          ret = ObMySQLUtil::store_int8(buf, len, static_cast<int64_t>(val), pos);
          break;
        }
        default: {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid obj_type", K(ret), K(obj_type));
        }
      }
    }
  }
  return ret;
}

int ObMySQLUtil::null_cell_str(char *buf, const int64_t len, MYSQL_PROTOCOL_TYPE type,
                               int64_t &pos, int64_t cell_index, char *bitmap)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input buf", K(ret), KP(buf));
  } else {
    if (len - pos <= 0) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      if (BINARY == type) {
        ObMySQLUtil::update_null_bitmap(bitmap, cell_index);
      } else {
        ret = ObMySQLUtil::store_null(buf, len, pos);
      }
    }
  }
  return ret;
}

int ObMySQLUtil::number_cell_str(
    char *buf, const int64_t len, const number::ObNumber &val, int64_t &pos, int16_t scale,
    bool zerofill, int32_t zflength)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid buf input", KP(buf), K(ret));
  } else {
    /* skip 1 byte to store length */
    int64_t length = 0;
    if (OB_UNLIKELY(zerofill && (pos + zflength + 1 > len))) {
      ret = OB_SIZE_OVERFLOW;
    } else if (OB_FAIL(val.format(buf + pos + 1, len - pos - 1, length, scale))) {
    } else {
      int64_t zero_cnt = 0;
      if (zerofill && (zero_cnt = zflength - length) > 0) {
        ObMySQLUtil::prepend_zeros(buf + pos + 1, length, zero_cnt);
        length = zflength;
      }

      ret = ObMySQLUtil::store_length(buf, len, length, pos);
      pos += length;
    }
  }
  return ret;
}

int ObMySQLUtil::datetime_cell_str(
    char *buf, const int64_t len,
    int64_t val, MYSQL_PROTOCOL_TYPE type, int64_t &pos,
    const ObTimeZoneInfo *tz_info, int16_t scale)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), KP(buf));
  } else {
    if (type == BINARY) {
      ObTime ob_time;
      uint8_t timelen = 0;
      if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(val, tz_info, ob_time))) {
        LOG_WARN("convert usec ", K(ret));
      } else {
        if (ob_time.parts_[DT_USEC]) {
          timelen = 11;
        } else if (ob_time.parts_[DT_HOUR] || ob_time.parts_[DT_MIN] || ob_time.parts_[DT_SEC]) {
          timelen = 7;
        } else if (ob_time.parts_[DT_YEAR] || ob_time.parts_[DT_MON] || ob_time.parts_[DT_MDAY]) {
          timelen = 4;
        } else {
          timelen = 0;
        }

        if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, timelen, pos))) {
          LOG_WARN("failed to store int", K(len), K(timelen), K(pos), K(ret));
        }

        if(timelen > 0 && OB_SUCC(ret)) {
          if (OB_FAIL(ObMySQLUtil::store_int2(buf, len, static_cast<int16_t>(ob_time.parts_[DT_YEAR]), pos))) {
            LOG_WARN("failed to store int", K(len), K(timelen), K(pos), K(ret));
          } else if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(ob_time.parts_[DT_MON]), pos))) {
            LOG_WARN("failed to store int", K(len), K(timelen), K(pos), K(ret));
          } else if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(ob_time.parts_[DT_MDAY]), pos))) {
            LOG_WARN("failed to store int", K(len), K(timelen), K(pos), K(ret));
          }
        }

        if(timelen > 4 && OB_SUCC(ret)) {
          if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(ob_time.parts_[DT_HOUR]), pos))) {
            LOG_WARN("failed to store int", K(len), K(timelen), K(pos), K(ret));
          } else if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(ob_time.parts_[DT_MIN]), pos))) {
            LOG_WARN("failed to store int", K(len), K(timelen), K(pos), K(ret));
          } else if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(ob_time.parts_[DT_SEC]), pos))) {
            LOG_WARN("failed to store int", K(len), K(timelen), K(pos), K(ret));
          }
        }

        if(timelen > 7 && OB_SUCC(ret)) {
          if (OB_FAIL(ObMySQLUtil::store_int4(buf, len, static_cast<int32_t>(ob_time.parts_[DT_USEC]), pos))) {
            LOG_WARN("failed to store int", K(len), K(timelen), K(pos), K(ret));
          }
        }
      }
    } else {
      /* skip 1 byte to store length */
      int64_t pos_begin = pos++;
      if (len - pos <= 1) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        const ObString nls_format;
        if (OB_FAIL(ObTimeConverter::datetime_to_str(val, tz_info, nls_format, scale, buf, len, pos))) {
          pos = pos_begin + 1;
        } else {
          // store length as beginning
          int64_t timelen = pos - pos_begin - 1;
          ret = ObMySQLUtil::store_length(buf, len, timelen, pos_begin);
        }
      }
    }
  }
  return ret;
}

int ObMySQLUtil::write_segment_str(char *buf, const int64_t len, int64_t &pos, const ObString &str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), KP(buf));
  } else if (OB_UNLIKELY(len - pos <= (1 + str.length()))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("buf is oversize", K(ret), K(len), K(pos), "str_len", str.length());
  } else if (OB_FAIL(ObMySQLUtil::store_length(buf, len, str.length(), pos))) {
    LOG_WARN("failed to store_length", K(ret), K(len), K(pos), "str_len", str.length());
  } else {
    if (!str.empty()) {
      MEMCPY(buf + pos, str.ptr(), str.length());
      pos += str.length();
    }
  }
  return ret;
}


int ObMySQLUtil::otimestamp_cell_str(
    char *buf, const int64_t len,
    const ObOTimestampData &ot_data, MYSQL_PROTOCOL_TYPE type, int64_t &pos,
    const ObDataTypeCastParams &dtc_params, const int16_t scale, const ObObjType obj_type)
{
  int ret = OB_SUCCESS;
  UNUSED(type);
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), KP(buf));
  } else if (OB_UNLIKELY(len - pos <= 1)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("buf is oversize", K(ret), K(len), K(pos));
  } else {
    /* skip 1 byte to store length */
    int64_t pos_begin = pos++;
    if (ot_data.is_null_value()) {
      //do nothing
    } else if (OB_FAIL(ObTimeConverter::encode_otimestamp(obj_type, buf, len, pos, dtc_params.tz_info_, ot_data, static_cast<int8_t>(scale)))) {
      LOG_WARN("failed to encode_otimestamp", K(ret));
    }

    if (OB_SUCC(ret)) {
      // store length as beginning
      int64_t total_len = pos - pos_begin - 1;
      ret = ObMySQLUtil::store_length(buf, len, total_len, pos_begin);
    }
  }
  return ret;
}

int ObMySQLUtil::otimestamp_cell_str2(
    char *buf, const int64_t len,
    const ObOTimestampData &ot_data, MYSQL_PROTOCOL_TYPE type, int64_t &pos,
    const ObDataTypeCastParams &dtc_params, const int16_t scale, const ObObjType obj_type)
{
  int ret = OB_SUCCESS;
  UNUSED(type);
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), KP(buf));
  } else if (OB_UNLIKELY(len - pos <= 1)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("buf is oversize", K(ret), K(len), K(pos));
  } else {
    /* skip 1 byte to store length */
    int64_t pos_begin = pos++;
    if (ot_data.is_null_value()) {
      //do nothing
    }  else {
      if (OB_FAIL(ObTimeConverter::otimestamp_to_str(ot_data, dtc_params, scale, obj_type, buf, len, pos))) {
        LOG_WARN("failed to convert timestamp_tz to str", K(ret));
        pos = pos_begin + 1;
      }
    }

    if (OB_SUCC(ret)) {
      // store length as beginning
      int64_t total_len = pos - pos_begin - 1;
      ret = ObMySQLUtil::store_length(buf, len, total_len, pos_begin);
    }
  }
  return ret;
}

int ObMySQLUtil::date_cell_str(
    char *buf, const int64_t len,
    int32_t val, MYSQL_PROTOCOL_TYPE type, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), KP(buf));
  } else {
    if (type == BINARY) {
      ObTime ob_time;
      uint8_t timelen = 0;
      if (OB_FAIL(ObTimeConverter::date_to_ob_time(val, ob_time))) {
        LOG_WARN("convert day to date failed", K(ret));
      } else if (0 == ob_time.parts_[DT_YEAR] && 0 == ob_time.parts_[DT_MON] && 0 == ob_time.parts_[DT_MDAY]) {
        timelen = 0;
        ret = ObMySQLUtil::store_int1(buf, len, timelen, pos);
      } else {
        timelen = 4;
        if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, timelen, pos))) {//length(1)
          LOG_WARN("failed to store int", K(len), K(timelen), K(pos), K(ret));
        } else if (OB_FAIL(
                       ObMySQLUtil::store_int2(buf, len, static_cast<int16_t>(ob_time.parts_[DT_YEAR]), pos))) {//year(2)
          LOG_WARN("failed to store int", K(len), K(timelen), K(pos), K(ret));
        } else if (OB_FAIL(
                       ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(ob_time.parts_[DT_MON]), pos))) {//mouth(1)
          LOG_WARN("failed to store int", K(len), K(timelen), K(pos), K(ret));
        } else if (OB_FAIL(
                       ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(ob_time.parts_[DT_MDAY]), pos))) {//day(1)
          LOG_WARN("failed to store int", K(len), K(timelen), K(pos), K(ret));
        }
      }
    } else {
      /* skip 1 byte to store length */
      int64_t pos_begin = pos++;
      if (len - pos <= 1) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        if (OB_FAIL(ObTimeConverter::date_to_str(val, buf, len, pos))) {
          pos = pos_begin + 1;
        } else {
          // store length as beginning
          int64_t timelen = pos - pos_begin - 1;
          ret = ObMySQLUtil::store_length(buf, len, timelen, pos_begin);
        }
      }
    }
  }
  return ret;
}

int ObMySQLUtil::time_cell_str(
    char *buf, const int64_t len,
    int64_t val, MYSQL_PROTOCOL_TYPE type, int64_t &pos, int16_t scale)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), KP(buf));
  } else {
    if (type == BINARY) {
      ObTime ob_time;
      uint8_t timelen = 0;
      if (OB_FAIL(ObTimeConverter::time_to_ob_time(val, ob_time))) {
        LOG_WARN("convert usec to timestamp failed", K(ret));
      } else {
        int ob_time_day = ob_time.parts_[DT_DATE] + ob_time.parts_[DT_HOUR] / 24;
        int ob_time_hour = ob_time.parts_[DT_HOUR] % 24;
        int8_t is_negative = (DT_MODE_NEG & ob_time.mode_) ? 1 : 0;
        if (ob_time.parts_[DT_USEC]) {
          timelen = 12;
        } else if (ob_time_day || ob_time_hour || ob_time.parts_[DT_MIN] || ob_time.parts_[DT_SEC]) {
          timelen = 8;
        } else {
          timelen = 0;
        }

        if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, timelen, pos))) {//length
          LOG_WARN("fail to store int", K(ret));
        }

        if(timelen > 0 && OB_SUCC(ret)) {
          if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(is_negative), pos))) {//is_negative(1)
            LOG_WARN("fail to store int", K(ret));
          } else if (OB_FAIL(ObMySQLUtil::store_int4(buf, len, static_cast<int32_t>(ob_time_day), pos))) {//days(4)
            LOG_WARN("fail to store int", K(ret));
          } else if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(ob_time_hour), pos))) {//hour(1)
            LOG_WARN("fail to store int", K(ret));
          } else if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(ob_time.parts_[DT_MIN]), pos))) {//minute(1)
            LOG_WARN("fail to store int", K(ret));
          } else if ( OB_FAIL(ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(ob_time.parts_[DT_SEC]), pos))) {//second(1)
            LOG_WARN("fail to store int", K(ret));
          }
        }

        if(timelen > 8 && OB_SUCC(ret)) {
          if (OB_FAIL(ObMySQLUtil::store_int4(buf, len, static_cast<int32_t>(ob_time.parts_[DT_USEC]), pos))) {//micro-second(4)
            LOG_WARN("fail to store int", K(ret));
          }
        }
      }
    } else {
      /* skip 1 byte to store length */
      int64_t pos_begin = pos++;
      if (len - pos <= 1) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        if (OB_FAIL(ObTimeConverter::time_to_str(val, scale, buf, len, pos))) {
          pos = pos_begin + 1;
        } else {
          // store length as beginning
          int64_t timelen = pos - pos_begin - 1;
          ret = ObMySQLUtil::store_length(buf, len, timelen, pos_begin);
        }
      }
    }
  }
  return ret;
}

int ObMySQLUtil::year_cell_str(
    char *buf, const int64_t len,
    uint8_t val, MYSQL_PROTOCOL_TYPE type, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), KP(buf));
  } else {
    if (type == BINARY) {
      int64_t year = 0;
      if (OB_FAIL(ObTimeConverter::year_to_int(val, year))) {
        LOG_WARN("failed to convert year to integer", K(ret));
      } else {
        if (OB_FAIL(ObMySQLUtil::store_int2(buf, len, static_cast<int16_t>(year), pos))) {
          LOG_WARN("failed to store int", K(len), K(pos), K(ret));
        }
      }
    } else {
      /* skip 1 byte to store length */
      int64_t pos_begin = pos++;
      if (len - pos <= 1) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        if (OB_FAIL(ObTimeConverter::year_to_str(val, buf, len, pos))) {
          pos = pos_begin + 1;
        } else {
          // store length as beginning
          uint64_t yearlen = pos - pos_begin - 1;
          ret = ObMySQLUtil::store_length(buf, len, yearlen, pos_begin);
        }
      }
    }
  }
  return ret;
}

int ObMySQLUtil::varchar_cell_str(char *buf, const int64_t len, const ObString &val,
    const bool is_oracle_raw, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), KP(buf));
  } else {
    int64_t length = is_oracle_raw ? val.length() * 2 : val.length();

    if (OB_LIKELY(length < len - pos)) {
      int64_t pos_bk = pos;
      if (OB_FAIL(ObMySQLUtil::store_length(buf, len, length, pos))) {
      } else {
        if (OB_LIKELY(length <= len - pos)) {
          //TODO::@xiaofeng, delete it latter after obclient/obj support RAW
          if (is_oracle_raw) {
            if (OB_FAIL(hex_print(val.ptr(), val.length(), buf, len, pos))) {
              pos = pos_bk;
              LOG_WARN("fail to hex_print", K(ret), K(val));
            }
          } else {
            MEMCPY(buf + pos, val.ptr(), length);
            pos += length;
          }
        } else {
          pos = pos_bk;
          ret = OB_SIZE_OVERFLOW;
        }
      }
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ObMySQLUtil::float_cell_str(char *buf, const int64_t len, float val,
                                MYSQL_PROTOCOL_TYPE type, int64_t &pos, int16_t scale,
                                bool zerofill, int32_t zflength)
{
  static const int FLT_LEN =  FLOAT_TO_STRING_CONVERSION_BUFFER_SIZE;
  static const int FLT_SIZE = sizeof (float);

  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), KP(buf));
  } else {
    if (BINARY == type) {
      if (len - pos > FLT_SIZE) {
        MEMCPY(buf + pos, &val, FLT_SIZE);
        pos += FLT_SIZE;
      } else {
        ret = OB_SIZE_OVERFLOW;
      }
    } else {
      const bool is_oracle = lib::is_oracle_mode();
      if (OB_UNLIKELY(zerofill && (pos + zflength + 1 > len))) {
        ret = OB_SIZE_OVERFLOW;
      } else if (OB_UNLIKELY(len - pos <= FLT_LEN)) {
        ret = OB_SIZE_OVERFLOW;
      } else if (len - pos > FLT_LEN) {
        int64_t length;
        if (val == INFINITY && is_oracle) {  // only show inf in oracle mode
          length = strlen("Inf");
          strncpy(buf + pos + 1, "Inf", length);
        } else if (val == -INFINITY && is_oracle) {
          length = strlen("-Inf");
          strncpy(buf + pos + 1, "-Inf", length);
        } else if (isnan(val) && is_oracle) {
          length = strlen("Nan");
          strncpy(buf + pos + 1, "Nan", length);
        } else {
          if (0 <= scale) {
            length = ob_fcvt(val, scale, FLT_LEN - 1, buf + pos + 1, NULL);
          } else {
            length = ob_gcvt_opt(val, OB_GCVT_ARG_FLOAT, FLT_LEN - 1, buf + pos + 1,
                                 NULL, is_oracle, TRUE);
          }
        }
        ObString tmp_str(0, length, buf + pos + 1);
        LOG_DEBUG("float_cell_str", K(val), K(scale), K(zerofill), K(zflength), K(tmp_str));
        if (length < 251) {
          int64_t zero_cnt = 0;
          if (zerofill && (zero_cnt = zflength - length) > 0) {
            ObMySQLUtil::prepend_zeros(buf + pos + 1, length, zero_cnt);
            length = zflength;
          }
          ret = ObMySQLUtil::store_length(buf, len, length, pos);
          pos += length;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("invalid length", K(length), K(ret)); //OB_ASSERT(length < 251);
        }
      }
    }
  }

  return ret;
}

int ObMySQLUtil::double_cell_str(char *buf, const int64_t len, double val,
                                 MYSQL_PROTOCOL_TYPE type, int64_t &pos, int16_t scale,
                                 bool zerofill, int32_t zflength)
{
  static const int DBL_LEN =  DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE;
  static const int DBL_SIZE = sizeof (double);

  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", KP(buf), K(ret));
  } else {
    if (BINARY == type) {
      if (lib::is_oracle_mode() && // only oracle mode need convert
          std::fpclassify(val) == FP_ZERO && std::signbit(val)) {
        val = val * -1; // if -0.0, change to 0.0
      }
      if (len - pos > DBL_SIZE) {
        MEMCPY(buf + pos, &val, DBL_SIZE);
        pos += DBL_SIZE;
      } else {
        ret = OB_SIZE_OVERFLOW;
      }
    } else {
      const bool is_oracle = lib::is_oracle_mode();
      // max size: DOUBLE_MAX + 3 bytes to store its length
      if (OB_UNLIKELY(zerofill && (pos + zflength + 3 > len))) {
        ret = OB_SIZE_OVERFLOW;
      } else if (OB_UNLIKELY(len - pos < DBL_LEN + 3)) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        // we skip 1 bytes to store length for most cases
        int64_t length;
        if (val == INFINITY && is_oracle) { // only show inf in oracle mode
          length = strlen("Inf");
          strncpy(buf + pos + 1, "Inf", length);
        } else if (val == -INFINITY && is_oracle) {
          length = strlen("-Inf");
          strncpy(buf + pos + 1, "-Inf", length);
        } else if (isnan(val) && is_oracle) {
          length = strlen("Nan");
          strncpy(buf + pos + 1, "Nan", length);
        } else {
          if (0 <= scale) {
            length = ob_fcvt(val, scale, DBL_LEN - 1, buf + pos + 1, NULL);
          } else {
            length = ob_gcvt_opt(val, OB_GCVT_ARG_DOUBLE, DBL_LEN - 1, buf + pos + 1,
                                 NULL, is_oracle, TRUE);
          }
        }
        ObString tmp_str(0, length, buf + pos + 1);
        LOG_DEBUG("double_cell_str", K(val), K(scale), K(zerofill), K(zflength), K(tmp_str));

        if (length <= DBL_LEN) { //OB_ASSERT(length <= DBL_LEN);
          int64_t zero_cnt = 0;
          if (zerofill && (zero_cnt = zflength - length) > 0) {
            ObMySQLUtil::prepend_zeros(buf + pos + 1, length, zero_cnt);
            length = zflength;
          }

          // 按照协议: 如果double的长度小于251字节，长度头只需要1个字节，否则长度头需要3个字节
          if (length < 251) {
            ret = ObMySQLUtil::store_length(buf, len, length, pos);
            pos += length;
          } else if (length >= 251) {
            // we need 3 btyes to hold length of double (maybe)
            memmove(buf + pos + 3, buf + pos + 1, length);
            ret = ObMySQLUtil::store_length(buf, len, length, pos);
            pos += length;
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("invalid length", K(length), K(ret));
        }
      }
    }
  }
  return ret;
}
int ObMySQLUtil::bit_cell_str(
    char *buf, const int64_t len, uint64_t val, int32_t bit_len,
    MYSQL_PROTOCOL_TYPE type, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t length = 0;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input buf", KP(buf), K(ret));
  } else if (TEXT == type) {
    length = (bit_len + 7) / 8;
    if (OB_FAIL(ObMySQLUtil::store_length(buf, len, length, pos))) {
      LOG_WARN("fail to store length", K(ret), KP(buf), K(len), K(length), K(pos));
    } else if (OB_FAIL(bit_to_char_array(val, bit_len, buf, len, pos))) {
      LOG_WARN("fail to trans bit to str", K(ret), KP(buf), K(len), K(val), K(pos), K(bit_len), KCSTRING(lbt()));
    } else {/*do nothing*/}
  } else if (BINARY == type) {
    length = (bit_len + 7) / 8;
    if (OB_FAIL(ObMySQLUtil::store_length(buf, len, length, pos))) {
      LOG_WARN("fail to store length", K(ret), KP(buf), K(len), K(length), K(pos));
    } else if (OB_FAIL(bit_to_char_array(val, bit_len, buf, len, pos))) {
      LOG_WARN("fail to trans bit to str", K(ret), KP(buf), K(len), K(val), K(pos), K(bit_len), KCSTRING(lbt()));
    } else {/*do nothing*/}
  } else {/*do nothing*/}
  return ret;
}

int ObMySQLUtil::interval_ym_cell_str(char *buf, const int64_t len, ObIntervalYMValue val,
                                      MYSQL_PROTOCOL_TYPE type, int64_t &pos, const ObScale scale)
{
  int ret = OB_SUCCESS;
  UNUSED(type);
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), KP(buf));
  } else if (OB_UNLIKELY(len - pos <= 1)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("buf is oversize", K(ret), K(len), K(pos));
  } else {
    /* skip 1 byte to store length */
    int64_t pos_begin = pos++;
    if (OB_FAIL(ObTimeConverter::encode_interval_ym(buf, len, pos, val, scale))) {
      LOG_WARN("fail to encode interval year to month", K(ret), K(val), K(scale));
    } else {
      // store length as beginning
      int64_t total_len = pos - pos_begin - 1;
      ret = ObMySQLUtil::store_length(buf, len, total_len, pos_begin);
    }
  }
  return ret;
}

int ObMySQLUtil::interval_ds_cell_str(char *buf, const int64_t len, ObIntervalDSValue val,
                                      MYSQL_PROTOCOL_TYPE type, int64_t &pos, const ObScale scale)
{

  int ret = OB_SUCCESS;
  UNUSED(type);
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), KP(buf));
  } else if (OB_UNLIKELY(len - pos <= 1)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("buf is oversize", K(ret), K(len), K(pos));
  } else {
    /* skip 1 byte to store length */
    int64_t pos_begin = pos++;
    if (OB_FAIL(ObTimeConverter::encode_interval_ds(buf, len, pos, val, scale))) {
      LOG_WARN("fail to encode interval day to second", K(ret), K(val), K(scale));
    }

    if (OB_SUCC(ret)) {
      // store length as beginning
      int64_t total_len = pos - pos_begin - 1;
      ret = ObMySQLUtil::store_length(buf, len, total_len, pos_begin);
    }
  }
  return ret;
}

int ObMySQLUtil::urowid_cell_str(char *buf, const int64_t len, const ObURowIDData &urowid_data,
                                 int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null buffer", K(ret), KP(buf));
  } else if (OB_UNLIKELY(pos >= len)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("buffer is not enought", K(ret), K(pos), K(len));
  } else {
    int64_t base64_buf_len = urowid_data.needed_base64_buffer_size();
    if (OB_LIKELY(base64_buf_len < len - pos)) {
      int64_t pos_bk = pos;
      if (OB_FAIL(ObMySQLUtil::store_length(buf, len, base64_buf_len, pos))) {
        LOG_WARN("failed to store length", K(ret));
      } else {
        if (OB_LIKELY(base64_buf_len <= len - pos)) {
          if (OB_FAIL(urowid_data.get_base64_str(buf, len, pos))) {
            LOG_WARN("failed to get base64 str", K(ret));
          }
        } else {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("buffer is not enough", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        pos = pos_bk;
      }
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ObMySQLUtil::sql_utd_cell_str(uint64_t tenant_id, char *buf, const int64_t len, const ObString &val, int64_t &pos)
{
  INIT_SUCC(ret);
  lib::ObMemAttr mem_attr(tenant_id, "XMLModule");
  lib::ObMallocHookAttrGuard malloc_guard(mem_attr);
  ObArenaAllocator allocator(mem_attr);
  ObMulModeNodeType node_type = M_MAX_TYPE;
  ObStringBuffer jbuf(&allocator);
  ParamPrint param_list;
  param_list.indent = 2;
  ObIMulModeBase *node = NULL;
  ObXmlNode *xml_node = NULL;
  ObMulModeMemCtx* xml_mem_ctx = nullptr;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid input args", K(ret), KP(buf));
  } else if (val.length() == 0) {
    if (OB_FAIL(ObMySQLUtil::store_null(buf, len, pos))) {
      OB_LOG(WARN, "fail to set null string", K(pos), K(len));
    }
  } else {
    int64_t new_length = val.length();
    if (OB_LIKELY(new_length < len - pos)) {
      int64_t pos_bk = pos;
      if (OB_FAIL(ObMySQLUtil::store_length(buf, len, new_length, pos))) {
        LOG_WARN("xml_cell_str store length failed", K(ret), K(len), K(new_length), K(pos));
      } else {
        if (OB_LIKELY(new_length <= len - pos)) {
          MEMCPY(buf + pos, val.ptr(), val.length());
          pos += new_length;
        } else {
          pos = pos_bk;
          ret = OB_SIZE_OVERFLOW;
        }
      }
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ObMySQLUtil::json_cell_str(uint64_t tenant_id, char *buf, const int64_t len, const ObString &val, int64_t &pos)
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr mem_attr(tenant_id, "JsonAlloc");
  ObArenaAllocator allocator(mem_attr);
  ObJsonBin j_bin(val.ptr(), val.length(), &allocator);
  ObIJsonBase *j_base = &j_bin;
  ObJsonBuffer jbuf(&allocator);
  static_cast<ObJsonBin*>(j_base)->set_seek_flag(true);
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid input args", K(ret), KP(buf));
  } else if (val.length() == 0) {
    if (OB_FAIL(ObMySQLUtil::store_null(buf, len, pos))) {
      OB_LOG(WARN, "fail to set null string", K(pos), K(len));
    }
  } else if (OB_FAIL(j_bin.reset_iter())) {
    OB_LOG(WARN, "fail to reset json bin iter", K(ret), K(val));
  } else if (OB_FAIL(j_base->print(jbuf, true))) {
    OB_LOG(WARN, "json binary to string failed in mysql mode", K(ret), K(val), K(*j_base));
  } else {
    int64_t new_length = jbuf.length();
    if (OB_LIKELY(new_length < len - pos)) {
      int64_t pos_bk = pos;
      if (OB_FAIL(ObMySQLUtil::store_length(buf, len, new_length, pos))) {
        OB_LOG(WARN, "json_cell_str store length failed", K(ret), K(len), K(new_length), K(pos));
      } else {
        if (OB_LIKELY(new_length <= len - pos)) {
          MEMCPY(buf + pos, jbuf.ptr(), new_length);
          pos += new_length;
        } else {
          pos = pos_bk;
          ret = OB_SIZE_OVERFLOW;
        }
      }
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }

  return ret;
}

int ObMySQLUtil::decimalint_cell_str(char *buf, const int64_t len, const ObDecimalInt *decint,
                                     const int32_t int_bytes, int16_t scale, int64_t &pos,
                                     bool zerofill, int32_t zflength)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null buffer", K(ret), K(buf));
  } else {
    int64_t length = 0;
    if (OB_UNLIKELY(zerofill && pos + zflength + 1 > len)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("buffer not enough", K(ret), K(zerofill), K(pos), K(zflength), K(len));
    } else if (OB_FAIL(
                 wide::to_string(decint, int_bytes, scale, buf + pos + 1, len - pos - 1, length))) {
      LOG_WARN("to_string failed", K(ret), K(scale), K(pos), K(len));
    } else {
      int64_t zero_cnt = 0;
      if (zerofill && (zero_cnt = zflength - length) > 0) {
        ObMySQLUtil::prepend_zeros(buf + pos + 1, length, zero_cnt);
        length = zflength;
      }

      ret = ObMySQLUtil::store_length(buf, len, length, pos);
      pos += length;
    }
  }
  return ret;
}

int ObMySQLUtil::geometry_cell_str(char *buf, const int64_t len, const ObString &val, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t length = val.length();
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), KP(buf));
  } else if (length < WKB_DATA_OFFSET + WKB_GEO_TYPE_SIZE) {
    if (OB_LIKELY(length < len - pos)) {
      int64_t pos_bk = pos;
      if (OB_FAIL(ObMySQLUtil::store_length(buf, len, length, pos))) {
        LOG_WARN("geometry_cell_str store length failed", K(ret), K(len), K(length), K(pos));
      } else {
        if (OB_LIKELY(length <= len - pos)) {
          MEMCPY(buf + pos, val.ptr(), length);
          pos += length;
        } else {
          pos = pos_bk;
          ret = OB_SIZE_OVERFLOW;
        }
      }
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  } else {
    uint8_t version = (*(val.ptr() + WKB_GEO_SRID_SIZE));
    uint8_t offset = WKB_GEO_SRID_SIZE;
    if (IS_GEO_VERSION(version)) {
      // version exist
      length = val.length() - WKB_VERSION_SIZE;
      offset += WKB_VERSION_SIZE;
    }

    if (OB_LIKELY(length < len - pos)) {
      int64_t pos_bk = pos;
      if (OB_FAIL(ObMySQLUtil::store_length(buf, len, length, pos))) {
        LOG_WARN("geometry_cell_str store length failed", K(ret), K(len), K(length), K(pos));
      } else {
        if (OB_LIKELY(length <= len - pos)) {
          MEMCPY(buf + pos, val.ptr(), WKB_GEO_SRID_SIZE); // srid
          pos += WKB_GEO_SRID_SIZE;
          MEMCPY(buf + pos, val.ptr() + offset, length - WKB_GEO_SRID_SIZE);
          pos += (length - WKB_GEO_SRID_SIZE);
        } else {
          pos = pos_bk;
          ret = OB_SIZE_OVERFLOW;
        }
      }
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

} // namespace obmysql
} // namespace oceanbase
