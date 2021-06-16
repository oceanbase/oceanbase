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

#ifndef _OB_MYSQL_UTIL_H_
#define _OB_MYSQL_UTIL_H_

#include <inttypes.h>
#include <stdint.h>
#include <float.h>  // for FLT_DIG and DBL_DIG
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/number/ob_number_v2.h"
#include "lib/timezone/ob_timezone_info.h"
#include "rpc/obmysql/ob_mysql_global.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace common {
class ObOTimestampData;
class ObURowIDData;
class ObLobLocator;
class ObDataTypeCastParams;
}  // namespace common
namespace obmysql {
enum MYSQL_PROTOCOL_TYPE {
  TEXT = 1,
  BINARY,
};

class ObMySQLUtil {
public:
  /**
   * update null bitmap for binary protocol
   * please review http://dev.mysql.com/doc/internals/en/prepared-statements.html#null-bitmap for detail
   *
   * @param bitmap[in/out]         input bitmap
   * @param field_index            index of field
   *
   */
  static void update_null_bitmap(char*& bitmap, int64_t field_index);

  /**
   * Write the length into buf, which may occupy 1,3,4,9 bytes.
   */
  static int store_length(char* buf, int64_t len, uint64_t length, int64_t& pos);
  /**
   * Read length field, use with store_length()
   */
  static int get_length(const char*& pos, uint64_t& length);
  /**
   * Read length field, use with store_length()
   */
  static int get_length(const char*& pos, uint64_t& length, uint64_t& pos_inc_len);
  /**
   * When storing numbers in length coded string format, the length that needs to be occupied
   */
  static uint64_t get_number_store_len(const uint64_t num);
  /**
   * Save a NULL flag.
   */
  static inline int store_null(char* buf, int64_t len, int64_t& pos);
  /**
   * Pack a string str ending with'\0' into buf (length coded string format)
   */
  static int store_str(char* buf, int64_t len, const char* str, int64_t& pos);
  /**
   * Pack a length of string str into buf (length coded string format)
   */
  static int store_str_v(char* buf, int64_t len, const char* str, const uint64_t length, int64_t& pos);
  /**
   * Pack the string str of the ObString structure into buf (length coded string format)
   */
  static int store_obstr(char* buf, int64_t len, ObString str, int64_t& pos);
  static int store_obstr_with_pre_space(char* buf, int64_t len, ObString str, int64_t& pos);
  /**
   * Pack a string str ending with'\0' into buf (zero terminated string format)
   */
  static int store_str_zt(char* buf, int64_t len, const char* str, int64_t& pos);
  /**
   * Pack a string str ending with'\0' into buf (none-zero terminated string format)
   */
  static int store_str_nzt(char* buf, int64_t len, const char* str, int64_t& pos);
  /**
   * Pack a length of string str into buf (zero terminated string format)
   */
  static int store_str_vzt(char* buf, int64_t len, const char* str, const uint64_t length, int64_t& pos);
  /**
   * Pack a length of string str into buf (none-zero terminated string format)
   */
  static int store_str_vnzt(char* buf, int64_t len, const char* str, int64_t length, int64_t& pos);
  /**
   * Pack the string str of the ObString structure into buf (zero terminated string format)
   */
  static int store_obstr_zt(char* buf, int64_t len, ObString str, int64_t& pos);
  /**
   * Pack the string str of the ObString structure into buf (none-zero terminated raw string format)
   */
  static int store_obstr_nzt(char* buf, int64_t len, ObString str, int64_t& pos);
  static int store_obstr_nzt_with_pre_space(char* buf, int64_t len, ObString str, int64_t& pos);
  //@{Serialize integer data, save the data in v to the position of buf+pos, and update pos
  static inline int store_int1(char* buf, int64_t len, int8_t v, int64_t& pos);
  static inline int store_int2(char* buf, int64_t len, int16_t v, int64_t& pos);
  static inline int store_int3(char* buf, int64_t len, int32_t v, int64_t& pos);
  static inline int store_int4(char* buf, int64_t len, int32_t v, int64_t& pos);
  static inline int store_int5(char* buf, int64_t len, int64_t v, int64_t& pos);
  static inline int store_int6(char* buf, int64_t len, int64_t v, int64_t& pos);
  static inline int store_int8(char* buf, int64_t len, int64_t v, int64_t& pos);
  //@}

  //@{ Signed integer in reverse sequence, write the result to v, and update pos
  static inline void get_int1(const char*& pos, int8_t& v);
  static inline void get_int2(const char*& pos, int16_t& v);
  static inline void get_int3(const char*& pos, int32_t& v);
  static inline void get_int4(const char*& pos, int32_t& v);
  static inline void get_int8(const char*& pos, int64_t& v);
  //@}

  //@{ Deserialize unsigned integer, write the result to v, and update pos
  static inline void get_uint1(const char*& pos, uint8_t& v);
  static inline void get_uint2(const char*& pos, uint16_t& v);
  static inline void get_uint3(const char*& pos, uint32_t& v);
  static inline void get_uint4(const char*& pos, uint32_t& v);
  static inline void get_uint5(const char*& pos, uint64_t& v);
  static inline void get_uint6(const char*& pos, uint64_t& v);
  static inline void get_uint8(const char*& pos, uint64_t& v);

  static inline void get_uint1(char*& pos, uint8_t& v);
  static inline void get_uint2(char*& pos, uint16_t& v);
  static inline void get_uint3(char*& pos, uint32_t& v);
  static inline void get_uint4(char*& pos, uint32_t& v);
  static inline void get_uint5(char*& pos, uint64_t& v);
  static inline void get_uint6(char*& pos, uint64_t& v);
  static inline void get_uint8(char*& pos, uint64_t& v);
  //@}

  /**
   * Add 0 to buf[0...offset), the original org_char_size byte data will be moved back by offset byte
   */
  static void prepend_zeros(char* buf, const int64_t org_char_size, int64_t offset);

  /**
   * Serialize a null type cell to the position of buf + pos.
   */
  static int null_cell_str(
      char* buf, const int64_t len, MYSQL_PROTOCOL_TYPE type, int64_t& pos, int64_t cell_index, char* bitmap);
  /**
   * Serialize an integer cell to the position of buf + pos.
   * (ObBoolType, ObIntType)
   */
  static int int_cell_str(char* buf, const int64_t len, int64_t val, const ObObjType obj_type, bool is_unsigned,
      MYSQL_PROTOCOL_TYPE type, int64_t& pos, bool zerofill, int32_t zflength);
  /**
   * Serialize a fixed-point number cell to the position of buf + pos.
   * (ObDecimalType)
   */
  static int number_cell_str(char* buf, const int64_t len, const number::ObNumber& val, int64_t& pos, int16_t scale,
      bool zerofill, int32_t zflength);
  /**
   * Serialize a datetime type cell to the position of buf + pos.
   * (ObDateTimeType, ObTimestampType)
   */
  static int datetime_cell_str(char* buf, const int64_t len, int64_t val, MYSQL_PROTOCOL_TYPE type, int64_t& pos,
      const ObTimeZoneInfo* tz_info, int16_t scale);

  /**
   * Serialize an obstring type cell to the position of buf + pos.
   * (ObDateTimeType, ObTimestampType)
   */
  static int write_segment_str(char* buf, const int64_t len, int64_t& pos, const common::ObString& str);

  /**
   * Serialize an otimestamp type cell to the position of buf + pos.
   * (ObDateTimeType, ObTimestampType)
   */
  static int otimestamp_cell_str(char* buf, const int64_t len, const ObOTimestampData& ot_data,
      MYSQL_PROTOCOL_TYPE type, int64_t& pos, const ObDataTypeCastParams& dtc_params, const int16_t scale,
      const ObObjType obj_type);
  static int otimestamp_cell_str2(char* buf, const int64_t len, const ObOTimestampData& ot_data,
      MYSQL_PROTOCOL_TYPE type, int64_t& pos, const ObDataTypeCastParams& dtc_params, const int16_t scale,
      const ObObjType obj_type);
  /**
   * Serialize a date type cell to the position of buf + pos.
   * (ObDateType)
   */
  static int date_cell_str(char* buf, const int64_t len, int32_t val, MYSQL_PROTOCOL_TYPE type, int64_t& pos);
  /**
   * Serialize a time type cell to the position of buf + pos.
   * (ObTimeType)
   */
  static int time_cell_str(
      char* buf, const int64_t len, int64_t val, MYSQL_PROTOCOL_TYPE type, int64_t& pos, int16_t scale);
  /**
   * Serialize a year type cell to the position of buf + pos.
   * (ObYearType)
   */
  static int year_cell_str(char* buf, const int64_t len, uint8_t val, MYSQL_PROTOCOL_TYPE type, int64_t& pos);
  /**
   * Serialize a string type cell to the position of buf + pos.
   * (ObVarcharType)
   */
  static int varchar_cell_str(char* buf, int64_t len, const ObString& val, const bool is_oracle_raw, int64_t& pos);
  /**
   * Serialize a floating-point number cell to the position of buf + pos.
   * (ObFloatType, ObDoubleType)
   */
  static int float_cell_str(char* buf, const int64_t len, float val, MYSQL_PROTOCOL_TYPE type, int64_t& pos,
      int16_t scale, bool zerofill, int32_t zflength);
  static int double_cell_str(char* buf, const int64_t len, double val, MYSQL_PROTOCOL_TYPE type, int64_t& pos,
      int16_t scale, bool zerofill, int32_t zflength);
  static int bit_cell_str(
      char* buf, const int64_t len, uint64_t val, int32_t bit_len, MYSQL_PROTOCOL_TYPE type, int64_t& pos);
  static int interval_ym_cell_str(
      char* buf, const int64_t len, ObIntervalYMValue val, MYSQL_PROTOCOL_TYPE type, int64_t& pos, const ObScale scale);
  static int interval_ds_cell_str(
      char* buf, const int64_t len, ObIntervalDSValue val, MYSQL_PROTOCOL_TYPE type, int64_t& pos, const ObScale scale);

  static int urowid_cell_str(char* buf, const int64_t len, const common::ObURowIDData& urowid_data, int64_t& pos);
  static int lob_locator_cell_str(char* buf, const int64_t len, const common::ObLobLocator& lob_locator, int64_t& pos);

public:
  static const uint64_t NULL_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMySQLUtil);
};  // class ObMySQLUtil

int ObMySQLUtil::store_null(char* buf, int64_t len, int64_t& pos)
{
  return store_int1(buf, len, static_cast<int8_t>(251), pos);
}

int ObMySQLUtil::store_int1(char* buf, int64_t len, int8_t v, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KP(buf), K(ret));
  } else {
    if (len >= pos + 1) {
      ob_int1store(buf + pos, v);
      pos++;
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ObMySQLUtil::store_int2(char* buf, int64_t len, int16_t v, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KP(buf), K(ret));
  } else {
    if (len >= pos + 2) {
      ob_int2store(buf + pos, v);
      pos += 2;
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ObMySQLUtil::store_int3(char* buf, int64_t len, int32_t v, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KP(buf), K(ret));
  } else {
    if (len >= pos + 3) {
      ob_int3store(buf + pos, v);
      pos += 3;
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ObMySQLUtil::store_int4(char* buf, int64_t len, int32_t v, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KP(buf), K(ret));
  } else {
    if (len >= pos + 4) {
      ob_int4store(buf + pos, v);
      pos += 4;
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ObMySQLUtil::store_int5(char* buf, int64_t len, int64_t v, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KP(buf), K(ret));
  } else {
    if (len >= pos + 5) {
      ob_int5store(buf + pos, v);
      pos += 5;
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ObMySQLUtil::store_int6(char* buf, int64_t len, int64_t v, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KP(buf), K(ret));
  } else {
    if (len >= pos + 6) {
      ob_int6store(buf + pos, v);
      pos += 6;
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ObMySQLUtil::store_int8(char* buf, int64_t len, int64_t v, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KP(buf), K(ret));
  } else {
    if (len >= pos + 8) {
      ob_int8store(buf + pos, v);
      pos += 8;
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

void ObMySQLUtil::get_int1(const char*& pos, int8_t& v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG(WARN, "invalid argument", K(pos));
  } else {
    v = ob_sint1korr(pos);
    pos++;
  }
}
void ObMySQLUtil::get_int2(const char*& pos, int16_t& v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG(WARN, "invalid argument", K(pos));
  } else {
    v = ob_sint2korr(pos);
    pos += 2;
  }
}
void ObMySQLUtil::get_int3(const char*& pos, int32_t& v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG(WARN, "invalid argument", K(pos));
  } else {
    v = ob_sint3korr(pos);
    pos += 3;
  }
}
void ObMySQLUtil::get_int4(const char*& pos, int32_t& v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG(WARN, "invalid argument", K(pos));
  } else {
    v = ob_sint4korr(pos);
    pos += 4;
  }
}
void ObMySQLUtil::get_int8(const char*& pos, int64_t& v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG(WARN, "invalid argument", K(pos));
  } else {
    v = ob_sint8korr(pos);
    pos += 8;
  }
}

void ObMySQLUtil::get_uint1(const char*& pos, uint8_t& v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG(WARN, "invalid argument", K(pos));
  } else {
    v = ob_uint1korr(pos);
    pos++;
  }
}
void ObMySQLUtil::get_uint2(const char*& pos, uint16_t& v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG(WARN, "invalid argument", K(pos));
  } else {
    v = ob_uint2korr(pos);
    pos += 2;
  }
}
void ObMySQLUtil::get_uint3(const char*& pos, uint32_t& v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG(WARN, "invalid argument", K(pos));
  } else {
    v = ob_uint3korr(pos);
    pos += 3;
  }
}
void ObMySQLUtil::get_uint4(const char*& pos, uint32_t& v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG(WARN, "invalid argument", K(pos));
  } else {
    v = ob_uint4korr(pos);
    pos += 4;
  }
}
void ObMySQLUtil::get_uint5(const char*& pos, uint64_t& v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG(WARN, "invalid argument", K(pos));
  } else {
    v = ob_uint5korr(pos);
    pos += 5;
  }
}
void ObMySQLUtil::get_uint6(const char*& pos, uint64_t& v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG(WARN, "invalid argument", K(pos));
  } else {
    v = ob_uint6korr(pos);
    pos += 6;
  }
}
void ObMySQLUtil::get_uint8(const char*& pos, uint64_t& v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG(WARN, "invalid argument", K(pos));
  } else {
    v = ob_uint8korr(pos);
    pos += 8;
  }
}

void ObMySQLUtil::get_uint1(char*& pos, uint8_t& v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG(WARN, "invalid argument", K(pos));
  } else {
    v = ob_uint1korr(pos);
    pos++;
  }
}
void ObMySQLUtil::get_uint2(char*& pos, uint16_t& v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG(WARN, "invalid argument", K(pos));
  } else {
    v = ob_uint2korr(pos);
    pos += 2;
  }
}
void ObMySQLUtil::get_uint3(char*& pos, uint32_t& v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG(WARN, "invalid argument", K(pos));
  } else {
    v = ob_uint3korr(pos);
    pos += 3;
  }
}
void ObMySQLUtil::get_uint4(char*& pos, uint32_t& v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG(WARN, "invalid argument", K(pos));
  } else {
    v = ob_uint4korr(pos);
    pos += 4;
  }
}
void ObMySQLUtil::get_uint5(char*& pos, uint64_t& v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG(WARN, "invalid argument", K(pos));
  } else {
    v = ob_uint5korr(pos);
    pos += 5;
  }
}
void ObMySQLUtil::get_uint6(char*& pos, uint64_t& v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG(WARN, "invalid argument", K(pos));
  } else {
    v = ob_uint6korr(pos);
    pos += 6;
  }
}
void ObMySQLUtil::get_uint8(char*& pos, uint64_t& v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG(WARN, "invalid argument", K(pos));
  } else {
    v = ob_uint8korr(pos);
    pos += 8;
  }
}

}  // namespace obmysql
}  // namespace oceanbase

#endif /* _OB_MYSQL_UTIL_H_ */
