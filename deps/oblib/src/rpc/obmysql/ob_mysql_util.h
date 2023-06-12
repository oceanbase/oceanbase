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
#include <float.h>              // for FLT_DIG and DBL_DIG
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/number/ob_number_v2.h"
#include "lib/timezone/ob_timezone_info.h"
#include "rpc/obmysql/ob_mysql_global.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace common
{
class ObOTimestampData;
struct ObURowIDData;
struct ObLobLocator;
class ObDataTypeCastParams;
}
namespace obmysql
{
enum MYSQL_PROTOCOL_TYPE
{
  TEXT = 1,
  BINARY,
};

class ObMySQLUtil
{
public:
  /**
   * update null bitmap for binary protocol
   * please review http://dev.mysql.com/doc/internals/en/prepared-statements.html#null-bitmap for detail
   *
   * @param bitmap[in/out]         input bitmap
   * @param field_index            index of field
   *
   */
  static void update_null_bitmap(char *&bitmap, int64_t field_index);

  /**
   * 将长度写入到buf里面，可能占1,3,4,9个字节。
   *
   * @param [in] buf 要写入长度的buf
   * @param [in] len buf的总字节数
   * @param [in] length 需要写入的长度的值
   * @param [in,out] pos buf已使用的字节数
   *
   * @return oceanbase error code
   */
  static int store_length(char *buf, int64_t len, uint64_t length, int64_t &pos);
  /**
   * 读取长度字段，配合 store_length() 使用
   *
   * @param [in,out] pos 读取长度的位置，读取后更新pos
   * @param [out] length 长度
   */
  static int get_length(const char *&pos, uint64_t &length);
  /**
   * 读取长度字段，配合 store_length() 使用
   *
   * @param [in,out] pos 读取长度的位置，读取后更新pos
   * @param [out] length 长度
   * @param [out] pos_inc_len pos增加的长度
   */
  static int get_length(const char *&pos, uint64_t &length, uint64_t &pos_inc_len);
  /**
   * 以length coded string格式存储数字时，需要占用的长度
   *
   * @param [in] 数字
   * @return 存储时需要字节数
   */
  static uint64_t get_number_store_len(const uint64_t num);
  /**
   * 存一个 NULL 标志。
   *
   * @param buf 写入的内存地址
   * @param len buf的总字节数
   * @param pos buf已写入的字节数
   */
  static inline int store_null(char *buf, int64_t len, int64_t &pos);
  /**
   * @see store_str_v()
   * 将一段以'\0'结尾的字符串str打包进入buf（length coded string格式）
   *
   * @param [in] buf 写入的内存地址
   * @param [in] len buf的总字节数
   * @param [in] str 需要打包的字符串
   * @param [in,out] pos buf已使用的字节数
   *
   * @return OB_SUCCESS or oceanbase error code.
   *
   * @see store_length()
   */
  static int store_str(char *buf, int64_t len, const char *str, int64_t &pos);
  /**
   * 将一段长度为length的字符串str打包进入buf（length coded string格式）
   *
   * @param [in] buf 写入的内存地址
   * @param [in] len buf的总字节数
   * @param [in] str 需要打包的字符串
   * @param [in] length str的长度
   * @param [in,out] pos buf已使用的字节数
   *
   * @return OB_SUCCESS or oceanbase error code.
   *
   * @see store_str()
   * @see store_length()
   */
  static int store_str_v(char *buf, int64_t len, const char *str,
                         const uint64_t length, int64_t &pos);
  /**
   * 将ObString结构的字符串str打包进入buf（length coded string格式）
   *
   * @param [in] buf 写入的内存地址
   * @param [in] len buf的总字节数
   * @param [in] str ObString结构的字符串
   * @param [in,out] pos buf已使用的字节数
   *
   * @return OB_SUCCESS or oceanbase error code.
   *
   * @see store_length()
   * @see store_str_v()
   */
  static int store_obstr(char *buf, int64_t len, ObString str, int64_t &pos);
  static int store_obstr_with_pre_space(char *buf, int64_t len, ObString str, int64_t &pos);
  /**
   * @see store_str_vzt()
   * 将一段以'\0'结尾的字符串str打包进入buf（zero terminated string格式）
   *
   * @param [in] buf 写入的内存地址
   * @param [in] len buf的总字节数
   * @param [in] str 需要打包的字符串
   * @param [in,out] pos buf已使用的字节数
   *
   * @return OB_SUCCESS or oceanbase error code.
   */
  static int store_str_zt(char *buf, int64_t len, const char *str, int64_t &pos);
  /**
   * @see store_str_vzt()
   * 将一段以'\0'结尾的字符串str打包进入buf（none-zero terminated string格式）
   *
   * @param [in] buf 写入的内存地址
   * @param [in] len buf的总字节数
   * @param [in] str 需要打包的字符串
   * @param [in,out] pos buf已使用的字节数
   *
   * @return OB_SUCCESS or oceanbase error code.
   */
  static int store_str_nzt(char *buf, int64_t len, const char *str, int64_t &pos);
  /**
   * @see store_str_zt()
   * 将一段长度为length的字符串str打包进入buf（zero terminated string格式）
   *
   * @param [in] buf 写入的内存地址
    * @param [in] len buf的总字节数
   * @param [in] str 需要打包的字符串
   * @param [in] length str的长度（不包括'\0'）
   * @param [in,out] pos buf已使用的字节数
   *
   * @return OB_SUCCESS or oceanbase error code.
   */
  static int store_str_vzt(char *buf, int64_t len, const char *str,
                           const uint64_t length, int64_t &pos);
  /**
   * @see store_str_zt()
   * 将一段长度为length的字符串str打包进入buf（none-zero terminated string格式）
   *
   * @param [in] buf 写入的内存地址
   * @param [in] len buf的总字节数
   * @param [in] str 需要打包的字符串
   * @param [in] length str的长度（不包括'\0'）
   * @param [in,out] pos buf已使用的字节数
   *
   * @return OB_SUCCESS or oceanbase error code.
   */
  static int store_str_vnzt(char *buf, int64_t len,
                            const char *str, int64_t length, int64_t &pos);
  /**
   * @see store_str_vzt()
   * 将ObString结构的字符串str打包进入buf（zero terminated string格式）
   *
   * @param [in] buf 写入的内存地址
   * @param [in] len buf的总字节数
   * @param [in] str ObString结构的字符串
   * @param [in,out] pos buf已使用的字节数
   *
   * @return OB_SUCCESS or oceanbase error code.
   */
  static int store_obstr_zt(char *buf, int64_t len, ObString str, int64_t &pos);
  /**
   * 将ObString结构的字符串str打包进入buf（none-zero terminated raw string格式）
   *
   * @param [in] buf 写入的内存地址
   * @param [in] len buf的总字节数
   * @param [in] str ObString结构的字符串
   * @param [in,out] pos buf已使用的字节数
   *
   * @return OB_SUCCESS or oceanbase error code.
   */
  static int store_obstr_nzt(char *buf, int64_t len, ObString str, int64_t &pos);
  static int store_obstr_nzt_with_pre_space(char *buf, int64_t len, ObString str, int64_t &pos);
  //@{Serialize integer data, save the data in v to the position of buf+pos, and update pos
  static inline int store_int1(char *buf, int64_t len, int8_t v, int64_t &pos);
  static inline int store_int2(char *buf, int64_t len, int16_t v, int64_t &pos);
  static inline int store_int3(char *buf, int64_t len, int32_t v, int64_t &pos);
  static inline int store_int4(char *buf, int64_t len, int32_t v, int64_t &pos);
  static inline int store_int5(char *buf, int64_t len, int64_t v, int64_t &pos);
  static inline int store_int6(char *buf, int64_t len, int64_t v, int64_t &pos);
  static inline int store_int8(char *buf, int64_t len, int64_t v, int64_t &pos);
  //@}

  //@{ Signed integer in reverse sequence, write the result to v, and update pos
  static inline void get_int1(const char *&pos, int8_t &v);
  static inline void get_int2(const char *&pos, int16_t &v);
  static inline void get_int3(const char *&pos, int32_t &v);
  static inline void get_int4(const char *&pos, int32_t &v);
  static inline void get_int8(const char *&pos, int64_t &v);
  //@}

  //@{ Deserialize unsigned integer, write the result to v, and update pos
  static inline void get_uint1(const char *&pos, uint8_t &v);
  static inline void get_uint2(const char *&pos, uint16_t &v);
  static inline void get_uint3(const char *&pos, uint32_t &v);
  static inline void get_uint4(const char *&pos, uint32_t &v);
  static inline void get_uint5(const char *&pos, uint64_t &v);
  static inline void get_uint6(const char *&pos, uint64_t &v);
  static inline void get_uint8(const char *&pos, uint64_t &v);

  static inline void get_uint1(char *&pos, uint8_t &v);
  static inline void get_uint2(char *&pos, uint16_t &v);
  static inline void get_uint3(char *&pos, uint32_t &v);
  static inline void get_uint4(char *&pos, uint32_t &v);
  static inline void get_uint5(char *&pos, uint64_t &v);
  static inline void get_uint6(char *&pos, uint64_t &v);
  static inline void get_uint8(char *&pos, uint64_t &v);
//@}


  /**
   * 在buf[0...offset)处添0，原来的org_char_size字节数据往后移动offset字节
   */
  static void prepend_zeros(char *buf, const int64_t org_char_size, int64_t offset);


  /**
   * 序列化一个null类型的cell到buf + pos的位置。
   *
   * @param [in] obj 需要序列化的cell
   * @param [in] buf 输出的buf
   * @param [in] len buf的大小
   * @param [in,out] pos 写入buf的位置
   * @param [in] field index
   *
   * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
   */
  static int null_cell_str(char *buf, const int64_t len,
                           MYSQL_PROTOCOL_TYPE type, int64_t &pos,
                           int64_t cell_index, char *bitmap);
  /**
   * 序列化一个整型的cell到buf + pos的位置。
   * (ObBoolType, ObIntType)
   *
   * @param [in] obj 需要序列化的cell
   * @param [in] buf 输出的buf
   * @param [in] len buf的大小
   * @param [in,out] pos 写入buf的位置
   *
   * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
   */
  static int int_cell_str(char *buf, const int64_t len, int64_t val,
                          const ObObjType obj_type,
                          bool is_unsigned,
                          MYSQL_PROTOCOL_TYPE type, int64_t &pos,
                          bool zerofill, int32_t zflength);
  /**
   * 序列化一个定点数型的cell到buf + pos的位置。
   * (ObDecimalType)
   *
   * @param [in] obj 需要序列化的cell
   * @param [in] buf 输出的buf
   * @param [in] len buf的大小
   * @param [in,out] pos 写入buf的位置
   *
   * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
   */
  static int number_cell_str(char *buf, const int64_t len,
                             const number::ObNumber &val, int64_t &pos, int16_t scale,
                             bool zerofill, int32_t zflength);
  /**
   * 序列化一个datetime型的cell到buf + pos的位置。
   * (ObDateTimeType, ObTimestampType)
   *
   * @param [in] obj 需要序列化的cell
   * @param [in] buf 输出的buf
   * @param [in] len buf的大小
   * @param [in,out] pos 写入buf的位置
   *
   * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
   */
  static int datetime_cell_str(char *buf, const int64_t len,
                               int64_t val, MYSQL_PROTOCOL_TYPE type, int64_t &pos,
                               const ObTimeZoneInfo *tz_info, int16_t scale);

  /**
   * 序列化一个obstring型的cell到buf + pos的位置。
   * (ObDateTimeType, ObTimestampType)
   *
   * @param [in] obj 需要序列化的cell
   * @param [in] buf 输出的buf
   * @param [in] len buf的大小
   * @param [in,out] pos 写入buf的位置
   *
   * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
   */
  static int write_segment_str(char *buf, const int64_t len, int64_t &pos, const common::ObString &str);

  /**
   * 序列化一个otimestamp型的cell到buf + pos的位置。
   * (ObDateTimeType, ObTimestampType)
   *
   * @param [in] obj 需要序列化的cell
   * @param [in] buf 输出的buf
   * @param [in] len buf的大小
   * @param [in,out] pos 写入buf的位置
   *
   * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
   */
  static int otimestamp_cell_str(char *buf, const int64_t len,
                                 const ObOTimestampData &ot_data, MYSQL_PROTOCOL_TYPE type, int64_t &pos,
                                 const ObDataTypeCastParams &dtc_params, const int16_t scale, const ObObjType obj_type);
  static int otimestamp_cell_str2(char *buf, const int64_t len,
                                 const ObOTimestampData &ot_data, MYSQL_PROTOCOL_TYPE type, int64_t &pos,
                                 const ObDataTypeCastParams &dtc_params, const int16_t scale, const ObObjType obj_type);
  /**
   * 序列化一个date型的cell到buf + pos的位置。
   * (ObDateType)
   *
   * @param [in] obj 需要序列化的cell
   * @param [in] buf 输出的buf
   * @param [in] len buf的大小
   * @param [in,out] pos 写入buf的位置
   *
   * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
   */
  static int date_cell_str(char *buf, const int64_t len,
                           int32_t val, MYSQL_PROTOCOL_TYPE type, int64_t &pos);
  /**
   * 序列化一个time型的cell到buf + pos的位置。
   * (ObTimeType)
   *
   * @param [in] obj 需要序列化的cell
   * @param [in] buf 输出的buf
   * @param [in] len buf的大小
   * @param [in,out] pos 写入buf的位置
   *
   * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
   */
  static int time_cell_str(char *buf, const int64_t len,
                           int64_t val, MYSQL_PROTOCOL_TYPE type, int64_t &pos, int16_t scale);
  /**
   * 序列化一个year型的cell到buf + pos的位置。
   * (ObYearType)
   *
   * @param [in] obj 需要序列化的cell
   * @param [in] buf 输出的buf
   * @param [in] len buf的大小
   * @param [in,out] pos 写入buf的位置
   *
   * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
   */
  static int year_cell_str(char *buf, const int64_t len,
                           uint8_t val, MYSQL_PROTOCOL_TYPE type, int64_t &pos);
  /**
   * 序列化一个字符串类型的cell到buf + pos的位置。
   * (ObVarcharType)
   *
   * @param [in] obj 需要序列化的cell
   * @param [in] buf 输出的buf
   * @param [in] len buf的大小
   * @param [in,out] pos 写入buf的位置
   *
   * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
   */
  static int varchar_cell_str(char *buf, int64_t len, const ObString &val,
                              const bool is_oracle_raw, int64_t &pos);
  /**
   * 序列化一个浮点数型的cell到buf + pos的位置。
   * (ObFloatType, ObDoubleType)
   *
   * @param [in] obj 需要序列化的cell
   * @param [in] buf 输出的buf
   * @param [in] len buf的大小
   * @param [in,out] pos 写入buf的位置
   *
   * @return 成功返回OB_SUCCESS， 失败返回oceanbase error code
   */
  static int float_cell_str(char *buf, const int64_t len, float val,
                            MYSQL_PROTOCOL_TYPE type, int64_t &pos, int16_t scale,
                            bool zerofill, int32_t zflength);
  static int double_cell_str(char *buf, const int64_t len, double val,
                             MYSQL_PROTOCOL_TYPE type, int64_t &pos, int16_t scale,
                             bool zerofill, int32_t zflength);
  static int bit_cell_str(char *buf, const int64_t len, uint64_t val,
                          int32_t bit_len, MYSQL_PROTOCOL_TYPE type, int64_t &pos);
  static int interval_ym_cell_str(char *buf, const int64_t len, ObIntervalYMValue val,
                                  MYSQL_PROTOCOL_TYPE type, int64_t &pos, const ObScale scale);
  static int interval_ds_cell_str(char *buf, const int64_t len, ObIntervalDSValue val,
                                  MYSQL_PROTOCOL_TYPE type, int64_t &pos, const ObScale scale);

  static int urowid_cell_str(char *buf, const int64_t len, const common::ObURowIDData &urowid_data,
                             int64_t &pos);
  static int lob_locator_cell_str(char *buf, const int64_t len,
                                  const common::ObLobLocator &lob_locator, int64_t &pos);
  static int json_cell_str(uint64_t tenant_id, char *buf, const int64_t len, const ObString &val, int64_t &pos);
  static int sql_utd_cell_str(uint64_t tenant_id, char *buf, const int64_t len, const ObString &val, int64_t &pos);
  static int geometry_cell_str(char *buf, const int64_t len, const ObString &val, int64_t &pos);
  static inline int16_t float_length(const int16_t scale);
public:
  static const uint64_t NULL_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMySQLUtil);
}; // class ObMySQLUtil

int ObMySQLUtil::store_null(char *buf, int64_t len, int64_t &pos)
{
  return store_int1(buf, len, static_cast<int8_t>(251), pos);
}

int ObMySQLUtil::store_int1(char *buf, int64_t len, int8_t v, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KP(buf), K(ret));
  } else {
    if (len >= pos + 1) {
      int1store(buf + pos, v);
      pos++;
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ObMySQLUtil::store_int2(char *buf, int64_t len, int16_t v, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KP(buf), K(ret));
  } else {
    if (len >= pos + 2) {
      int2store(buf + pos, v);
      pos += 2;
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ObMySQLUtil::store_int3(char *buf, int64_t len, int32_t v, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KP(buf), K(ret));
  } else {
    if (len >= pos + 3) {
      int3store(buf + pos, v);
      pos += 3;
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ObMySQLUtil::store_int4(char *buf, int64_t len, int32_t v, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KP(buf), K(ret));
  } else {
    if (len >= pos + 4) {
      int4store(buf + pos, v);
      pos += 4;
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ObMySQLUtil::store_int5(char *buf, int64_t len, int64_t v, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KP(buf), K(ret));
  } else {
    if (len >= pos + 5) {
      int5store(buf + pos, v);
      pos += 5;
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ObMySQLUtil::store_int6(char *buf, int64_t len, int64_t v, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KP(buf), K(ret));
  } else {
    if (len >= pos + 6) {
      int6store(buf + pos, v);
      pos += 6;
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ObMySQLUtil::store_int8(char *buf, int64_t len, int64_t v, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KP(buf), K(ret));
  } else {
    if (len >= pos + 8) {
      int8store(buf + pos, v);
      pos += 8;
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

void ObMySQLUtil::get_int1(const char *&pos, int8_t &v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid argument", KP(pos));
  } else {
    v = sint1korr(pos);
    pos++;
  }
}
void ObMySQLUtil::get_int2(const char *&pos, int16_t &v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid argument", KP(pos));
  } else {
    v = sint2korr(pos);
    pos += 2;
  }
}
void ObMySQLUtil::get_int3(const char *&pos, int32_t &v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid argument", KP(pos));
  } else {
    v = sint3korr(pos);
    pos += 3;
  }
}
void ObMySQLUtil::get_int4(const char *&pos, int32_t &v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid argument", KP(pos));
  } else {
    v = sint4korr(pos);
    pos += 4;
  }
}
void ObMySQLUtil::get_int8(const char *&pos, int64_t &v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid argument", KP(pos));
  } else {
    v = sint8korr(pos);
    pos += 8;
  }
}


void ObMySQLUtil::get_uint1(const char *&pos, uint8_t &v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid argument", KP(pos));
  } else {
    v = uint1korr(pos);
    pos ++;
  }
}
void ObMySQLUtil::get_uint2(const char *&pos, uint16_t &v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid argument", KP(pos));
  } else {
    v = uint2korr(pos);
    pos += 2;
  }
}
void ObMySQLUtil::get_uint3(const char *&pos, uint32_t &v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid argument", KP(pos));
  } else {
    v = uint3korr(pos);
    pos += 3;
  }
}
void ObMySQLUtil::get_uint4(const char *&pos, uint32_t &v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid argument", KP(pos));
  } else {
    v = uint4korr(pos);
    pos += 4;
  }
}
void ObMySQLUtil::get_uint5(const char *&pos, uint64_t &v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid argument", KP(pos));
  } else {
    v = uint5korr(pos);
    pos += 5;
  }
}
void ObMySQLUtil::get_uint6(const char *&pos, uint64_t &v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid argument", KP(pos));
  } else {
    v = uint6korr(pos);
    pos += 6;
  }
}
void ObMySQLUtil::get_uint8(const char *&pos, uint64_t &v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid argument", KP(pos));
  } else {
    v = uint8korr(pos);
    pos += 8;
  }
}

void ObMySQLUtil::get_uint1(char *&pos, uint8_t &v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid argument", KP(pos));
  } else {
    v = uint1korr(pos);
    pos ++;
  }
}
void ObMySQLUtil::get_uint2(char *&pos, uint16_t &v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid argument", KP(pos));
  } else {
    v = uint2korr(pos);
    pos += 2;
  }
}
void ObMySQLUtil::get_uint3(char *&pos, uint32_t &v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid argument", KP(pos));
  } else {
    v = uint3korr(pos);
    pos += 3;
  }
}
void ObMySQLUtil::get_uint4(char *&pos, uint32_t &v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid argument", KP(pos));
  } else {
    v = uint4korr(pos);
    pos += 4;
  }
}
void ObMySQLUtil::get_uint5(char *&pos, uint64_t &v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid argument", KP(pos));
  } else {
    v = uint5korr(pos);
    pos += 5;
  }
}
void ObMySQLUtil::get_uint6(char *&pos, uint64_t &v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid argument", KP(pos));
  } else {
    v = uint6korr(pos);
    pos += 6;
  }
}
void ObMySQLUtil::get_uint8(char *&pos, uint64_t &v)
{
  if (OB_ISNULL(pos)) {
    OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid argument", KP(pos));
  } else {
    v = uint8korr(pos);
    pos += 8;
  }
}

/*
 * get precision for double type, keep same with MySQL
 */
int16_t ObMySQLUtil::float_length(const int16_t scale)
{
  return (scale >= 0 && scale <= OB_MAX_DOUBLE_FLOAT_SCALE) ? DBL_DIG + 2 + scale : DBL_DIG + 8;
}

}      // namespace obmysql
}      // namespace oceanbase

#endif /* _OB_MYSQL_UTIL_H_ */
