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

#include "lib/ob_define.h"
#include <openssl/evp.h>
#include "lib/container/ob_se_array.h"
#include "lib/charset/ob_charset.h"
#include "lib/string/ob_sql_string.h"
#include "lib/number/ob_number_v2.h"
#include "lib/rowid/ob_urowid.h"
#include "share/datum/ob_datum.h"

#ifndef OCEANBASE_SHARE_OB_ORDER_PERSERVING_ENCODER_H
#define OCEANBASE_SHARE_OB_ORDER_PERSERVING_ENCODER_H

namespace oceanbase
{
using namespace common;
using namespace common::number;
namespace share
{
// sort perserving encoder mode
enum ObSorEncMode {
  OB_INVALID_MODE = 0,
  OB_NF = 1,        // compare nulls first
  OB_NL = 2,        // compare nulls last
  OB_FT_ORC = 3,    // need field termination in oracle mode
  OB_FT_MYSQL = 4,  // need field termnination in mysql mode
  OB_DESC = 5,      // descrease, Notes: asc by default
  OB_NONE = 6,      // do nothing
  OB_CMP_CB = 7,    // combine bytes to compare
  OB_MAX_MODE
};

class ObEncParam {
public:
  ObCollationType cs_type_;
  bool is_var_len_;
  bool is_asc_;
  bool is_nullable_;
  bool is_null_first_;
  bool is_memcmp_;
  bool is_valid_uni_;
  ObObjType type_;
  ObSorEncMode enc_mod_;

  ObEncParam()
    : cs_type_(ObCollationType::CS_TYPE_INVALID),
      is_var_len_(false),
      is_asc_(false),
      is_nullable_(false),
      is_null_first_(false),
      is_memcmp_(false),
      is_valid_uni_(true),
      type_(ObObjType::ObNullType),
      enc_mod_(ObSorEncMode::OB_INVALID_MODE)
  {}

  TO_STRING_KV(K_(cs_type),
               K_(is_var_len),
               K_(is_memcmp),
               K_(is_valid_uni),
               K_(type),
               K_(is_asc),
               K_(is_nullable),
               K_(is_null_first),
               K_(enc_mod));
};

class ObOrderPerservingEncoder {
public:
  //
  static int make_order_perserving_encode_from_object(
    ObDatum &data, unsigned char *to, int64_t max_buf_len, int64_t &to_len, ObEncParam &param);

  static int make_order_perserving_encode_from_object(ObObj &obj,
                                                      unsigned char *to,
                                                      int64_t max_buf_len,
                                                      int64_t &to_len);

  static int encode_from_string_fixlen(
    ObString val, unsigned char *to, int64_t max_buf_len, int64_t &to_len, ObEncParam &param);

  static int encode_from_string_varlen(
    ObString val, unsigned char *to, int64_t max_buf_len, int64_t &to_len, ObEncParam &param);
  static int encode_from_string_varlen(
    ObString val, unsigned char *to, int64_t max_buf_len, int64_t &to_len, ObCollationType cs);
  static int encode_from_int8(int8_t val, unsigned char *to, int64_t &to_len);
  static int encode_from_int16(int16_t val, unsigned char *to, int64_t &to_len);
  static int encode_from_int32(int32_t val, unsigned char *to, int64_t &to_len);
  static int encode_from_int(int64_t val, unsigned char *to, int64_t &to_len);

  static int encode_from_uint8(uint8_t val, unsigned char *to, int64_t &to_len);
  static int encode_from_uint16(uint16_t val, unsigned char *to, int64_t &to_len);
  static int encode_from_uint32(uint32_t val, unsigned char *to, int64_t &to_len);
  static int encode_from_uint(uint64_t val, unsigned char *to, int64_t &to_len);

  static int encode_from_double(double val, unsigned char *to, int64_t &to_len);

  static int encode_from_float(float val, unsigned char *to, int64_t &to_len);

  static int encode_from_number(ObNumber val,
                                unsigned char *to,
                                int64_t max_buf_len,
                                int64_t &to_len);

  static int encode_from_timestamp(ObOTimestampData val, unsigned char *to, int64_t &to_len);

  static int encode_from_interval_ds(ObIntervalDSValue val, unsigned char *to, int64_t &to_len);
  static int encode_tails(unsigned char *to, int64_t max_buf_len, int64_t &to_len, bool is_mem, common::ObCollationType cs,  bool with_empty_str);
  inline static bool can_encode_sortkey(common::ObObjType type, common::ObCollationType cs)
  {
    return (type == ObTinyIntType || type == ObSmallIntType || type == ObDateType
           || type == ObMediumIntType || type == ObInt32Type || type == ObIntervalYMType
           || type == ObTimeType || type == ObDateTimeType || type == ObTimestampType
           || type == ObIntType || type == ObYearType || type == ObUTinyIntType
           || type == ObUSmallIntType || type == ObUMediumIntType || type == ObUInt32Type
           || type == ObUMediumIntType || type == ObUInt32Type || type == ObUInt64Type
           || type == ObFloatType || type == ObUFloatType || type == ObDoubleType
           || type == ObUDoubleType || type == ObNumberType || type == ObUNumberType
           || type == ObNumberFloatType || type == ObTimestampTZType || type == ObTimestampLTZType
           || type == ObTimestampNanoType || type == ObIntervalDSType || type == ObVarcharType
           || type == ObNVarchar2Type || type == ObRawType || type == ObNCharType
           || type == ObCharType)
           && (cs == CS_TYPE_COLLATION_FREE || cs == CS_TYPE_BINARY || cs == CS_TYPE_UTF8MB4_BIN
              || cs == CS_TYPE_GBK_BIN || cs == CS_TYPE_GB18030_BIN || cs == CS_TYPE_UTF8MB4_GENERAL_CI
              || cs == CS_TYPE_GBK_CHINESE_CI
              // utf 16 will be open later
              //|| cs == CS_TYPE_UTF16_GENERAL_CI || cs == CS_TYPE_UTF16_BIN
              || cs == CS_TYPE_GB18030_CHINESE_CI || ObCharset::is_gb18030_2022(cs));
  }

private:
  const static uint64_t SIGN_MASK_64 = 0x8000000000000000;
  const static uint32_t SIGN_MASK_32 = 0x80000000;
  const static uint32_t SIGN_MASK_16 = 0x8000;
  const static uint32_t SIGN_MASK_8 = 0x80;
  static int convert_ob_charset_utf8mb4_bin(unsigned char *data,
                                            int64_t len,
                                            unsigned char *to,
                                            int64_t &to_len);
  static int convert_ob_charset_utf8mb4_bin_sp(unsigned char *data,
                                               int64_t len,
                                               unsigned char *to,
                                               int64_t &to_len);
};

// allocator
class ObSortkeyConditioner {
public:
  static int process_key_conditioning(
    ObDatum &data, unsigned char *to, int64_t max_buf_len, int64_t &to_len, ObEncParam &param);
  static int process_key_conditioning(ObObj &obj,
                                      unsigned char *to,
                                      int64_t max_buf_len,
                                      int64_t &to_len);
  // adds null position flag
  static void process_decrease(unsigned char *to, int64_t to_len);
  // static int process_compare_by_combined_bytes()
};

}  // namespace share
}  // namespace oceanbase
#endif
