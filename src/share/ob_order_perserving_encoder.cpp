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

#define USING_LOG_PREFIX COMMON

#include "share/ob_order_perserving_encoder.h"
#include <byteswap.h>
#include "common/ob_target_specific.h"
#include "storage/blocksstable/encoding/ob_encoding_query_util.h"
#if OB_USE_MULTITARGET_CODE
#include <emmintrin.h>
#include <immintrin.h>
#endif

namespace oceanbase
{
using namespace common;
namespace share
{

// used for memcmp comparsion
OB_DECLARE_AVX2_SPECIFIC_CODE(

static constexpr int AVX2_SIZE = sizeof(__m256i);

inline bool check_terminator_simd(unsigned char *data, int64_t len, char ch)
{
  bool ret = true;
  uchar *end = data + len;
  __m256i spch_bin = _mm256_set1_epi8(ch);
  for (;ret && data + AVX2_SIZE <= end;) {
    __m256i val = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(data));
    __m256i cmp = _mm256_cmpeq_epi8(val, spch_bin);
    uint32_t mask = _mm256_movemask_epi8(cmp);
    if (0 != mask) {
      ret = false;
    }
    data += AVX2_SIZE;
  }

  while (ret && data < end) {
    if (*data == ch) {
      ret = false;
    }
    data++;
  }
  return ret;
}
)

int ObOrderPerservingEncoder::make_order_perserving_encode_from_object(
  ObDatum &data, unsigned char *to, int64_t max_buf_len, int64_t &to_len, ObEncParam &param)
{
  int ret = OB_SUCCESS;
  switch (param.type_) {
    // for integer values
    case ObTinyIntType: {
      if (to_len + sizeof(int8_t) > max_buf_len) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_TRACE("no enough memory to do encoding", K(ret), K(param.type_));
      } else {
        encode_from_int8(data.get_tinyint(), to, to_len);
      }
      break;
    }
    case ObSmallIntType: {
      if (to_len + sizeof(int16_t) > max_buf_len) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_TRACE("no enough memory to do encoding", K(ret), K(param.type_));
      } else {
        encode_from_int16(data.get_smallint(), to, to_len);
      }
      break;
    }
    case ObDateType:
    case ObMySQLDateType:
    case ObMediumIntType:
    case ObInt32Type: {
      if (to_len + sizeof(int32_t) > max_buf_len) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_TRACE("no enough memory to do encoding", K(ret), K(param.type_));
      } else {
        encode_from_int32(data.get_int32(), to, to_len);
      }
      break;
    }
    case ObIntervalYMType:
    case ObTimeType:
    case ObDateTimeType:
    case ObMySQLDateTimeType:
    case ObTimestampType:
    case ObIntType: {
      if (to_len + sizeof(int64_t) > max_buf_len) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_TRACE("no enough memory to do encoding", K(ret), K(param.type_));
      } else {
        encode_from_int(data.get_int(), to, to_len);
      }
      break;
    }
    case ObYearType:
    case ObUTinyIntType: {
      if (to_len + sizeof(uint8_t) > max_buf_len) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_TRACE("no enough memory to do encoding", K(ret), K(param.type_));
      } else {
        encode_from_uint8(data.get_utinyint(), to, to_len);
      }
      break;
    }
    case ObUSmallIntType: {
      if (to_len + sizeof(uint16_t) > max_buf_len) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_TRACE("no enough memory to do encoding", K(ret), K(param.type_));
      } else {
        encode_from_uint16(data.get_usmallint(), to, to_len);
      }
      break;
    }
    case ObUMediumIntType:
    case ObUInt32Type: {
      if (to_len + sizeof(uint32_t) > max_buf_len) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_TRACE("no enough memory to do encoding", K(ret), K(param.type_));
      } else {
        encode_from_uint32(data.get_uint32(), to, to_len);
      }
      break;
    }
    case ObUInt64Type: {
      if (to_len + sizeof(uint64_t) > max_buf_len) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_TRACE("no enough memory to do encoding", K(ret), K(param.type_));
      } else {
        encode_from_uint(data.get_uint(), to, to_len);
      }
      break;
    }
    // for float values
    case ObFloatType:
    case ObUFloatType: {
      if (to_len + sizeof(float) > max_buf_len) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_TRACE("no enough memory to do encoding", K(ret), K(param.type_));
      } else {
        encode_from_float(data.get_float(), to, to_len);
      }
      break;
    }
    case ObDoubleType:
    case ObUDoubleType: {
      if (to_len + sizeof(double) > max_buf_len) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_TRACE("no enough memory to do encoding", K(ret), K(param.type_));
      } else {
        encode_from_double(data.get_double(), to, to_len);
      }
      break;
    }
    // for obnumber
    case ObNumberType:
    case ObUNumberType:
    case ObNumberFloatType: {
      if (OB_FAIL(encode_from_number(data.get_number(), to, max_buf_len, to_len))) {
        if (ret == OB_BUF_NOT_ENOUGH) {
          // ignore ret
        } else {
          LOG_WARN("failed to encode number", K(ret));
        }
      }
      break;
    }
    // for date
    case ObTimestampTZType: {
      if (to_len + sizeof(int64_t) + sizeof(uint16_t) > max_buf_len) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_TRACE("no enough memory to do encoding", K(ret), K(param.type_));
      } else {
        encode_from_timestamp(data.get_otimestamp_tz(), to, to_len);
      }
      break;
    }
    case ObTimestampLTZType:
    case ObTimestampNanoType: {
      if (to_len + sizeof(int64_t) + sizeof(uint16_t) > max_buf_len) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_TRACE("no enough memory to do encoding", K(ret), K(param.type_));
      } else {
        encode_from_timestamp(data.get_otimestamp_tiny(), to, to_len);
      }
      break;
    }
    case ObIntervalDSType: {
      if (to_len + sizeof(int64_t) + sizeof(int32_t) > max_buf_len) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_TRACE("no enough memory to do encoding", K(ret), K(param.type_));
      } else {
        encode_from_interval_ds(data.get_interval_ds(), to, to_len);
      }
      break;
    }
    case ObRawType:
    case ObVarcharType:
    case ObNVarchar2Type: {
      param.is_var_len_ = false;
      if (OB_FAIL(encode_from_string_varlen(data.get_string(), to, max_buf_len, to_len, param))) {
        if (ret == OB_BUF_NOT_ENOUGH) {
          // ignore ret
        } else {
          LOG_WARN("failed to encode fix len str", K(ret));
        }
      }
      break;
    }
    case ObNCharType:
    case ObCharType: {
      if (OB_FAIL(encode_from_string_varlen(data.get_string(), to, max_buf_len, to_len, param))) {
        if (ret == OB_BUF_NOT_ENOUGH) {
          // ignore ret
        } else {
          LOG_WARN("failed to encode string", K(ret));
        }
      }
      break;
    }
    case ObDecimalIntType: {
      if (to_len + data.len_ > max_buf_len) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_TRACE("no enough memory to do encoding", K(ret), K(data.len_), K(param.type_));
      } else if (OB_FAIL(encode_from_decint(data.get_decimal_int(), data.len_, to, to_len))) {
        LOG_WARN("encode from decimal int failed", K(ret));
      }
      break;
    }
    case ObURowIDType:
    case ObUnknownType:
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType:
    case ObBitType:
    case ObEnumType:
    case ObSetType:
    case ObEnumInnerType:
    case ObSetInnerType:
    case ObLobType:
    case ObExtendType:
    case ObHexStringType:
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("this type cannot make sortkey", K(ret), K(param.type_));
    }
  }

  return ret;
}

// used for memcmp comparsion
int ObOrderPerservingEncoder::convert_ob_charset_utf8mb4_bin(unsigned char *data,
                                                             int64_t len,
                                                             unsigned char *to,
                                                             int64_t &to_len)
{
  unsigned char *d_e = data + len;
  while (data < d_e) {
    *to = *data;
    if (*data == 0x00) {
      to++;
      to_len++;
      *to = 0x01;
    }
    data++;
    to++;
    to_len++;
  }
  *to = 0x00;
  *(to + 1) = 0x00;
  to_len += 2;
  return OB_SUCCESS;
}

// used for space comparsion (0x20)
int ObOrderPerservingEncoder::convert_ob_charset_utf8mb4_bin_sp(unsigned char *data,
                                                                int64_t len,
                                                                unsigned char *to,
                                                                int64_t &to_len)
{
  unsigned char *d_e = data + len;
  while (*(d_e - 1) == 0x20 && d_e - 1 >= data)
    d_e--;

  while (data < d_e) {
    if (*data == 0x20) {
      int16_t sp_cnt = 0;
      while (*data == 0x20) {
        sp_cnt++;
        data++;
        if (data == d_e)
          sp_cnt = 0;
      }

      int16_t sp_cnt_mask = 0;
      int16_t tmp = (int16_t)((*data) - 0x20);
      int16_t x = (~tmp) >> 16;
      MEMCPY(to, (unsigned char *)&x, 2);
      *to = 0x20;
      if (tmp > 0) {
        *(to+1) = 0x21;
        sp_cnt_mask = 0xFFFF;
      } else {
        *(to+1) = 0x19;
        sp_cnt_mask = 0;
      }
      to += 2;
      sp_cnt = ((sp_cnt) ^ sp_cnt_mask) ^ 0x8000;
      sp_cnt = bswap_16(sp_cnt);
      MEMCPY(to, (unsigned char *)&sp_cnt, 2);
      to += 2;
      to_len += 4;
    }

    *to = *data;
    data++;
    to++;
    to_len++;
  }
  *to = 0x20;
  *(to + 1) = 0x20;
  to_len += 2;
  return OB_SUCCESS;
}


int ObOrderPerservingEncoder::encode_from_string_varlen(
  ObString str, unsigned char *to, int64_t max_buf_len, int64_t &to_len, ObEncParam &param)
{
  int ret = OB_SUCCESS;
  ObCollationType cs = param.cs_type_;
  bool is_valid_uni = false;

  int64_t safty_buf_size = 20;
  // tail is up to 8 byte and [space] will be expand to 10byte,
  // therefore safty buffer size round up to 20(byte)
  // and src will only expand 7 times at most when encoding.
  // for bad case
  // [space] A [space] A
  // [space] will expand to 10 byte
  // A will expand to 4 byte
  // therefore src will expand (10+4)/2=>7 times at most when encoding
  if ((to_len + 7 * str.length() + safty_buf_size) > max_buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_TRACE("no enough memory to do encoding for string", K(ret));
  } else if (str.empty() || (str.length()==1 && *str.ptr()=='\0')) {
    if (OB_FAIL(encode_tails(to, max_buf_len, to_len, param.is_memcmp_, cs, str.length()==1 && *str.ptr()=='\0'))) {
      LOG_WARN("failed to encode tails", K(ret));
    }
  } else if (cs == CS_TYPE_COLLATION_FREE || cs == CS_TYPE_BINARY) {
#if OB_USE_MULTITARGET_CODE
    if (param.is_simdopt_ && common::is_arch_supported(ObTargetArch::AVX2)
      && specific::avx2::check_terminator_simd((unsigned char *)str.ptr(), str.length(), 0x00)) {
      MEMCPY(to, str.ptr(), str.length());
      to += str.length();
      *(to) = 0x00;
      *(to+1) = 0x00;
      to += 2;
      to_len += (str.length() + 2);
    } else {
      convert_ob_charset_utf8mb4_bin((unsigned char *)str.ptr(), str.length(), to, to_len);
    }
#else
    convert_ob_charset_utf8mb4_bin((unsigned char *)str.ptr(), str.length(), to, to_len);
#endif
  } else if (cs == CS_TYPE_UTF8MB4_BIN || cs == CS_TYPE_GBK_BIN ||
             cs == CS_TYPE_GB18030_BIN || cs == CS_TYPE_GB18030_2022_BIN) {
    if (param.is_memcmp_) {
#if OB_USE_MULTITARGET_CODE
      if (param.is_simdopt_ && common::is_arch_supported(ObTargetArch::AVX2)
        && specific::avx2::check_terminator_simd((unsigned char *)str.ptr(), str.length(), 0x00)) {
        MEMCPY(to, str.ptr(), str.length());
        to += str.length();
        *(to) = 0x00;
        *(to+1) = 0x00;
        to += 2;
        to_len += (str.length() + 2);
      } else {
        convert_ob_charset_utf8mb4_bin((unsigned char *)str.ptr(), str.length(), to, to_len);
      }
#else
      convert_ob_charset_utf8mb4_bin((unsigned char *)str.ptr(), str.length(), to, to_len);
#endif
    } else {
#if OB_USE_MULTITARGET_CODE
      if (param.is_simdopt_ && common::is_arch_supported(ObTargetArch::AVX2)
        && specific::avx2::check_terminator_simd((unsigned char *)str.ptr(), str.length(), 0x20)) {
        MEMCPY(to, str.ptr(), str.length());
        to += str.length();
        *(to) = 0x20;
        *(to+1) = 0x20;
        to += 2;
        to_len += (str.length() + 2);
      } else {
        convert_ob_charset_utf8mb4_bin_sp((unsigned char *)str.ptr(), str.length(), to, to_len);
      }
#else
      convert_ob_charset_utf8mb4_bin_sp((unsigned char *)str.ptr(), str.length(), to, to_len);
#endif
    }
  } else if (cs == CS_TYPE_UTF8MB4_GENERAL_CI) {
#if OB_USE_MULTITARGET_CODE
    if (param.is_simdopt_ && common::is_arch_supported(ObTargetArch::AVX2)
      && storage::is_ascii_str(str.ptr(), str.length())
      && ((param.is_memcmp_ && specific::avx2::check_terminator_simd((unsigned char *)str.ptr(), str.length(), 0x00))
      || (!param.is_memcmp_ && specific::avx2::check_terminator_simd((unsigned char *)str.ptr(), str.length(), 0x20)))) {
      MEMCPY(to, str.ptr(), str.length());
      str_toupper((char *)to, str.length());
      to += str.length();
      if (param.is_memcmp_) {
        *(to) = 0x00;
        *(to+1) = 0x00;
      } else {
        *(to) = 0x20;
        *(to+1) = 0x20;
      }
      to += 2;
      to_len += (str.length() + 2);
    } else {
      // no simd opt
      int64_t res_len = ObCharset::sortkey_var_len(cs, str.ptr(), str.length(), (char *)to,
                                                 max_buf_len - to_len - safty_buf_size,
                                                 param.is_memcmp_, param.is_valid_uni_);
      if (!param.is_valid_uni_) {
        // invalid unicode, do nothing
      } else {
        to_len += res_len;
      }
    }
#else
    int64_t res_len = ObCharset::sortkey_var_len(cs, str.ptr(), str.length(), (char *)to,
                                                 max_buf_len - to_len - safty_buf_size,
                                                 param.is_memcmp_, param.is_valid_uni_);
    if (!param.is_valid_uni_) {
      // invalid unicode, do nothing
    } else {
      to_len += res_len;
    }
#endif
  } else if (cs == CS_TYPE_GBK_CHINESE_CI
             || cs == CS_TYPE_UTF16_GENERAL_CI || cs == CS_TYPE_UTF16_BIN
             || cs == CS_TYPE_UTF16LE_GENERAL_CI || cs == CS_TYPE_UTF16LE_BIN
             || cs == CS_TYPE_GB18030_CHINESE_CI
             || (CS_TYPE_GB18030_2022_PINYIN_CI <= cs && cs <= CS_TYPE_GB18030_2022_STROKE_CS)) {
    int64_t res_len = ObCharset::sortkey_var_len(cs, str.ptr(), str.length(), (char *)to,
                                                 max_buf_len - to_len - safty_buf_size,
                                                 param.is_memcmp_, param.is_valid_uni_);
    if (!param.is_valid_uni_) {
      // invalid unicode, do nothing
    } else {
      to_len += res_len;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_TRACE("not support collation", K(cs));
  }
  return ret;
}

int ObOrderPerservingEncoder::encode_from_string_fixlen(
  ObString str, unsigned char *to, int64_t max_buf_len, int64_t &to_len, ObEncParam &param)
{
  int ret = OB_SUCCESS;
  ObCollationType cs = param.cs_type_;
  bool is_valid_uni = false;
  if ((to_len + 4 * str.length() + 2) > max_buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_TRACE("no enough memory to do encoding for fixed string", K(ret));
  } else if (cs == CS_TYPE_COLLATION_FREE || cs == CS_TYPE_BINARY || cs == CS_TYPE_UTF8MB4_BIN
             || cs == CS_TYPE_GBK_BIN || cs == CS_TYPE_GB18030_BIN || cs == CS_TYPE_GB18030_2022_BIN) {
    MEMCPY(to, str.ptr(), str.length());
    to_len += str.length();
  } else {
    to_len
      += ObCharset::sortkey(cs, str.ptr(), str.length(), (char *)to, max_buf_len, is_valid_uni);
  }
  return ret;
}

int ObOrderPerservingEncoder::encode_from_int8(int8_t val, unsigned char *to, int64_t &to_len)
{
  val ^= SIGN_MASK_8;
  to_len += sizeof(int8_t);
  *to = val;
  return OB_SUCCESS;
}

int ObOrderPerservingEncoder::encode_from_int16(int16_t val, unsigned char *to, int64_t &to_len)
{
  val ^= SIGN_MASK_16;
  val = bswap_16(val);
  to_len += sizeof(int16_t);
  MEMCPY(to, (unsigned char *)&val, sizeof(val));
  return OB_SUCCESS;
}

int ObOrderPerservingEncoder::encode_from_int32(int32_t val, unsigned char *to, int64_t &to_len)
{
  val ^= SIGN_MASK_32;
  val = bswap_32(val);
  to_len += sizeof(int32_t);
  MEMCPY(to, (unsigned char *)&val, sizeof(val));
  return OB_SUCCESS;
}

int ObOrderPerservingEncoder::encode_from_int(int64_t val, unsigned char *to, int64_t &to_len)
{
  val ^= SIGN_MASK_64;
  val = bswap_64(val);
  to_len += sizeof(int64_t);
  MEMCPY(to, (unsigned char *)&val, sizeof(val));
  return OB_SUCCESS;
}

int ObOrderPerservingEncoder::encode_from_uint8(uint8_t val, unsigned char *to, int64_t &to_len)
{
  to_len += sizeof(uint8_t);
  *to = val;
  return OB_SUCCESS;
}

int ObOrderPerservingEncoder::encode_from_uint16(uint16_t val, unsigned char *to, int64_t &to_len)
{
  val = bswap_16(val);
  to_len += sizeof(uint16_t);
  MEMCPY(to, (unsigned char *)&val, sizeof(val));
  return OB_SUCCESS;
}

int ObOrderPerservingEncoder::encode_from_uint32(uint32_t val, unsigned char *to, int64_t &to_len)
{
  val = bswap_32(val);
  to_len += sizeof(uint32_t);
  MEMCPY(to, (unsigned char *)&val, sizeof(val));
  return OB_SUCCESS;
}

int ObOrderPerservingEncoder::encode_from_uint(uint64_t val, unsigned char *to, int64_t &to_len)
{
  val = bswap_64(val);
  to_len += sizeof(uint64_t);
  MEMCPY(to, (unsigned char *)&val, sizeof(val));
  return OB_SUCCESS;
}

int ObOrderPerservingEncoder::encode_from_double(double val, unsigned char *to, int64_t &to_len)
{
  // to avoid +0 and -0
  if (val == 0.0) {
    val = 0.0;
  }

  int64_t val_int;
  to_len += sizeof(val);
  MEMCPY(&val_int, &val, sizeof(val));
  // int: neg pad FF, pos pad 00
  val_int = (val_int ^ (val_int >> 63)) | ((~val_int) & 0x8000000000000000ULL);
  val_int = bswap_64(val_int);
  MEMCPY(to, &val_int, sizeof(val));
  return OB_SUCCESS;
}

int ObOrderPerservingEncoder::encode_from_float(float val, unsigned char *to, int64_t &to_len)
{
  // to avoid +0 and -0
  if (val == 0.0) {
    val = 0.0;
  }

  int32_t val_int;
  to_len += sizeof(val);
  MEMCPY(&val_int, &val, sizeof(val));
  // int: neg pad FF, pos pad 00
  val_int = (val_int ^ (val_int >> 31)) | ((~val_int) & 0x80000000U);
  val_int = bswap_32(val_int);
  MEMCPY(to, &val_int, sizeof(val));
  return OB_SUCCESS;
}

int ObOrderPerservingEncoder::encode_from_number(ObNumber val,
                                                 unsigned char *to,
                                                 int64_t max_buf_len,
                                                 int64_t &to_len)
{
  int ret = OB_SUCCESS;
  ObNumberDesc desc = val.d_;
  if (to_len + sizeof(int8_t) + desc.len_ * sizeof(uint32_t) + 2 * sizeof(int32_t) > max_buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_TRACE("no enough memory to do encoding for obnumber", K(ret));
  } else {
    int8_t se = desc.se_;
    // int: neg pad FF, pos pad 00
    *to = se;
    to_len++;
    to++;

    // digits encoding
    int32_t digits_mask = static_cast<int32_t>((int64_t)((~se) ^ 0x80) >> 8);
    uint32_t *digits_ptr = val.get_digits();
    for (int64_t i = 0; i < desc.len_; i++) {
      uint32_t dig = bswap_32((digits_ptr[i] + 1) ^ digits_mask);
      MEMCPY(to, &dig, sizeof(dig));
      to_len += sizeof(dig);
      to += sizeof(dig);
    }
    MEMCPY(to, &digits_mask, sizeof(digits_mask));
    to_len += sizeof(digits_mask);
    to += sizeof(digits_mask);
  }
  return ret;
}

int ObOrderPerservingEncoder::encode_from_timestamp(ObOTimestampData val,
                                                    unsigned char *to,
                                                    int64_t &to_len)
{
  int64_t time_us = val.time_us_;
  uint16_t nsec = val.time_ctx_.tail_nsec_;

  uint64_t t1 = time_us ^ SIGN_MASK_64;
  t1 = bswap_64(t1);

  MEMCPY(to, (unsigned char *)&t1, sizeof(uint64_t));
  to_len += sizeof(t1);
  to += sizeof(t1);

  nsec = bswap_32(nsec);
  MEMCPY(to, (unsigned char *)&nsec, sizeof(nsec));
  to_len += sizeof(nsec);
  to += sizeof(nsec);

  return OB_SUCCESS;
}

int ObOrderPerservingEncoder::encode_from_interval_ds(ObIntervalDSValue val,
                                                      unsigned char *to,
                                                      int64_t &to_len)
{
  int64_t nsec = val.nsecond_;
  int32_t frac_nsec = val.fractional_second_;

  nsec ^= SIGN_MASK_64;
  nsec = bswap_64(nsec);
  MEMCPY(to, (unsigned char *)&nsec, sizeof(nsec));
  to_len += sizeof(nsec);
  to += sizeof(nsec);

  frac_nsec ^= SIGN_MASK_32;
  frac_nsec = bswap_32(frac_nsec);
  MEMCPY(to, (unsigned char *)&frac_nsec, sizeof(frac_nsec));
  to_len += sizeof(frac_nsec);
  to += sizeof(frac_nsec);

  return OB_SUCCESS;
}

template <typename T>
int ObOrderPerservingEncoder::encode_from_decint(const T &decint, unsigned char *to,
                                                 int64_t &to_len)
{
  static_assert(wide::IsWideInteger<T>::value, "");
  int ret = OB_SUCCESS;
  const int item_count  = T::ITEM_COUNT;
  int64_t high = static_cast<int64_t>(decint.items_[item_count - 1]);
  // encode_from_int(high, to, to_len);
  high ^= SIGN_MASK_64;
  high = bswap_64(high);
  MEMCPY(to, (unsigned char *)&high, sizeof(high));
  to += sizeof(high);
  to_len += sizeof(high);
  for (int i = item_count - 2; i >= 0; i--) {
    uint64_t val = bswap_64(decint.items_[i]);
    MEMCPY(to, (unsigned char *)&val, sizeof(val));
    to_len += sizeof(val);
    to += sizeof(val);
  }
  return ret;
}

int ObOrderPerservingEncoder::encode_from_decint(const ObDecimalInt *decint, int32_t int_bytes, unsigned char *to, int64_t &to_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(decint)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid decimal int", K(ret), K(decint));
  } else {
    switch (int_bytes) {
    case sizeof(int32_t): {
      ret = encode_from_int32(*reinterpret_cast<const int32_t *>(decint), to, to_len);
      break;
    }
    case sizeof(int64_t): {
      ret = encode_from_int(*reinterpret_cast<const int64_t *>(decint), to, to_len);
      break;
    }
    case sizeof(int128_t): {
      ret = encode_from_decint(*reinterpret_cast<const int128_t *>(decint), to, to_len);
      break;
    }
    case sizeof(int256_t): {
      ret = encode_from_decint(*reinterpret_cast<const int256_t *>(decint), to, to_len);
      break;
    }
    case sizeof(int512_t): {
      ret = encode_from_decint(*reinterpret_cast<const int512_t *>(decint), to, to_len);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected int bytes", K(ret), K(int_bytes));
    }
    }
  }
  return ret;
}

int ObOrderPerservingEncoder::encode_tails(unsigned char *to, int64_t max_buf_len,
                                           int64_t &to_len, bool is_mem,
                                           common::ObCollationType cs, bool with_empty_str)
{
  int ret = OB_SUCCESS;
  // do nothing
  if (to_len + 8 > max_buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("no enough memory to do encoding for string", K(ret));
  } else if (cs == CS_TYPE_COLLATION_FREE || cs == CS_TYPE_BINARY) {
    if (with_empty_str) {
      *to = 0x00;
      to++;
      to_len++;
    }
    *to = 0x00;
    *(to+1) = 0x00;
    to_len += 2;
  } else if (cs == CS_TYPE_UTF8MB4_BIN
           || cs == CS_TYPE_GBK_BIN || cs == CS_TYPE_GB18030_BIN
           || cs == CS_TYPE_GB18030_2022_BIN
           || cs == CS_TYPE_UTF8MB4_GENERAL_CI
           || cs == CS_TYPE_GBK_CHINESE_CI
           || cs == CS_TYPE_UTF16_GENERAL_CI
           || cs == CS_TYPE_UTF16_BIN
           || cs == CS_TYPE_GB18030_CHINESE_CI
           || cs == CS_TYPE_UTF16LE_GENERAL_CI
           || cs == CS_TYPE_UTF16LE_BIN
           || (CS_TYPE_GB18030_2022_PINYIN_CI <= cs && cs <= CS_TYPE_GB18030_2022_STROKE_CS)) {
    if (with_empty_str) {
      *to = 0x00;
      to++;
      to_len++;
    }
    if (is_mem) {
      *to = 0x00;
      *(to+1) = 0x00;
    } else {
      *to = 0x20;
      *(to+1) = 0x20;
    }
    to_len += 2;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support collation", K(cs));
  }
  return ret;
}

int ObSortkeyConditioner::process_key_conditioning(
  ObDatum &data, unsigned char *to, int64_t max_buf_len, int64_t &to_len, ObEncParam &param)
{
  int ret = OB_SUCCESS;
  // process null pos
  if (OB_ISNULL(to)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(to));
  } else if (max_buf_len < 1) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_TRACE("no enough memory to do encoding for obnumber", K(ret));
  } else if (param.is_nullable_) {
    if (param.is_null_first_)
      *to = (param.type_ == ObNullType || data.is_null()) ? 0x00 : 0x01;
    else
      *to = (param.type_ == ObNullType || data.is_null()) ? 0x02 : 0x01;
    to_len++;
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (*to != 0x01) {
    // do nothing
  } else if (OB_FAIL(share::ObOrderPerservingEncoder::make_order_perserving_encode_from_object(
               data, to + to_len, max_buf_len, to_len, param))) {
    if (ret != OB_BUF_NOT_ENOUGH) {
      LOG_WARN("failed  to encode sortkey", K(ret));
    }
  } else if (max_buf_len < to_len) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_TRACE("no enough memory to do encoding for obnumber", K(ret));
  } else if (!param.is_asc_) {
    if (param.is_nullable_) {
      process_decrease(to + 1, to_len - 1);
    } else {
      process_decrease(to, to_len);
    }
  }

  return ret;
}


// simd opt
void ObSortkeyConditioner::process_decrease(unsigned char *to, int64_t to_len)
{
  for (int64_t i = 0; i < to_len; i++) {
    *(to + i) ^= 0xFF;
  }
}

}  // namespace share
}  // end namespace oceanbase
