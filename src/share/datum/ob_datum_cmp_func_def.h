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

#ifndef OCEANBASE_OB_DATUM_CMP_FUNC_DEF_H
#define OCEANBASE_OB_DATUM_CMP_FUNC_DEF_H

#include <type_traits>

#include "common/object/ob_obj_type.h"
#include "lib/charset/ob_charset.h"
#include "lib/number/ob_number_v2.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/rowid/ob_urowid.h"
#include "ob_datum.h"
#include "ob_datum_util.h"
#include "lib/json_type/ob_json_base.h" // for ObIJsonBase
#include "lib/json_type/ob_json_bin.h" // for ObJsonBin
#include "lib/wide_integer/ob_wide_integer.h"
#include "share/ob_lob_access_utils.h" // for Text types
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace common
{
namespace datum_cmp
{

// We define three core comparison:
//
// 1. ObDatumTypeCmp<ObObjType, ObObjType>: define non string obj type compare,
//    if comparison not deinfed for specified type, use ObDatumTCCmp defined comparison.
// 2. ObDatumTCCmp<ObObjTypeClass, ObObjTypeClass>: define non string obj type class compare
// 3. ObDatumStrCmp<ObCollationType, bool>: define string compare

template <bool V = true>
struct ObDefined
{
  constexpr static bool defined_ = V;
};

// define compare by ObObjTypeClass
template <ObObjTypeClass L_TC, ObObjTypeClass R_TC>
struct ObDatumTCCmp : public ObDefined<false>
{
  inline static int cmp(const ObDatum &, const ObDatum &, int &cmp_ret)
  {
    cmp_ret = 0;
    return OB_SUCCESS;
  }
};

template <ObObjTypeClass TC>
struct ObTCPayloadCmp : public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret)
  {
    cmp_ret = ObDatumPayload<TC>::get(l) == ObDatumPayload<TC>::get(r)
              ? 0
              : (ObDatumPayload<TC>::get(l) < ObDatumPayload<TC>::get(r) ? -1 : 1);
    return OB_SUCCESS;
  }
};

// cmp(signed, unsgined)
struct ObSignedUnsignedCmp : public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret)
  {
    cmp_ret = l.get_int() < 0
              ? -1
              : (l.get_int() < r.get_uint()
                ? -1
                : (l.get_int() == r.get_uint() ? 0 : 1));
    return OB_SUCCESS;
  }
};

// cmp(unsigned, signed)
struct ObUnsignedSignedCmp : public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret)
  {
    int ret = ObSignedUnsignedCmp::cmp(r, l, cmp_ret);
    cmp_ret = -cmp_ret;
    return ret;
  }
};

// specialization for all type class.
template <> struct ObDatumTCCmp<ObIntTC, ObIntTC> : public ObTCPayloadCmp<ObIntTC> {};
template <> struct ObDatumTCCmp<ObUIntTC, ObUIntTC> : public ObTCPayloadCmp<ObUIntTC> {};
template <>
struct ObDatumTCCmp<ObFloatTC, ObFloatTC> : public ObDefined<>
{
  template <typename T>
  inline static int real_value_cmp(T l, T r, int &cmp_ret)
  {
    cmp_ret = 0;
    // Note: For NaN, we can't use C language compare logic, which is not compatible
    // with oracle rule.
    // Oracle NaN compare rule: NaN is the king (bigger than any number)
    if (isnan(l) || isnan(r)) {
      if (isnan(l) && isnan(r)) {
        cmp_ret = 0;
      } else if (isnan(l)) {
        // l is nan, r is not nan:left always bigger than right
        cmp_ret = 1;
      } else {
        // l is not nan, r is nan, left always less than right
        cmp_ret = -1;
      }
    } else {
      cmp_ret = l == r ? 0 : (l < r ? -1 : 1);
    }
    return OB_SUCCESS;
  }

  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret)
  {
    return real_value_cmp(l.get_float(), r.get_float(), cmp_ret);
  }
};

template <ObScale SCALE>
struct ObFixedDoubleCmp: public ObDefined<>
{
  constexpr static double LOG_10[] =
  {
    1e000, 1e001, 1e002, 1e003, 1e004, 1e005, 1e006, 1e007,
    1e008, 1e009, 1e010, 1e011, 1e012, 1e013, 1e014, 1e015,
    1e016, 1e017, 1e018, 1e019, 1e020, 1e021, 1e022, 1e023,
    1e024, 1e025, 1e026, 1e027, 1e028, 1e029, 1e030, 1e031
  };
  constexpr static double P = 5 / LOG_10[SCALE + 1];
  inline static int cmp(const ObDatum &l_datum, const ObDatum &r_datum, int &cmp_ret)
  {
    cmp_ret = 0;
    const double l = l_datum.get_double();
    const double r = r_datum.get_double();
    if (isnan(l) || isnan(r)) {
      if (isnan(l) && isnan(r)) {
        cmp_ret = 0;
      } else if (isnan(l)) {
        // l is nan, r is not nan:left always bigger than right
        cmp_ret = 1;
      } else {
        // l is not nan, r is nan, left always less than right
        cmp_ret = -1;
      }
    } else if (l == r || fabs(l - r) < P) {
      cmp_ret = 0;
    } else {
      cmp_ret = (l < r ? -1 : 1);
    }
    return OB_SUCCESS;
  }
};


template <>
struct ObDatumTCCmp<ObDoubleTC, ObDoubleTC> : public ObDatumTCCmp<ObFloatTC, ObFloatTC>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret)
  {
    return ObDatumTCCmp<ObFloatTC, ObFloatTC>::real_value_cmp(l.get_double(), r.get_double(), cmp_ret);
  }
};

template <>
struct ObDatumTCCmp<ObNumberTC, ObNumberTC> : public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret)
  {
    cmp_ret = number::ObNumber::compare(l.get_number_desc(), l.get_number_digits(),
                                        r.get_number_desc(), r.get_number_digits());
    return OB_SUCCESS;
  }
};

template<>
struct ObDatumTCCmp<ObDecimalIntTC, ObDecimalIntTC>: public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(wide::compare(l, r, cmp_ret))) {
      COMMON_LOG(WARN, "compare error", K(ret));
    }
    return ret;
  }
};

template<ObDecimalIntWideType lw, ObDecimalIntWideType rw>
struct ObDecintCmp: public ObDefined<>
{
  static_assert(lw < DECIMAL_INT_MAX && rw < DECIMAL_INT_MAX, "");
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret)
  {
    decint_cmp_fp cmp =
      wide::ObDecimalIntCmpSet::decint_decint_cmp_set_[(2 << (lw + 1))][(2 << (rw + 1))];
    cmp_ret = cmp(l.get_decimal_int(), r.get_decimal_int());
    return OB_SUCCESS;
  }
};

template <> struct ObDatumTCCmp<ObDateTC, ObDateTC> : public ObTCPayloadCmp<ObDateTC> {};
template <> struct ObDatumTCCmp<ObTimeTC, ObTimeTC> : public ObTCPayloadCmp<ObTimeTC> {};
template <> struct ObDatumTCCmp<ObYearTC, ObYearTC> : public ObTCPayloadCmp<ObYearTC> {};
template <> struct ObDatumTCCmp<ObBitTC, ObBitTC> : public ObTCPayloadCmp<ObBitTC> {};
template <> struct ObDatumTCCmp<ObEnumSetTC, ObEnumSetTC> : public ObTCPayloadCmp<ObEnumSetTC> {};

// different type class compare
template <> struct ObDatumTCCmp<ObIntTC, ObUIntTC> : public ObSignedUnsignedCmp {};
template <> struct ObDatumTCCmp<ObIntTC, ObEnumSetTC> : public ObSignedUnsignedCmp {};
template <> struct ObDatumTCCmp<ObUIntTC, ObIntTC> : public ObUnsignedSignedCmp {};
template <> struct ObDatumTCCmp<ObEnumSetTC, ObIntTC> : public ObUnsignedSignedCmp {};
template <> struct ObDatumTCCmp<ObUIntTC, ObEnumSetTC> : public ObTCPayloadCmp<ObUIntTC> {};
template <> struct ObDatumTCCmp<ObEnumSetTC, ObUIntTC> : public ObTCPayloadCmp<ObUIntTC> {};


// special process for extend and null class type.

// extend type vs any type is depend on extend type is min or max
template <ObObjTypeClass TC>
struct ObDatumTCCmp<ObExtendTC, TC> : public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &, int &cmp_ret)
  {
    cmp_ret = (ObObj::MIN_OBJECT_VALUE == *l.int_ ? -1 : 1);
    return OB_SUCCESS;
  }
};

template <ObObjTypeClass TC>
struct ObDatumTCCmp<TC, ObExtendTC> : public ObDefined<>
{
  inline static int cmp(const ObDatum &, const ObDatum &r, int &cmp_ret)
  {
    cmp_ret = (ObObj::MIN_OBJECT_VALUE == *r.int_ ? -1 : 1);
    return OB_SUCCESS;
  }
};

template <>
struct ObDatumTCCmp<ObExtendTC, ObExtendTC> : public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret)
  {
    cmp_ret = (ObObj::MIN_OBJECT_VALUE == *l.int_ && ObObj::MIN_OBJECT_VALUE == *r.int_)
              || (ObObj::MAX_OBJECT_VALUE == *l.int_ && ObObj::MAX_OBJECT_VALUE == *r.int_)
              ? 0 : (ObObj::MIN_OBJECT_VALUE == *l.int_ ? -1 : 1);
    return OB_SUCCESS;
  }
};

struct ObDummyCmp : public ObDefined<>
{
  inline static int cmp(const ObDatum &, const ObDatum &, int &cmp_ret)
  {
    cmp_ret = 0;
    return OB_SUCCESS;
  }
};

// null type compare is never used (out layer guaranteed), but should be defined.
template <ObObjTypeClass TC> struct ObDatumTCCmp<TC, ObNullTC> : public ObDummyCmp {};
template <ObObjTypeClass TC> struct ObDatumTCCmp<ObNullTC, TC> : public ObDummyCmp {};
template <> struct ObDatumTCCmp<ObNullTC, ObExtendTC> : public ObDummyCmp {};
template <> struct ObDatumTCCmp<ObExtendTC, ObNullTC> : public ObDummyCmp {};
template <> struct ObDatumTCCmp<ObNullTC, ObNullTC> : public ObDummyCmp {};


///////////////////////////////////////////////////////////////////////////////
// begin define compare by ObObjType
///////////////////////////////////////////////////////////////////////////////

// define compare by ObObjType
template <ObObjType L_T, ObObjType R_T>
struct ObDatumTypeCmp : public ObDefined<false>
{
  inline static int cmp(const ObDatum &, const ObDatum &, int &cmp_ret)
  {
    cmp_ret = 0;
    return OB_SUCCESS;
  }
};

template <>
struct ObDatumTypeCmp<ObDateTimeType, ObDateTimeType> : public ObTCPayloadCmp<ObDateTimeTC> {};
template <>
struct ObDatumTypeCmp<ObTimestampType, ObTimestampType> : public ObTCPayloadCmp<ObDateTimeTC> {};

template <>
struct ObDatumTypeCmp<ObTimestampLTZType, ObTimestampLTZType> : public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret)
  {
    cmp_ret = l.get_otimestamp_tiny().compare(r.get_otimestamp_tiny());
    return OB_SUCCESS;
  }
};

template <> struct ObDatumTypeCmp<ObTimestampLTZType, ObTimestampNanoType>
  : public ObDatumTypeCmp<ObTimestampLTZType, ObTimestampLTZType> {};

template <> struct ObDatumTypeCmp<ObTimestampNanoType, ObTimestampLTZType>
  : public ObDatumTypeCmp<ObTimestampLTZType, ObTimestampLTZType> {};

template <> struct ObDatumTypeCmp<ObTimestampNanoType, ObTimestampNanoType>
  : public ObDatumTypeCmp<ObTimestampLTZType, ObTimestampLTZType> {};

template <>
struct ObDatumTypeCmp<ObTimestampTZType, ObTimestampTZType> : public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret)
  {
    cmp_ret = l.get_otimestamp_tz().compare(r.get_otimestamp_tz());
    return OB_SUCCESS;
  }
};

template <>
struct ObDatumTypeCmp<ObIntervalYMType, ObIntervalYMType> : public ObTCPayloadCmp<ObIntTC> {};

template <>
struct ObDatumTypeCmp<ObIntervalDSType, ObIntervalDSType> : public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret)
  {
    cmp_ret = l.get_interval_ds().compare(r.get_interval_ds());
    return OB_SUCCESS;
  }
};

template <>
struct ObDatumTypeCmp<ObURowIDType, ObURowIDType> : public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret)
  {
    const ObURowIDData l_v(l.len_, reinterpret_cast<const uint8_t *>(l.ptr_));
    const ObURowIDData r_v(r.len_, reinterpret_cast<const uint8_t *>(r.ptr_));
    cmp_ret = l_v.compare(r_v);
    return OB_SUCCESS;
  }
};

template <bool HAS_LOB_LOCATOR>
struct ObDatumJsonCmp : public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret)
  {
    int ret = OB_SUCCESS;
    cmp_ret = 0;
    ObString l_data;
    ObString r_data;
    common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObTextStringIter l_instr_iter(ObJsonType, CS_TYPE_BINARY, l.get_string(), HAS_LOB_LOCATOR);
    ObTextStringIter r_instr_iter(ObJsonType, CS_TYPE_BINARY, r.get_string(), HAS_LOB_LOCATOR);
    if (OB_FAIL(l_instr_iter.init(0, NULL, &allocator))) {
      COMMON_LOG(WARN, "Lob: init left lob str iter failed", K(ret), K(l));
    } else if (OB_FAIL(l_instr_iter.get_full_data(l_data))) {
      COMMON_LOG(WARN, "Lob: get left lob str iter full data failed ", K(ret), K(l_instr_iter));
    } else if (OB_FAIL(r_instr_iter.init(0, NULL, &allocator))) {
      COMMON_LOG(WARN, "Lob: init right lob str iter failed", K(ret), K(ret), K(r));
    } else if (OB_FAIL(r_instr_iter.get_full_data(r_data))) {
      COMMON_LOG(WARN, "Lob: get right lob str iter full data failed ", K(ret), K(r_instr_iter));
    } else {
      ObJsonBin j_bin_l(l_data.ptr(), l_data.length(), &allocator);
      ObJsonBin j_bin_r(r_data.ptr(), r_data.length(), &allocator);
      ObIJsonBase *j_base_l = &j_bin_l;
      ObIJsonBase *j_base_r = &j_bin_r;

      if (OB_FAIL(j_bin_l.reset_iter())) {
        COMMON_LOG(WARN, "fail to reset left json bin iter", K(ret), K(l.len_));
      } else if (OB_FAIL(j_bin_r.reset_iter())) {
        COMMON_LOG(WARN, "fail to reset right json bin iter", K(ret), K(r.len_));
      } else if (OB_FAIL(j_base_l->compare(*j_base_r, cmp_ret))) {
        COMMON_LOG(WARN, "fail to compare json", K(ret), K(*j_base_l), K(*j_base_r));
      }
    }

    return ret;
  }
};

template <bool HAS_LOB_HEADER>
struct ObDatumGeoCmp : public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret)
  {
    int ret = OB_SUCCESS;
    cmp_ret = 0;
    ObString l_data;
    ObString r_data;
    common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObTextStringIter l_instr_iter(ObGeometryType, CS_TYPE_BINARY, l.get_string(), HAS_LOB_HEADER);
    ObTextStringIter r_instr_iter(ObGeometryType, CS_TYPE_BINARY, r.get_string(), HAS_LOB_HEADER);
    if (OB_FAIL(l_instr_iter.init(0, NULL, &allocator))) {
      COMMON_LOG(WARN, "Lob: init left lob str iter failed", K(ret), K(l));
    } else if (OB_FAIL(l_instr_iter.get_full_data(l_data))) {
      COMMON_LOG(WARN, "Lob: get left lob str iter full data failed ", K(ret), K(l_instr_iter));
    } else if (OB_FAIL(r_instr_iter.init(0, NULL, &allocator))) {
      COMMON_LOG(WARN, "Lob: init right lob str iter failed", K(ret), K(ret), K(r));
    } else if (OB_FAIL(r_instr_iter.get_full_data(r_data))) {
      COMMON_LOG(WARN, "Lob: get right lob str iter full data failed ", K(ret), K(r_instr_iter));
    } else {
      cmp_ret = ObCharset::strcmpsp(CS_TYPE_BINARY, l_data.ptr(), l_data.length(), r_data.ptr(), r_data.length(), false);
    }
    cmp_ret = cmp_ret > 0 ? 1 : (cmp_ret < 0 ? -1 : 0);
    return ret;
  }
};

template <bool HAS_LOB_HEADER>
struct ObDatumUDTCmp : public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret)
  {
    UNUSED(l);
    UNUSED(r);
    UNUSED(cmp_ret);
    return OB_ERR_NO_ORDER_MAP_SQL;
  }
};

///////////////////////////////////////////////////////////////////////////////
// begin define string compare functions
///////////////////////////////////////////////////////////////////////////////
typedef ObConstIntMapping<0,
    CS_TYPE_GBK_CHINESE_CI, 1,
    CS_TYPE_UTF8MB4_GENERAL_CI, 1,
    CS_TYPE_UTF8MB4_BIN, 1,
    CS_TYPE_UTF16_GENERAL_CI, 1,
    CS_TYPE_UTF16_BIN, 1,
    CS_TYPE_BINARY, 1,
    CS_TYPE_GBK_BIN, 1,
    CS_TYPE_UTF16_UNICODE_CI, 1,
    CS_TYPE_UTF8MB4_UNICODE_CI, 1,
    CS_TYPE_GB18030_CHINESE_CI, 1,
    CS_TYPE_GB18030_BIN, 1,
    CS_TYPE_LATIN1_SWEDISH_CI,1,
    CS_TYPE_LATIN1_BIN,1,
    CS_TYPE_GB18030_2022_BIN, 1,
    CS_TYPE_GB18030_2022_PINYIN_CI, 1,
    CS_TYPE_GB18030_2022_PINYIN_CS, 1,
    CS_TYPE_GB18030_2022_RADICAL_CI, 1,
    CS_TYPE_GB18030_2022_RADICAL_CS, 1,
    CS_TYPE_GB18030_2022_STROKE_CI, 1,
    CS_TYPE_GB18030_2022_STROKE_CS, 1 > SupportedCollections;

// bool is_calc_with_end_space(ObObjType type1, ObObjType type2,
//                            bool is_oracle_mode,
//                            ObCollationType cs_type1,
//                            ObCollationType cs_type2)
// {
//  return is_oracle_mode && ( (ObVarcharType == type1 && CS_TYPE_BINARY != cs_type1)
//                             || (ObVarcharType == type2 && CS_TYPE_BINARY != cs_type2)
//                             || (ObNVarchar2Type == type1)
//                             || (ObNVarchar2Type == type2) );
// }
//
template <ObCollationType CS_TYPE, bool WITH_END_SPACE>
struct ObDatumStrCmp : public ObDefined<SupportedCollections::liner_search(CS_TYPE)>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret)
  { // ToDo: @gehao need to handle ObDatum has_lob_header flags ?
    cmp_ret = ObCharset::strcmpsp(
        CS_TYPE, l.ptr_, l.len_, r.ptr_, r.len_, WITH_END_SPACE);
    cmp_ret = cmp_ret > 0 ? 1 : (cmp_ret < 0 ? -1 : 0);
    return OB_SUCCESS;
  }
};

template <ObCollationType CS_TYPE, bool WITH_END_SPACE>
struct ObDatumTextCmp : public ObDefined<SupportedCollections::liner_search(CS_TYPE)>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret)
  {
    int ret = OB_SUCCESS;
    cmp_ret = 0;
    ObString l_data;
    ObString r_data;
    const ObLobCommon& rlob = r.get_lob_data();
    const ObLobCommon& llob = l.get_lob_data();
    if (r.len_ != 0 && !rlob.is_mem_loc_ && rlob.in_row_ &&
        l.len_ != 0 && !llob.is_mem_loc_ && llob.in_row_) {
      cmp_ret = ObCharset::strcmpsp(CS_TYPE,
                llob.get_inrow_data_ptr(), static_cast<int32_t>(llob.get_byte_size(l.len_)),
                rlob.get_inrow_data_ptr(), static_cast<int32_t>(rlob.get_byte_size(r.len_)), WITH_END_SPACE);
    } else {
      common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      ObTextStringIter l_instr_iter(ObLongTextType, CS_TYPE, l.get_string(), true); // longtext only indicates its a lob type
      ObTextStringIter r_instr_iter(ObLongTextType, CS_TYPE, r.get_string(), true);
      if (OB_FAIL(l_instr_iter.init(0, NULL, &allocator))) {
        COMMON_LOG(WARN, "Lob: init left lob str iter failed", K(ret), K(CS_TYPE), K(l));
      } else if (OB_FAIL(l_instr_iter.get_full_data(l_data))) {
        COMMON_LOG(WARN, "Lob: get left lob str iter full data failed ", K(ret), K(CS_TYPE), K(l_instr_iter));
      } else if (OB_FAIL(r_instr_iter.init(0, NULL, &allocator))) {
        COMMON_LOG(WARN, "Lob: init right lob str iter failed", K(ret), K(ret), K(r));
      } else if (OB_FAIL(r_instr_iter.get_full_data(r_data))) {
        COMMON_LOG(WARN, "Lob: get right lob str iter full data failed ", K(ret), K(CS_TYPE), K(r_instr_iter));
      } else {
        cmp_ret = ObCharset::strcmpsp(
            CS_TYPE, l_data.ptr(), l_data.length(), r_data.ptr(), r_data.length(), WITH_END_SPACE);
      }
    }
    // if error occur when reading outrow lobs, the compare result is wrong.
    cmp_ret = cmp_ret > 0 ? 1 : (cmp_ret < 0 ? -1 : 0);
    return ret;
  }
};

template <ObCollationType CS_TYPE, bool WITH_END_SPACE>
struct ObDatumTextStringCmp : public ObDefined<SupportedCollections::liner_search(CS_TYPE)>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret)
  {
    int ret = OB_SUCCESS;
    cmp_ret = 0;
    const ObLobCommon& llob = l.get_lob_data();
    if (l.len_ != 0 && !llob.is_mem_loc_ && llob.in_row_) {
      cmp_ret = ObCharset::strcmpsp(CS_TYPE,
                llob.get_inrow_data_ptr(), static_cast<int32_t>(llob.get_byte_size(l.len_)),
                r.ptr_, r.len_, WITH_END_SPACE);
    } else {
      ObString l_data;
      common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      ObTextStringIter l_instr_iter(ObLongTextType, CS_TYPE, l.get_string(), true); // longtext only indicates its a lob type
      if (OB_FAIL(l_instr_iter.init(0, NULL, &allocator))) {
        COMMON_LOG(WARN, "Lob: init left lob str iter failed", K(ret), K(CS_TYPE), K(l));
      } else if (OB_FAIL(l_instr_iter.get_full_data(l_data))) {
      COMMON_LOG(WARN, "Lob: get left lob str iter full data failed ", K(ret), K(CS_TYPE), K(l_instr_iter));
      } else {
        cmp_ret = ObCharset::strcmpsp(
            CS_TYPE, l_data.ptr(), l_data.length(), r.ptr_, r.len_, WITH_END_SPACE);
      }
    }
    // if error occur when reading outrow lobs, the compare result is wrong.
    cmp_ret = cmp_ret > 0 ? 1 : (cmp_ret < 0 ? -1 : 0);
    return ret;
  }
};

template <ObCollationType CS_TYPE, bool WITH_END_SPACE>
struct ObDatumStringTextCmp : public ObDefined<SupportedCollections::liner_search(CS_TYPE)>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret)
  {
    int ret = OB_SUCCESS;
    cmp_ret = 0;
    const ObLobCommon& rlob = r.get_lob_data();
    if (r.len_ != 0 && !rlob.is_mem_loc_ && rlob.in_row_ && rlob.reserve_ == 0) {
      cmp_ret = ObCharset::strcmpsp(CS_TYPE,
                l.ptr_, l.len_,
                rlob.get_inrow_data_ptr(), static_cast<int32_t>(rlob.get_byte_size(r.len_)), WITH_END_SPACE);
    } else {
      ObString r_data;
      common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      ObTextStringIter r_instr_iter(ObLongTextType, CS_TYPE, r.get_string(), true);  // longtext only indicates its a lob type
      if (OB_FAIL(r_instr_iter.init(0, NULL, &allocator))) {
        COMMON_LOG(WARN, "Lob: init right lob str iter failed", K(ret), K(ret), K(r));
      } else if (OB_FAIL(r_instr_iter.get_full_data(r_data))) {
        COMMON_LOG(WARN, "Lob: get right lob str iter full data failed ", K(ret), K(CS_TYPE), K(r_instr_iter));
      } else {
        cmp_ret = ObCharset::strcmpsp(
            CS_TYPE, l.ptr_, l.len_, r_data.ptr(), r_data.length(), WITH_END_SPACE);
      }
    }
    // if error occur when reading outrow lobs, the compare result is wrong.
    cmp_ret = cmp_ret > 0 ? 1 : (cmp_ret < 0 ? -1 : 0);
    return ret;
  }
};

} // end namespace datum_cmp
} // end namespace common
} // end namespace oceanbase
#endif // OCEANBASE_OB_DATUM_CMP_FUNC_DEF_H_
