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
  inline static int cmp(const ObDatum &, const ObDatum &) { return 0; }
};

template <ObObjTypeClass TC>
struct ObTCPayloadCmp : public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r)
  {
    return ObDatumPayload<TC>::get(l) == ObDatumPayload<TC>::get(r)
        ? 0
        : (ObDatumPayload<TC>::get(l) < ObDatumPayload<TC>::get(r) ? -1 : 1);
  }
};

// cmp(signed, unsgined)
struct ObSignedUnsignedCmp : public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r)
  {
    return l.get_int() < 0
        ? -1
        : (l.get_int() < r.get_uint()
           ? -1
           : (l.get_int() == r.get_uint() ? 0 : 1));
  }
};

// cmp(unsigned, signed)
struct ObUnsignedSignedCmp : public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r)
  {
    return -ObSignedUnsignedCmp::cmp(r, l);
  }
};

// specialization for all type class.
template <> struct ObDatumTCCmp<ObIntTC, ObIntTC> : public ObTCPayloadCmp<ObIntTC> {};
template <> struct ObDatumTCCmp<ObUIntTC, ObUIntTC> : public ObTCPayloadCmp<ObUIntTC> {};
template <>
struct ObDatumTCCmp<ObFloatTC, ObFloatTC> : public ObDefined<>
{
  template <typename T>
  inline static int real_value_cmp(T l, T r)
  {
    int ret = 0;
    // Note: For NaN, we can't use C language compare logic, which is not compatible
    // with oracle rule.
    // Oracle NaN compare rule: NaN is the king (bigger than any number)
    if (isnan(l) || isnan(r)) {
      if (isnan(l) && isnan(r)) {
        ret = 0;
      } else if (isnan(l)) {
        // l is nan, r is not nan:left always bigger than right
        ret = 1;
      } else {
        // l is not nan, r is nan, left always less than right
        ret = -1;
      }
    } else {
      ret = l == r ? 0 : (l < r ? -1 : 1);
    }
    return ret;
  }

  inline static int cmp(const ObDatum &l, const ObDatum &r)
  {
    return real_value_cmp(l.get_float(), r.get_float());
  }
};

template <>
struct ObDatumTCCmp<ObDoubleTC, ObDoubleTC> : public ObDatumTCCmp<ObFloatTC, ObFloatTC>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r)
  {
    return ObDatumTCCmp<ObFloatTC, ObFloatTC>::real_value_cmp(l.get_double(), r.get_double());
  }
};

template <>
struct ObDatumTCCmp<ObNumberTC, ObNumberTC> : public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r)
  {
    return number::ObNumber::compare(l.get_number_desc(), l.get_number_digits(),
                                     r.get_number_desc(), r.get_number_digits());
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
  inline static int cmp(const ObDatum &l, const ObDatum &)
  {
    return (ObObj::MIN_OBJECT_VALUE == *l.int_ ? -1 : 1);
  }
};

template <ObObjTypeClass TC>
struct ObDatumTCCmp<TC, ObExtendTC> : public ObDefined<>
{
  inline static int cmp(const ObDatum &, const ObDatum &r)
  {
    return (ObObj::MIN_OBJECT_VALUE == *r.int_ ? -1 : 1);
  }
};

template <>
struct ObDatumTCCmp<ObExtendTC, ObExtendTC> : public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r)
  {
    return (ObObj::MIN_OBJECT_VALUE == *l.int_ && ObObj::MIN_OBJECT_VALUE == *r.int_)
        || (ObObj::MAX_OBJECT_VALUE == *l.int_ && ObObj::MAX_OBJECT_VALUE == *r.int_)
        ? 0 : (ObObj::MIN_OBJECT_VALUE == *l.int_ ? -1 : 1);
  }
};

struct ObDummyCmp : public ObDefined<>
{
  inline static int cmp(const ObDatum &, const ObDatum &)
  {
    return 0;
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
  inline static int cmp(const ObDatum &, const ObDatum &) { return 0; }
};

template <>
struct ObDatumTypeCmp<ObDateTimeType, ObDateTimeType> : public ObTCPayloadCmp<ObDateTimeTC> {};
template <>
struct ObDatumTypeCmp<ObTimestampType, ObTimestampType> : public ObTCPayloadCmp<ObDateTimeTC> {};

template <>
struct ObDatumTypeCmp<ObTimestampLTZType, ObTimestampLTZType> : public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r)
  {
    return l.get_otimestamp_tiny().compare(r.get_otimestamp_tiny());
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
  inline static int cmp(const ObDatum &l, const ObDatum &r)
  {
    return l.get_otimestamp_tz().compare(r.get_otimestamp_tz());
  }
};

template <>
struct ObDatumTypeCmp<ObIntervalYMType, ObIntervalYMType> : public ObTCPayloadCmp<ObIntTC> {};

template <>
struct ObDatumTypeCmp<ObIntervalDSType, ObIntervalDSType> : public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r)
  {
    return l.get_interval_ds().compare(r.get_interval_ds());
  }
};

template <>
struct ObDatumTypeCmp<ObURowIDType, ObURowIDType> : public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r)
  {
    const ObURowIDData l_v(l.len_, reinterpret_cast<const uint8_t *>(l.ptr_));
    const ObURowIDData r_v(r.len_, reinterpret_cast<const uint8_t *>(r.ptr_));
    return l_v.compare(r_v);
  }
};

template <>
struct ObDatumTypeCmp<ObJsonType, ObJsonType> : public ObDefined<>
{
  inline static int cmp(const ObDatum &l, const ObDatum &r)
  {
    int ret = OB_SUCCESS;
    int result = 0;
    ObJsonBin j_bin_l(l.ptr_, l.len_);
    ObJsonBin j_bin_r(r.ptr_, r.len_);
    ObIJsonBase *j_base_l = &j_bin_l;
    ObIJsonBase *j_base_r = &j_bin_r;

    if (OB_FAIL(j_bin_l.reset_iter())) {
      COMMON_LOG(WARN, "fail to reset left json bin iter", K(ret), K(l.len_));
    } else if (OB_FAIL(j_bin_r.reset_iter())) {
      COMMON_LOG(WARN, "fail to reset right json bin iter", K(ret), K(r.len_));
    } else if (OB_FAIL(j_base_l->compare(*j_base_r, result))) {
      COMMON_LOG(WARN, "fail to compare json", K(ret), K(*j_base_l), K(*j_base_r));
    }

    return result;
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
    CS_TYPE_GB18030_BIN, 1> SupportedCollections;

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
  inline static int cmp(const ObDatum &l, const ObDatum &r)
  {
    int res = ObCharset::strcmpsp(
        CS_TYPE, l.ptr_, l.len_, r.ptr_, r.len_, WITH_END_SPACE);
    return res > 0 ? 1 : (res < 0 ? -1 : 0);
  }
};

} // end namespace datum_cmp
} // end namespace common
} // end namespace oceanbase
#endif // OCEANBASE_OB_DATUM_CMP_FUNC_DEF_H_
