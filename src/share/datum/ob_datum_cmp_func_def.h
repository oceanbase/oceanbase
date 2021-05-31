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

namespace oceanbase {
namespace common {

namespace cmp_func_helper {
// ************ CmpFunc Impl Helper ************

template <ObObjTypeClass tc1, ObObjTypeClass tc2>
struct ObDatumCmpHelperByTC {
  constexpr static bool defined_ = false;
  inline static int cmp(const ObDatum& datum1, const ObDatum& datum2)
  {
    UNUSED(datum1);
    UNUSED(datum2);
    return 0;
  }
};

// Default CmpFunc Impl
template <>
struct ObDatumCmpHelperByTC<ObMaxTC, ObMaxTC> {
  constexpr static bool defined_ = false;
  inline static int cmp(const ObDatum& datum1, const ObDatum& datum2)
  {
    UNUSED(datum1);
    UNUSED(datum2);
    return 0;
  }
};

// tc1 == tc2
// only has same TC, define cmp<IntTC, UIntTC>, <EnumTC, IntTC/UIntTC>) except
template <ObObjTypeClass tc>
struct ObDatumCmpHelperByTC<tc, tc> {
  constexpr static bool defined_ = true;
  typedef const char* (*invalid_func_type)(const ObDatum&);
  inline static int cmp(const ObDatum& datum1, const ObDatum& datum2)
  {
    static_assert(!std::is_same<decltype(&ObDatumPayload<tc>::get), invalid_func_type>::value,
        "Not Supported TypeClass For SameTC Cmp");
    return ObDatumPayload<tc>::get(datum1) == ObDatumPayload<tc>::get(datum2)
               ? 0
               : (ObDatumPayload<tc>::get(datum1) < ObDatumPayload<tc>::get(datum2) ? -1 : 1);
  }
};

// <Signed, Unsigned>
template <ObObjTypeClass tc1, ObObjTypeClass tc2>
struct ObDatumCmpHelperBySignSU {
  constexpr static bool defined_ = true;
  inline static int cmp(const ObDatum& datum1, const ObDatum& datum2)
  {
    return ObDatumPayload<tc1>::get(datum1) < 0
               ? -1
               : (ObDatumPayload<tc1>::get(datum1) < ObDatumPayload<tc2>::get(datum2)
                         ? -1
                         : (ObDatumPayload<tc1>::get(datum1) == ObDatumPayload<tc2>::get(datum2) ? 0 : 1));
  }
};

// <Unsigned, Signed>
template <ObObjTypeClass tc1, ObObjTypeClass tc2>
struct ObDatumCmpHelperBySignUS {
  constexpr static bool defined_ = true;
  inline static int cmp(const ObDatum& datum1, const ObDatum& datum2)
  {
    return ObDatumPayload<tc2>::get(datum2) < 0
               ? 1
               : (ObDatumPayload<tc1>::get(datum1) < ObDatumPayload<tc2>::get(datum2)
                         ? -1
                         : (ObDatumPayload<tc1>::get(datum1) == ObDatumPayload<tc2>::get(datum2) ? 0 : 1));
  }
};

// <Unsigned, UnSigned>
template <ObObjTypeClass tc1, ObObjTypeClass tc2>
struct ObDatumCmpHelperBySignUU {
  constexpr static bool defined_ = true;
  inline static int cmp(const ObDatum& datum1, const ObDatum& datum2)
  {
    return ObDatumPayload<tc1>::get(datum1) < ObDatumPayload<tc2>::get(datum2)
               ? -1
               : (ObDatumPayload<tc1>::get(datum1) == ObDatumPayload<tc2>::get(datum2) ? 0 : 1);
  }
};

// <ObObjType, ObObjType>::cmp
template <ObObjType obj_type1, ObObjType obj_type2>
struct ObDatumCmpHelperByType
    : public ObDatumCmpHelperByTC<ObObjTypeTraits<obj_type1>::tc_, ObObjTypeTraits<obj_type2>::tc_> {};

// ==========================================
// Specialized template class implementation
// is required for certain TypeClass or specific types
// ==========================================
#ifndef DATUM_CMP_FUNC_DEF
#define DATUM_CMP_FUNC_DEF

#define DECL_SAME_TC_CMP(tc)                               \
  template <>                                              \
  struct ObDatumCmpHelperByTC<tc, tc> {                    \
    constexpr static bool defined_ = true;                 \
    inline static int cmp(const ObDatum&, const ObDatum&); \
  };

#define IMPL_SAME_TC_CMP(tc) inline int ObDatumCmpHelperByTC<tc, tc>::cmp(const ObDatum& datum1, const ObDatum& datum2)

#define DEF_SAME_TC_CMP(tc) \
  DECL_SAME_TC_CMP(tc)      \
  IMPL_SAME_TC_CMP(tc)

#define DECL_TC1_TC2_CMP(tc1, tc2)                         \
  template <>                                              \
  struct ObDatumCmpHelperByTC<tc1, tc2> {                  \
    constexpr static bool defined_ = true;                 \
    inline static int cmp(const ObDatum&, const ObDatum&); \
  };

#define IMPL_TC1_TC2_CMP(tc1, tc2) \
  inline int ObDatumCmpHelperByTC<tc1, tc2>::cmp(const ObDatum& datum1, const ObDatum& datum2)

#define DEF_TC1_TC2_CMP(tc1, tc2) \
  DECL_TC1_TC2_CMP(tc1, tc2)      \
  IMPL_TC1_TC2_CMP(tc1, tc2)

#define DECL_TC1_ANYTC_CMP(tc1)                            \
  template <ObObjTypeClass tc2>                            \
  struct ObDatumCmpHelperByTC<tc1, tc2> {                  \
    constexpr static bool defined_ = true;                 \
    inline static int cmp(const ObDatum&, const ObDatum&); \
  };

#define IMPL_TC1_ANYTC_CMP(tc1) \
  template <ObObjTypeClass tc2> \
  inline int ObDatumCmpHelperByTC<tc1, tc2>::cmp(const ObDatum& datum1, const ObDatum& datum2)
#define DEF_TC1_ANYTC_CMP(tc1) \
  DECL_TC1_ANYTC_CMP(tc1)      \
  IMPL_TC1_ANYTC_CMP(tc1)

#define DECL_TC2_ANYTC_CMP(tc2)                            \
  template <ObObjTypeClass tc1>                            \
  struct ObDatumCmpHelperByTC<tc1, tc2> {                  \
    constexpr static bool defined_ = true;                 \
    inline static int cmp(const ObDatum&, const ObDatum&); \
  };

#define IMPL_TC2_ANYTC_CMP(tc2) \
  template <ObObjTypeClass tc1> \
  inline int ObDatumCmpHelperByTC<tc1, tc2>::cmp(const ObDatum& datum1, const ObDatum& datum2)
#define DEF_TC2_ANYTC_CMP(tc2) \
  DECL_TC2_ANYTC_CMP(tc2)      \
  IMPL_TC2_ANYTC_CMP(tc2)

#define DECL_TYPE_TYPE_CMP(type1, type2)                   \
  template <>                                              \
  struct ObDatumCmpHelperByType<type1, type2> {            \
    constexpr static bool defined_ = true;                 \
    inline static int cmp(const ObDatum&, const ObDatum&); \
  };

#define IMPL_TYPE_TYPE_CMP(type1, type2) \
  inline int ObDatumCmpHelperByType<type1, type2>::cmp(const ObDatum& datum1, const ObDatum& datum2)

#define DEF_TYPE_TYPE_CMP(type1, type2) \
  DECL_TYPE_TYPE_CMP(type1, type2)      \
  IMPL_TYPE_TYPE_CMP(type1, type2)

#define DECL_STR_CMP_FOR_CS(cs_type)                               \
  template <bool calc_with_end_space>                              \
  struct ObDatumStrCmpHelperCSImpl<cs_type, calc_with_end_space> { \
    constexpr static bool defined_ = true;                         \
    inline static int cmp(const ObDatum&, const ObDatum&);         \
  };

#define IMPL_STR_CMP_FOR_CS(cs_type)                                       \
  template <bool calc_with_end_space>                                      \
  inline int ObDatumStrCmpHelperCSImpl<cs_type, calc_with_end_space>::cmp( \
      const ObDatum& datum1, const ObDatum& datum2)

#define DEF_STR_CMP_FOR_CS(cs_type) \
  DECL_STR_CMP_FOR_CS(cs_type)      \
  IMPL_STR_CMP_FOR_CS(cs_type)

// Implementation of ObNumberTC comparison
// ObNumberTC needs an int64_t + int32_t in data storage
// The default <TC, TC>::cmp cannot achieve the comparison of data storage
// more than one int64_t, so special implementation is required
DEF_SAME_TC_CMP(ObNumberTC)
{
  const number::ObCompactNumber& l_num = ObDatumPayload<ObNumberTC>::get(datum1);
  const number::ObCompactNumber& r_num = ObDatumPayload<ObNumberTC>::get(datum2);
  return number::ObNumber::compare(l_num.desc_, l_num.digits_, r_num.desc_, r_num.digits_);
}

// Extend TYPE and any type of comparison depend on the value of Extend
DEF_TC1_ANYTC_CMP(ObExtendTC)
{
  UNUSED(datum2);
  return (ObObj::MIN_OBJECT_VALUE == *datum1.int_ ? -1 : 1);
}

DEF_TC2_ANYTC_CMP(ObExtendTC)
{
  UNUSED(datum1);
  return (ObObj::MIN_OBJECT_VALUE == *datum2.int_ ? 1 : -1);
}

DEF_SAME_TC_CMP(ObExtendTC)
{
  return (ObObj::MIN_OBJECT_VALUE == *datum1.int_ && ObObj::MIN_OBJECT_VALUE == *datum2.int_) ||
                 (ObObj::MAX_OBJECT_VALUE == *datum1.int_ && ObObj::MAX_OBJECT_VALUE == *datum2.int_)
             ? 0
             : (ObObj::MIN_OBJECT_VALUE == *datum1.int_ ? -1 : 1);
}

DEF_TC1_TC2_CMP(ObExtendTC, ObNullTC)
{
  UNUSED(datum1);
  UNUSED(datum2);
  return 0;
}

DEF_TC1_TC2_CMP(ObNullTC, ObExtendTC)
{
  UNUSED(datum1);
  UNUSED(datum2);
  return 0;
}

// not be called
DEF_TC1_ANYTC_CMP(ObNullTC)
{
  UNUSED(datum1);
  UNUSED(datum2);
  return 0;
}

DEF_TC2_ANYTC_CMP(ObNullTC)
{
  UNUSED(datum1);
  UNUSED(datum2);
  return 0;
}

DEF_SAME_TC_CMP(ObNullTC)
{
  UNUSED(datum1);
  UNUSED(datum2);
  return 0;
}

DEF_TYPE_TYPE_CMP(ObTimestampLTZType, ObTimestampLTZType)
{
  const ObOTimestampData l = datum1.get_otimestamp_tiny();
  const ObOTimestampData r = datum2.get_otimestamp_tiny();
  return l.compare(r);
}

DEF_TYPE_TYPE_CMP(ObTimestampLTZType, ObTimestampNanoType)
{
  const ObOTimestampData l = datum1.get_otimestamp_tiny();
  const ObOTimestampData r = datum2.get_otimestamp_tiny();
  return l.compare(r);
}

DEF_TYPE_TYPE_CMP(ObTimestampNanoType, ObTimestampLTZType)
{
  const ObOTimestampData l = datum1.get_otimestamp_tiny();
  const ObOTimestampData r = datum2.get_otimestamp_tiny();
  return l.compare(r);
}

DEF_TYPE_TYPE_CMP(ObTimestampNanoType, ObTimestampNanoType)
{
  const ObOTimestampData l = datum1.get_otimestamp_tiny();
  const ObOTimestampData r = datum2.get_otimestamp_tiny();
  return l.compare(r);
}

DEF_TYPE_TYPE_CMP(ObTimestampTZType, ObTimestampTZType)
{
  const ObOTimestampData l = datum1.get_otimestamp_tz();
  const ObOTimestampData r = datum2.get_otimestamp_tz();
  return l.compare(r);
}

DEF_TYPE_TYPE_CMP(ObIntervalYMType, ObIntervalYMType)
{
  return ObDatumCmpHelperByTC<ObIntTC, ObIntTC>::cmp(datum1, datum2);
}

// <IntervalDSType, IntervalDSType>
DEF_TYPE_TYPE_CMP(ObIntervalDSType, ObIntervalDSType)
{
  const ObIntervalDSValue* l = reinterpret_cast<const ObIntervalDSValue*>(ObDatumPayload<ObIntervalTC>::get(datum1));
  const ObIntervalDSValue* r = reinterpret_cast<const ObIntervalDSValue*>(ObDatumPayload<ObIntervalTC>::get(datum2));
  return l->compare(*r);
}

DEF_TYPE_TYPE_CMP(ObURowIDType, ObURowIDType)
{
  const ObURowIDData l(datum1.len_, reinterpret_cast<const uint8_t*>(datum1.ptr_));
  const ObURowIDData r(datum2.len_, reinterpret_cast<const uint8_t*>(datum2.ptr_));
  return l.compare(r);
}

//
// General comparison function definition
//

#define DEF_CMP_FUNC_BY_SIGN_UNSIGN(tc1, tc2) \
  template <>                                 \
  struct ObDatumCmpHelperByTC<tc1, tc2> : public ObDatumCmpHelperBySignSU<tc1, tc2> {}

#define DEF_CMP_FUNC_BY_UNSIGN_SIGN(tc1, tc2) \
  template <>                                 \
  struct ObDatumCmpHelperByTC<tc1, tc2> : public ObDatumCmpHelperBySignUS<tc1, tc2> {}

#define DEF_CMP_FUNC_BY_UNSIGN_UNSIGN(tc1, tc2) \
  template <>                                   \
  struct ObDatumCmpHelperByTC<tc1, tc2> : public ObDatumCmpHelperBySignUU<tc1, tc2> {}

#define UNDEF_CMP_FUNC_BY_TC(tc1, tc2) \
  template <>                          \
  struct ObDatumCmpHelperByTC<tc1, tc2> : public ObDatumCmpHelperByTC<ObMaxTC, ObMaxTC> {}

#define UNDEF_CMP_FUNC_BY_TYPE(type1, type2) \
  template <>                                \
  struct ObDatumCmpHelperByType<type1, type2> : public ObDatumCmpHelperByTC<ObMaxTC, ObMaxTC> {}

// Special <TC, TC> Cmp Funcs
DEF_CMP_FUNC_BY_SIGN_UNSIGN(ObIntTC, ObUIntTC);
DEF_CMP_FUNC_BY_SIGN_UNSIGN(ObIntTC, ObEnumSetTC);
DEF_CMP_FUNC_BY_UNSIGN_SIGN(ObUIntTC, ObIntTC);
DEF_CMP_FUNC_BY_UNSIGN_SIGN(ObEnumSetTC, ObIntTC);
DEF_CMP_FUNC_BY_UNSIGN_UNSIGN(ObUIntTC, ObEnumSetTC);
DEF_CMP_FUNC_BY_UNSIGN_UNSIGN(ObEnumSetTC, ObUIntTC);

// Do not Define These <TC, TC> Cmp Funcs
UNDEF_CMP_FUNC_BY_TC(ObStringTC, ObStringTC);
UNDEF_CMP_FUNC_BY_TC(ObRawTC, ObRawTC);
UNDEF_CMP_FUNC_BY_TC(ObTextTC, ObTextTC);
UNDEF_CMP_FUNC_BY_TC(ObIntervalTC, ObIntervalTC);
UNDEF_CMP_FUNC_BY_TC(ObEnumSetInnerTC, ObEnumSetInnerTC);
UNDEF_CMP_FUNC_BY_TC(ObOTimestampTC, ObOTimestampTC);
UNDEF_CMP_FUNC_BY_TC(ObRowIDTC, ObRowIDTC);
UNDEF_CMP_FUNC_BY_TC(ObLobTC, ObLobTC);

// Do not define These <Type, Type> Cmp Funcs
UNDEF_CMP_FUNC_BY_TYPE(ObDateTimeType, ObTimestampType);
UNDEF_CMP_FUNC_BY_TYPE(ObTimestampType, ObDateTimeType);

//
// Define comparison function according to Collation Type
//
typedef ObConstIntMapping<0, CS_TYPE_UTF8MB4_GENERAL_CI, 1, CS_TYPE_UTF8MB4_BIN, 1, CS_TYPE_BINARY, 1>
    SupportedCollections;

template <ObCollationType cs_type, bool calc_with_end_space>
struct ObDatumStrCmpCore {
  constexpr static bool defined_ = SupportedCollections::liner_search(cs_type);
  inline static int cmp(const ObDatum& datum1, const ObDatum& datum2)
  {
    int cmp_res = ObCharset::strcmpsp(cs_type,
        datum1.ptr_,
        static_cast<int64_t>(datum1.len_),
        datum2.ptr_,
        static_cast<int64_t>(datum2.len_),
        calc_with_end_space);
    return cmp_res > 0 ? 1 : (cmp_res < 0 ? -1 : 0);
  }
};

#undef DECL_SAME_TC_CMP
#undef IMPL_SAME_TC_CMP
#undef DEF_SAME_TC_CMP
#undef DECL_TYPE_TYPE_CMP
#undef IMPL_TYPE_TYPE_CMP
#undef DEF_TYPE_TYPE_CMP
#undef DECL_STR_CMP_FOR_CS
#undef IMPL_STR_CMP_FOR_CS
#undef DEF_STR_CMP_FOR_CS
#undef DEF_CMP_FUNC_BY_SIGN_UNSIGN
#undef DEF_CMP_FUNC_BY_UNSIGN_SIGN
#undef UNDEF_CMP_FUNC_BY_TC
#undef DEF_TC1_ANYTC_CMP
#undef DEF_TC2_ANTTC_CMP
#undef DEF_TC1_TC2_CMP
#undef DECL_TC1_ANYTC_CMP
#undef IMPL_TC1_ANYTC_CMP
#undef DECL_TC2_ANYTC_CMP
#undef IMPL_TC2_ANYTC_CMP
#undef DECL_TC1_TC2_CMP
#undef IMPL_TC1_TC2_CMP

#endif  // DATUM_CMP_FUNC_DEF

// ===============================================
// **************** Core Cmp Func ****************
// ===============================================

// Implementation of the core comparison function
// Only define the comparison between the same TypeClass (except <IntTC, EnumTC>, <IntTC, UIntTC>)
// The non-string comparison function can be implemented in two ways:
// #1. Same TC:
// a). <TypeClass, TypeClass> implementation, such as <IntTC, IntTC>.
// Use this method when the comparison method in TypeClass is the same
// b). <ObjType, ObjType> implementation, such as <IntervalYM, IntervalYM>.
// The same TypeClass needs to distinguish between types when comparing, so implement a separate comparison function for
// each type #2. Different TC The comparison between different TCs only defines: a) <IntTC, UIntTC> b) <IntTC,
// ObEnumSetTC> c) <ObUIntTC, ObtIntTC> d) <ObEnumSetTC, ObIntTC>
//
// String type comparisons can be compared as long as the Collation is the same, so
// Determine a comparison function according to the two dimensions of <ObCollationType, bool calc_with_end_space>
// calc_with_end_space calculation:
// bool is_calc_with_end_space(ObObjType type1, ObObjType type2,
//                            bool is_oracle_mode,
//                            ObCollationType cs_type1,
//                            ObCollationType cs_type2)
// {
//  return is_oracle_mode && ( (ObVarcharType == type1 && CS_TYPE_BINARY !=
//  cs_type1)
//                             || (ObVarcharType == type2 && CS_TYPE_BINARY !=
//                             cs_type2)
//                             || (ObNVarchar2Type == type1)
//                             || (ObNVarchar2Type == type2) );
// }
//
// Examples:
// 1. Add a new generic TypeClass implementation Do Nothing
// 2. Add a new specific implementation of TypeClass
// DEF_SAME_TC_CMP(tc) {
// func impl...
//}
// 3. Add a new specific implementation of ObjType
// DEF_TYPE_TYPE_CMP(type1, type2)
// {
// func impl...
//}
// 4. Do not define the comparison function of <TC1, TC2>: UNDEF_CMP_FUNC_BY_TC(TC1, TC2)
//
// 5. Define a new Collation Type string comparison function:
// DEF_STR_CMP_FUNC_BY_CS(NEW_CS_TYPE);
//
// 6. If the general string comparison logic is not applicable, you can define a new comparison logic
// DEF_STR_CMP_FOR_CS(cs_type)
// {
// func impl...
//}
template <ObObjType type1, ObObjType type2>
struct ObDatumCmpFuncByType : public ObDatumCmpHelperByType<type1, type2> {};

}  // end namespace cmp_func_helper
}  // end namespace common
}  // end namespace oceanbase
#endif  // OCEANBASE_OB_DATUM_CMP_FUNC_DEF_H_
