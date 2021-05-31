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

#define USING_LOG_PREFIX SHARE

#include "ob_datum_funcs.h"
#include "ob_datum_cmp_func_def.h"
#include "common/object/ob_obj_funcs.h"
#include "sql/engine/ob_serializable_function.h"

namespace oceanbase {
using namespace sql;
namespace common {

using namespace cmp_func_helper;

template <ObObjType type1, ObObjType type2, bool null_first>
struct ObDatumNullsafeCmpFuncByType : public ObDatumCmpFuncByType<type1, type2> {
  inline static int cmp(const ObDatum& datum1, const ObDatum& datum2)
  {
    int ret = 0;
    if (datum1.is_null() && datum2.is_null()) {
      ret = 0;
    } else if (datum1.is_null()) {
      ret = null_first ? -1 : 1;
    } else if (datum2.is_null()) {
      ret = null_first ? 1 : -1;
    } else {
      ret = ObDatumCmpFuncByType<type1, type2>::cmp(datum1, datum2);
    }
    return ret;
  }
};

template <bool null_first>
struct ObDatumNullsafeCmpFuncByType<ObNullType, ObNullType, null_first> {
  constexpr static bool defined_ = true;
  inline static int cmp(const ObDatum& datum1, const ObDatum& datum2)
  {
    UNUSED(datum1);
    UNUSED(datum2);
    return 0;
  }
};

template <ObCollationType cs_type, bool calc_with_end_space, bool null_first>
struct ObDatumNullsafeStrCmp : public ObDatumStrCmpCore<cs_type, calc_with_end_space> {
  inline static int cmp(const ObDatum& datum1, const ObDatum& datum2)
  {
    int ret = 0;
    if (datum1.is_null() && datum2.is_null()) {
      ret = 0;
    } else if (datum1.is_null()) {
      ret = null_first ? -1 : 1;
    } else if (datum2.is_null()) {
      ret = null_first ? 1 : -1;
    } else {
      ret = ObDatumStrCmpCore<cs_type, calc_with_end_space>::cmp(datum1, datum2);
    }
    return ret;
  }
};

// init type type compare function array
template <int X, int Y>
struct InitTypeCmpArray {
  template <bool... args>
  using Cmp = ObDatumNullsafeCmpFuncByType<static_cast<ObObjType>(X), static_cast<ObObjType>(Y), args...>;
  static void init_array()
  {
    auto& funcs = ObDatumFuncs::NULLSAFE_CMP_FUNCS;
    funcs[X][Y][0] = Cmp<0>::defined_ ? &Cmp<0>::cmp : NULL;
    funcs[X][Y][1] = Cmp<1>::defined_ ? &Cmp<1>::cmp : NULL;
  }
};

ObDatumCmpFuncType ObDatumFuncs::NULLSAFE_CMP_FUNCS[ObMaxType][ObMaxType][2];

bool g_cmp_array_inited = Ob2DArrayConstIniter<ObMaxType, ObMaxType, InitTypeCmpArray>::init();

// int string compare function array
template <int IDX>
struct InitStrCmpArray {
  template <bool... args>
  using StrCmp = ObDatumNullsafeStrCmp<static_cast<ObCollationType>(IDX), args...>;
  static void init_array()
  {
    auto& funcs = ObDatumFuncs::NULLSAFE_STR_CMP_FUNCS;
    funcs[IDX][0][0] = StrCmp<0, 0>::defined_ ? &StrCmp<0, 0>::cmp : NULL;
    funcs[IDX][0][1] = StrCmp<0, 1>::defined_ ? &StrCmp<0, 1>::cmp : NULL;
    funcs[IDX][1][0] = StrCmp<1, 0>::defined_ ? &StrCmp<1, 0>::cmp : NULL;
    funcs[IDX][1][1] = StrCmp<1, 1>::defined_ ? &StrCmp<1, 0>::cmp : NULL;
  }
};

ObDatumCmpFuncType ObDatumFuncs::NULLSAFE_STR_CMP_FUNCS[CS_TYPE_MAX][2][2];
bool g_str_cmp_array_inited = ObArrayConstIniter<CS_TYPE_MAX, InitStrCmpArray>::init();

ObDatumCmpFuncType ObDatumFuncs::get_nullsafe_cmp_func(const ObObjType type1, const ObObjType type2,
    const ObCmpNullPos null_pos, const ObCollationType cs_type, const bool is_oracle_mode)
{
  OB_ASSERT(type1 >= ObNullType && type1 < ObMaxType);
  OB_ASSERT(type2 >= ObNullType && type2 < ObMaxType);
  OB_ASSERT(cs_type > CS_TYPE_INVALID && cs_type < CS_TYPE_MAX);
  OB_ASSERT(null_pos >= NULL_LAST && null_pos < MAX_NULL_POS);

  ObDatumCmpFuncType func_ptr = NULL;
  int null_pos_idx = NULL_LAST == null_pos ? 0 : 1;
  if (!is_string_type(type1) || !is_string_type(type2)) {
    func_ptr = NULLSAFE_CMP_FUNCS[type1][type2][null_pos_idx];
  } else {
    int64_t calc_with_end_space_idx = (is_calc_with_end_space(type1, type2, is_oracle_mode, cs_type, cs_type) ? 1 : 0);
    func_ptr = NULLSAFE_STR_CMP_FUNCS[cs_type][calc_with_end_space_idx][null_pos_idx];
  }
  return func_ptr;
}

bool ObDatumFuncs::is_string_type(const ObObjType type)
{
  const ObObjTypeClass tc = OBJ_TYPE_TO_CLASS[type];
  return (tc == ObStringTC || tc == ObRawTC || tc == ObTextTC || tc == ObEnumSetInnerTC);
}

OB_SERIALIZE_MEMBER(ObCmpFunc, ser_cmp_func_);
OB_SERIALIZE_MEMBER(ObHashFunc, ser_hash_func_);

template <ObObjType type, typename T>
struct DatumHashCalculator {
  static uint64_t calc_datum_hash(const ObDatum& datum, const uint64_t seed)
  {
    uint64_t res = seed;
    if (datum.is_null()) {
      int null_type = ObNullType;
      res = T::hash(&null_type, sizeof(null_type), res);
    } else {
      res = ObjHashCalculator<type, T, ObDatum>::calc_hash_value(datum, seed);
    }
    return res;
  }
};

#define DEF_DATUM_TIMESTAMP_HASH_FUNCS(OBJTYPE, TYPE, DESC, VTYPE)                         \
  template <typename T>                                                                    \
  struct DatumHashCalculator<OBJTYPE, T> {                                                 \
    static uint64_t calc_datum_hash(const ObDatum& datum, const uint64_t seed)             \
    {                                                                                      \
      uint64_t res = seed;                                                                 \
      if (datum.is_null()) {                                                               \
        int null_type = ObNullType;                                                        \
        res = T::hash(&null_type, sizeof(null_type), res);                                 \
      } else {                                                                             \
        ObOTimestampData tmp_data = datum.get_##TYPE();                                    \
        res = T::hash(&tmp_data.time_us_, static_cast<int32_t>(sizeof(int64_t)), seed);    \
        res = T::hash(&tmp_data.time_ctx_.DESC, static_cast<int32_t>(sizeof(VTYPE)), res); \
      }                                                                                    \
      return res;                                                                          \
    }                                                                                      \
  };

DEF_DATUM_TIMESTAMP_HASH_FUNCS(ObTimestampTZType, otimestamp_tz, desc_, uint32_t);
DEF_DATUM_TIMESTAMP_HASH_FUNCS(ObTimestampLTZType, otimestamp_tiny, time_desc_, uint16_t);
DEF_DATUM_TIMESTAMP_HASH_FUNCS(ObTimestampNanoType, otimestamp_tiny, time_desc_, uint16_t);

OB_INLINE static uint64_t datum_varchar_hash(
    const ObDatum& datum, const ObCollationType cs_type, const bool calc_end_space, const uint64_t seed, hash_algo algo)
{
  return ObCharset::hash(cs_type, datum.get_string().ptr(), datum.get_string().length(), seed, calc_end_space, algo);
}

OB_INLINE static uint64_t datum_lob_locator_hash(
    const ObDatum& datum, const ObCollationType cs_type, const uint64_t seed, hash_algo algo)
{
  const ObLobLocator& lob_locator = datum.get_lob_locator();
  return ObCharset::hash(cs_type, lob_locator.get_payload_ptr(), lob_locator.payload_size_, seed, algo);
}

template <ObCollationType cs_type, bool calc_end_space, typename T, bool is_lob_locator>
struct DatumStrHashCalculator {
  static uint64_t calc_datum_hash(const ObDatum& datum, const uint64_t seed)
  {
    uint64_t res = seed;
    if (datum.is_null()) {
      int null_type = ObNullType;
      res = T::hash(&null_type, sizeof(null_type), res);
    } else {
      res = datum_varchar_hash(datum, cs_type, calc_end_space, seed, T::is_varchar_hash ? T::hash : NULL);
    }
    return res;
  }
};

template <ObCollationType cs_type, bool calc_end_space, typename T>
struct DatumStrHashCalculator<cs_type, calc_end_space, T, true> {
  static uint64_t calc_datum_hash(const ObDatum& datum, const uint64_t seed)
  {
    uint64_t res = seed;
    if (datum.is_null()) {
      int null_type = ObNullType;
      res = T::hash(&null_type, sizeof(null_type), res);
    } else {
      res = datum_lob_locator_hash(datum, cs_type, seed, T::is_varchar_hash ? T::hash : NULL);
    }
    return res;
  }
};

// init basic function array
template <int X>
struct InitBasicFuncArray {
  template <typename T>
  using Hash = DatumHashCalculator<static_cast<ObObjType>(X), T>;
  template <bool null_first>
  using Cmp = ObDatumNullsafeCmpFuncByType<static_cast<ObObjType>(X), static_cast<ObObjType>(X), null_first>;
  static void init_array()
  {
    auto& basic_funcs = ObDatumFuncs::EXPR_BASIC_FUNCS;
    basic_funcs[X].default_hash_ = Hash<ObDefaultHash>::calc_datum_hash;
    basic_funcs[X].murmur_hash_ = Hash<ObMurmurHash>::calc_datum_hash;
    basic_funcs[X].xx_hash_ = Hash<ObXxHash>::calc_datum_hash;
    basic_funcs[X].wy_hash_ = Hash<ObWyHash>::calc_datum_hash;
    basic_funcs[X].null_first_cmp_ = Cmp<1>::defined_ ? &Cmp<1>::cmp : NULL;
    basic_funcs[X].null_last_cmp_ = Cmp<0>::defined_ ? &Cmp<0>::cmp : NULL;
  }
};

// init basic string function array
template <int X, int Y>
struct InitBasicStrFuncArray {
  template <typename T, bool is_lob_locator>
  using Hash = DatumStrHashCalculator<static_cast<ObCollationType>(X), static_cast<bool>(Y), T, is_lob_locator>;
  template <bool null_first>
  using StrCmp = ObDatumNullsafeStrCmp<static_cast<ObCollationType>(X), static_cast<bool>(Y), null_first>;
  static void init_array()
  {
    auto& basic_funcs = ObDatumFuncs::EXPR_BASIC_STR_FUNCS;
    basic_funcs[X][Y][0].default_hash_ = Hash<ObDefaultHash, false>::calc_datum_hash;
    basic_funcs[X][Y][0].murmur_hash_ = Hash<ObMurmurHash, false>::calc_datum_hash;
    basic_funcs[X][Y][0].xx_hash_ = Hash<ObXxHash, false>::calc_datum_hash;
    basic_funcs[X][Y][0].wy_hash_ = Hash<ObWyHash, false>::calc_datum_hash;
    basic_funcs[X][Y][0].null_first_cmp_ = StrCmp<1>::defined_ ? &StrCmp<1>::cmp : NULL;
    basic_funcs[X][Y][0].null_last_cmp_ = StrCmp<0>::defined_ ? &StrCmp<0>::cmp : NULL;

    basic_funcs[X][Y][1].default_hash_ = Hash<ObDefaultHash, true>::calc_datum_hash;
    basic_funcs[X][Y][1].murmur_hash_ = Hash<ObMurmurHash, true>::calc_datum_hash;
    basic_funcs[X][Y][1].xx_hash_ = Hash<ObXxHash, true>::calc_datum_hash;
    basic_funcs[X][Y][1].wy_hash_ = Hash<ObWyHash, true>::calc_datum_hash;
    basic_funcs[X][Y][1].null_first_cmp_ = NULL;  // cmp lob locator not support
    basic_funcs[X][Y][1].null_last_cmp_ = NULL;
  }
};

ObExprBasicFuncs ObDatumFuncs::EXPR_BASIC_FUNCS[ObMaxType];
ObExprBasicFuncs ObDatumFuncs::EXPR_BASIC_STR_FUNCS[CS_TYPE_MAX][2][2];

bool g_basic_funcs_array_inited = ObArrayConstIniter<ObMaxType, InitBasicFuncArray>::init();
bool g_basic_str_array_inited = Ob2DArrayConstIniter<CS_TYPE_MAX, 2, InitBasicStrFuncArray>::init();

ObExprBasicFuncs* ObDatumFuncs::get_basic_func(const ObObjType type, const ObCollationType cs_type)
{
  ObExprBasicFuncs* res = NULL;
  if ((type >= ObNullType && type < ObMaxType)) {
    if (is_string_type(type)) {
      OB_ASSERT(cs_type > CS_TYPE_INVALID && cs_type < CS_TYPE_MAX);
      bool calc_end_space = is_varying_len_char_type(type, cs_type) && lib::is_oracle_mode();
      bool is_lob_locator = false;
      res = &EXPR_BASIC_STR_FUNCS[cs_type][calc_end_space][is_lob_locator];
    } else if (ob_is_lob_locator(type)) {
      OB_ASSERT(cs_type > CS_TYPE_INVALID && cs_type < CS_TYPE_MAX);
      bool calc_end_space = false;
      bool is_lob_locator = true;
      res = &EXPR_BASIC_STR_FUNCS[cs_type][calc_end_space][is_lob_locator];
    } else {
      res = &EXPR_BASIC_FUNCS[type];
    }
  } else {
    LOG_WARN("invalid obj type", K(type));
  }
  return res;
}

}  // end namespace common

// register function serialization
using namespace common;
namespace sql {
// NULLSAFE_CMP_FUNCS is two dimension array, need to convert to index stable array first.
static void* g_ser_nullsafe_cmp_funcs[ObMaxType][ObMaxType][2];
static_assert(ObMaxType * ObMaxType * 2 == ObDatumFuncs::get_nullsafe_cmp_funcs_size(), "unexpected size");
bool g_ser_nullsafe_cmp_funcs_init = ObFuncSerialization::convert_NxN_array(
    reinterpret_cast<void**>(g_ser_nullsafe_cmp_funcs), ObDatumFuncs::get_nullsafe_cmp_funcs(), ObMaxType, 2, 0, 2);

REG_SER_FUNC_ARRAY(
    OB_SFA_DATUM_NULLSAFE_CMP, g_ser_nullsafe_cmp_funcs, sizeof(g_ser_nullsafe_cmp_funcs) / sizeof(void*));

static_assert(CS_TYPE_MAX * 2 * 2 == ObDatumFuncs::get_nullsafe_str_cmp_funcs_size(), "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_NULLSAFE_STR_CMP, ObDatumFuncs::get_nullsafe_str_cmp_funcs(),
    ObDatumFuncs::get_nullsafe_str_cmp_funcs_size());

// When new function add to ObExprBasicFuncs, EXPR_BASIC_FUNCS should split into
// multi arrays for register.
static_assert(
    sizeof(sql::ObExprBasicFuncs) == 6 * sizeof(void*) && ObMaxType * 6 == ObDatumFuncs::get_basic_funcs_size(),
    "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_EXPR_BASIC, ObDatumFuncs::get_basic_funcs(), ObDatumFuncs::get_basic_funcs_size());

static_assert(CS_TYPE_MAX * 2 * 2 * 6 == ObDatumFuncs::get_basic_str_funcs_size(), "unexpected size");
REG_SER_FUNC_ARRAY(
    OB_SFA_EXPR_STR_BASIC, ObDatumFuncs::get_basic_str_funcs(), ObDatumFuncs::get_basic_str_funcs_size());

}  // end namespace sql
}  // end namespace oceanbase
