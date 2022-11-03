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
#include "sql/engine/ob_bit_vector.h"

namespace oceanbase {
using namespace sql;
namespace common {

template <ObObjType L_T, ObObjType R_T, bool NULL_FIRST>
struct ObNullSafeDatumTypeCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r) {
    int ret = 0;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDatumTypeCmp<L_T, R_T>::cmp(l, r);
    }
    return ret;
  }
};

template <ObObjTypeClass L_TC, ObObjTypeClass R_TC, bool NULL_FIRST>
struct ObNullSafeDatumTCCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r) {
    int ret = 0;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDatumTCCmp<L_TC, R_TC>::cmp(l, r);
    }
    return ret;
  }
};

template <ObCollationType CS_TYPE, bool WITH_END_SPACE, bool NULL_FIRST>
struct ObNullSafeDatumStrCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r) {
    int ret = 0;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDatumStrCmp<CS_TYPE, WITH_END_SPACE>::cmp(l, r);
    }
    return ret;
  }
};

static ObDatumCmpFuncType NULLSAFE_TYPE_CMP_FUNCS[ObMaxType][ObMaxType][2];
// init type type compare function array
template <int X, int Y>
struct InitTypeCmpArray
{
  template <bool... args>
  using Cmp = ObNullSafeDatumTypeCmp<
      static_cast<ObObjType>(X), static_cast<ObObjType>(Y), args...>;
  using Def = datum_cmp::ObDatumTypeCmp<static_cast<ObObjType>(X), static_cast<ObObjType>(Y)>;
  static void init_array()
  {
    NULLSAFE_TYPE_CMP_FUNCS[X][Y][0] = Def::defined_ ? &Cmp<0>::cmp : NULL;
    NULLSAFE_TYPE_CMP_FUNCS[X][Y][1] = Def::defined_ ? &Cmp<1>::cmp : NULL;
  }
};
bool g_type_cmp_array_inited = Ob2DArrayConstIniter<ObMaxType, ObMaxType, InitTypeCmpArray>::init();

static ObDatumCmpFuncType NULLSAFE_TC_CMP_FUNCS[ObMaxTC][ObMaxTC][2];
// init type class compare function array
template <int X, int Y>
struct InitTCCmpArray
{
  template <bool... args>
  using Cmp = ObNullSafeDatumTCCmp<
      static_cast<ObObjTypeClass>(X), static_cast<ObObjTypeClass>(Y), args...>;
  using Def = datum_cmp::ObDatumTCCmp<
      static_cast<ObObjTypeClass>(X),
      static_cast<ObObjTypeClass>(Y)>;
  static void init_array()
  {
    NULLSAFE_TC_CMP_FUNCS[X][Y][0] = Def::defined_ ? &Cmp<0>::cmp : NULL;
    NULLSAFE_TC_CMP_FUNCS[X][Y][1] = Def::defined_ ? &Cmp<1>::cmp : NULL;
  }
};

bool g_tc_cmp_array_inited = Ob2DArrayConstIniter<ObMaxTC, ObMaxTC, InitTCCmpArray>::init();

static int64_t fill_type_with_tc_cmp_func()
{
  int64_t cnt = 0;
  for (int64_t i = 0; i < ObMaxType; i++) {
    ObObjTypeClass i_tc = ob_obj_type_class((ObObjType)i);
    for (int64_t j = 0; j < ObMaxType; j++) {
      ObObjTypeClass j_tc = ob_obj_type_class((ObObjType)j);
      if (NULL == NULLSAFE_TYPE_CMP_FUNCS[i][j][0]) {
        NULLSAFE_TYPE_CMP_FUNCS[i][j][0] = NULLSAFE_TC_CMP_FUNCS[i_tc][j_tc][0];
        NULLSAFE_TYPE_CMP_FUNCS[i][j][1] = NULLSAFE_TC_CMP_FUNCS[i_tc][j_tc][1];
        cnt++;
      }
    }
  }
  return cnt;
}
int64_t g_fill_type_with_tc_cmp_func = fill_type_with_tc_cmp_func();

// cs_type, tenant_mode, calc_with_end_space
// now only RawTC, StringTC, TextTC defined str cmp funcs
static ObDatumCmpFuncType NULLSAFE_STR_CMP_FUNCS[CS_TYPE_MAX][2][2];
// int string compare function array
template <int IDX>
struct InitStrCmpArray
{
  template <bool... args>
  using StrCmp = ObNullSafeDatumStrCmp<static_cast<ObCollationType>(IDX), args...>;
  using Def = datum_cmp::ObDatumStrCmp<static_cast<ObCollationType>(IDX), false>;
  static void init_array()
  {
    auto &funcs = NULLSAFE_STR_CMP_FUNCS;
    funcs[IDX][0][0] = Def::defined_ ? &StrCmp<0, 0>::cmp : NULL;
    funcs[IDX][0][1] = Def::defined_ ? &StrCmp<0, 1>::cmp : NULL;
    funcs[IDX][1][0] = Def::defined_ ? &StrCmp<1, 0>::cmp : NULL;
    funcs[IDX][1][1] = Def::defined_ ? &StrCmp<1, 1>::cmp : NULL;
  }
};

bool g_str_cmp_array_inited = ObArrayConstIniter<CS_TYPE_MAX, InitStrCmpArray>::init();

ObDatumCmpFuncType ObDatumFuncs::get_nullsafe_cmp_func(
    const ObObjType type1, const ObObjType type2, const ObCmpNullPos null_pos,
    const ObCollationType cs_type, const bool is_oracle_mode) {
  OB_ASSERT(type1 >= ObNullType && type1 < ObMaxType);
  OB_ASSERT(type2 >= ObNullType && type2 < ObMaxType);
  OB_ASSERT(cs_type > CS_TYPE_INVALID && cs_type < CS_TYPE_MAX);
  OB_ASSERT(null_pos >= NULL_LAST && null_pos < MAX_NULL_POS);

  ObDatumCmpFuncType func_ptr = NULL;
  int null_pos_idx = NULL_LAST == null_pos ? 0 : 1;
  if (!is_string_type(type1) || !is_string_type(type2)) {
    func_ptr = NULLSAFE_TYPE_CMP_FUNCS[type1][type2][null_pos_idx];
  } else {
    int64_t calc_with_end_space_idx =
        (is_calc_with_end_space(type1, type2, is_oracle_mode, cs_type, cs_type) ? 1 : 0);
    func_ptr = NULLSAFE_STR_CMP_FUNCS[cs_type][calc_with_end_space_idx][null_pos_idx];
  }
  return func_ptr;
}

bool ObDatumFuncs::is_string_type(const ObObjType type)
{
  const ObObjTypeClass tc = OBJ_TYPE_TO_CLASS[type];
  return (tc == ObStringTC || tc == ObRawTC || tc == ObTextTC);
}

bool ObDatumFuncs::is_json(const ObObjType type)
{
  const ObObjTypeClass tc = OBJ_TYPE_TO_CLASS[type];
  return (tc == ObJsonTC);
}

OB_SERIALIZE_MEMBER(ObCmpFunc, ser_cmp_func_);
OB_SERIALIZE_MEMBER(ObHashFunc, ser_hash_func_, ser_batch_hash_func_);

template <typename T>
struct DefHashMethod
{
  typedef T HashMethod;
};
template <ObObjType type, typename T>
struct DatumHashCalculator : public DefHashMethod<T>
{
  static uint64_t calc_datum_hash(const ObDatum &datum, const uint64_t seed)
  {
    return ObjHashCalculator<type, T, ObDatum>::calc_hash_value(datum, seed);
  }
};

#define DEF_DATUM_TIMESTAMP_HASH_FUNCS(OBJTYPE, TYPE, DESC, VTYPE)              \
  template <typename T>                                                         \
  struct DatumHashCalculator<OBJTYPE, T> : public DefHashMethod<T>              \
  {                                                                             \
    static uint64_t calc_datum_hash(const ObDatum &datum, const uint64_t seed)  \
    {                                                                           \
      ObOTimestampData tmp_data = datum.get_##TYPE();                           \
      uint64_t v = T::hash(&tmp_data.time_us_, static_cast<int32_t>(sizeof(int64_t)), seed);  \
      return T::hash(&tmp_data.time_ctx_.DESC, static_cast<int32_t>(sizeof(VTYPE)), v);       \
    }                                                                                         \
  };

DEF_DATUM_TIMESTAMP_HASH_FUNCS(ObTimestampTZType, otimestamp_tz, desc_, uint32_t);
DEF_DATUM_TIMESTAMP_HASH_FUNCS(ObTimestampLTZType, otimestamp_tiny, time_desc_, uint16_t);
DEF_DATUM_TIMESTAMP_HASH_FUNCS(ObTimestampNanoType, otimestamp_tiny, time_desc_, uint16_t);

OB_INLINE static uint64_t datum_varchar_hash(const ObDatum &datum,
                                            const ObCollationType cs_type,
                                            const bool calc_end_space,
                                            const uint64_t seed,
                                            hash_algo algo)
{
  return ObCharset::hash(cs_type, datum.get_string().ptr(), datum.get_string().length(), seed,
                          calc_end_space, algo);
}

OB_INLINE static uint64_t datum_lob_locator_hash(const ObDatum &datum,
                                            const ObCollationType cs_type,
                                            const uint64_t seed,
                                            hash_algo algo)
{
  const ObLobLocator &lob_locator = datum.get_lob_locator();
  return ObCharset::hash(cs_type, lob_locator.get_payload_ptr(), lob_locator.payload_size_,
                        seed, algo);
}

template <ObCollationType cs_type, bool calc_end_space, typename T, bool is_lob_locator>
struct DatumStrHashCalculator : public DefHashMethod<T>
{
  static uint64_t calc_datum_hash(const ObDatum &datum, const uint64_t seed)
  {
    return datum_varchar_hash(datum, cs_type, calc_end_space, seed,
                              T::is_varchar_hash ? T::hash : NULL);
  }
};

template <ObCollationType cs_type, bool calc_end_space, typename T>
struct DatumStrHashCalculator<cs_type, calc_end_space, T, true> : public DefHashMethod<T>
{
  static uint64_t calc_datum_hash(const ObDatum &datum, const uint64_t seed)
  {
    return datum_lob_locator_hash(datum, cs_type, seed, T::is_varchar_hash ? T::hash : NULL);
  }
};

template <typename T, bool IS_VEC>
struct VectorIter
{
  explicit VectorIter(T *vec) : vec_(vec) {}
  T &operator[](const int64_t i) const { return IS_VEC ? vec_[i] : vec_[0]; }
private:
  T *vec_;
};

template <typename DatumHashFunc>
struct DefHashFunc
{
  static uint64_t null_hash(const uint64_t seed)
  {
    const int null_type = ObNullType;
    return DatumHashFunc::HashMethod::hash(&null_type, sizeof(null_type), seed);
  }

  static uint64_t hash(const ObDatum &datum, const uint64_t seed)
  {
    uint64_t v = 0;
    if (datum.is_null()) {
      v = null_hash(seed);
    } else {
      v = DatumHashFunc::calc_datum_hash(datum, seed);
    }
    return v;
  }

  template <typename DATUM_VEC, typename SEED_VEC>
  static void do_hash_batch(uint64_t *hash_values,
                         const DATUM_VEC &datum_vec,
                         const ObBitVector &skip,
                         const int64_t size,
                         const SEED_VEC &seed_vec)
  {
    ObBitVector::flip_foreach(skip, size,
      [&](int64_t idx) __attribute__((always_inline)) { hash_values[idx] = hash(datum_vec[idx], seed_vec[idx]); return OB_SUCCESS; }
    );
  }

  static void hash_batch(uint64_t *hash_values,
                         ObDatum *datums,
                         const bool is_batch_datum,
                         const ObBitVector &skip,
                         const int64_t size,
                         const uint64_t *seeds,
                         const bool is_batch_seed)
  {
    if (is_batch_datum && !is_batch_seed) {
      do_hash_batch(hash_values, VectorIter<const ObDatum, true>(datums), skip, size,
                    VectorIter<const uint64_t, false>(seeds));
    } else if (is_batch_datum && is_batch_seed) {
      do_hash_batch(hash_values, VectorIter<const ObDatum, true>(datums), skip, size,
                    VectorIter<const uint64_t, true>(seeds));
    } else if (!is_batch_datum && is_batch_seed) {
      do_hash_batch(hash_values, VectorIter<const ObDatum, false>(datums), skip, size,
                    VectorIter<const uint64_t, true>(seeds));
    } else {
      do_hash_batch(hash_values, VectorIter<const ObDatum, false>(datums), skip, size,
                    VectorIter<const uint64_t, false>(seeds));
    }
  }
};

static ObExprBasicFuncs EXPR_BASIC_FUNCS[ObMaxType];

// init basic function array
template <int X>
struct InitBasicFuncArray
{
  template <typename T>
  using Hash = DefHashFunc<DatumHashCalculator<static_cast<ObObjType>(X), T>>;
  template <bool NULL_FIRST>
  using TCCmp = ObNullSafeDatumTCCmp<
      ObObjTypeTraits<static_cast<ObObjType>(X)>::tc_,
      ObObjTypeTraits<static_cast<ObObjType>(X)>::tc_,
      NULL_FIRST>;
  using TCDef = datum_cmp::ObDatumTCCmp<
      ObObjTypeTraits<static_cast<ObObjType>(X)>::tc_,
      ObObjTypeTraits<static_cast<ObObjType>(X)>::tc_>;
  template <bool NULL_FIRST>
  using TypeCmp = ObNullSafeDatumTypeCmp<
      static_cast<ObObjType>(X),
      static_cast<ObObjType>(X),
      NULL_FIRST>;
  using TypeDef = datum_cmp::ObDatumTypeCmp<
      static_cast<ObObjType>(X),
      static_cast<ObObjType>(X)>;
  static void init_array()
  {
    auto &basic_funcs = EXPR_BASIC_FUNCS;
    basic_funcs[X].default_hash_ = Hash<ObDefaultHash>::hash;
    basic_funcs[X].default_hash_batch_= Hash<ObDefaultHash>::hash_batch;
    basic_funcs[X].murmur_hash_ = Hash<ObMurmurHash>::hash;
    basic_funcs[X].murmur_hash_batch_ = Hash<ObMurmurHash>::hash_batch;
    basic_funcs[X].xx_hash_ = Hash<ObXxHash>::hash;
    basic_funcs[X].xx_hash_batch_ = Hash<ObXxHash>::hash_batch;
    basic_funcs[X].wy_hash_ = Hash<ObWyHash>::hash;
    basic_funcs[X].wy_hash_batch_ = Hash<ObWyHash>::hash_batch;
    basic_funcs[X].null_first_cmp_ = TypeDef::defined_
        ? &TypeCmp<1>::cmp
        : TCDef::defined_ ? &TCCmp<1>::cmp : NULL;
    basic_funcs[X].null_last_cmp_ = TypeDef::defined_
        ? &TypeCmp<0>::cmp
        : TCDef::defined_ ? &TCCmp<0>::cmp : NULL;
  }
};

// [CS_TYPE][CALC_END_SPACE][IS_LOB_LOCATOR]
static ObExprBasicFuncs EXPR_BASIC_STR_FUNCS[CS_TYPE_MAX][2][2];

// init basic string function array
template <int X, int Y>
struct InitBasicStrFuncArray
{
  template <typename T, bool is_lob_locator>
  using Hash = DefHashFunc<DatumStrHashCalculator<static_cast<ObCollationType>(X),
        static_cast<bool>(Y), T, is_lob_locator>>;
  template <bool null_first>
  using StrCmp = ObNullSafeDatumStrCmp<static_cast<ObCollationType>(X),
                                       static_cast<bool>(Y), null_first>;
  using Def = datum_cmp::ObDatumStrCmp<
      static_cast<ObCollationType>(X),
      static_cast<bool>(Y)>;
  static void init_array()
  {
    if (datum_cmp::SupportedCollections::liner_search(static_cast<ObCollationType>(X))) {
      auto &basic_funcs = EXPR_BASIC_STR_FUNCS;
      basic_funcs[X][Y][0].default_hash_ = Hash<ObDefaultHash, false>::hash;
      basic_funcs[X][Y][0].default_hash_batch_ = Hash<ObDefaultHash, false>::hash_batch;
      basic_funcs[X][Y][0].murmur_hash_ = Hash<ObMurmurHash, false>::hash;
      basic_funcs[X][Y][0].murmur_hash_batch_ = Hash<ObMurmurHash, false>::hash_batch;
      basic_funcs[X][Y][0].xx_hash_ = Hash<ObXxHash, false>::hash;
      basic_funcs[X][Y][0].xx_hash_batch_ = Hash<ObXxHash, false>::hash_batch;
      basic_funcs[X][Y][0].wy_hash_ = Hash<ObWyHash, false>::hash;
      basic_funcs[X][Y][0].wy_hash_batch_ = Hash<ObWyHash, false>::hash_batch;
      basic_funcs[X][Y][0].null_first_cmp_ = Def::defined_ ? &StrCmp<1>::cmp : NULL;
      basic_funcs[X][Y][0].null_last_cmp_ = Def::defined_ ? &StrCmp<0>::cmp : NULL;

      basic_funcs[X][Y][1].default_hash_ = Hash<ObDefaultHash, true>::hash;
      basic_funcs[X][Y][1].default_hash_batch_ = Hash<ObDefaultHash, true>::hash_batch;
      basic_funcs[X][Y][1].murmur_hash_ = Hash<ObMurmurHash, true>::hash;
      basic_funcs[X][Y][1].murmur_hash_batch_ = Hash<ObMurmurHash, true>::hash_batch;
      basic_funcs[X][Y][1].xx_hash_ = Hash<ObXxHash, true>::hash;
      basic_funcs[X][Y][1].xx_hash_batch_ = Hash<ObXxHash, true>::hash_batch;
      basic_funcs[X][Y][1].wy_hash_ = Hash<ObWyHash, true>::hash;
      basic_funcs[X][Y][1].wy_hash_batch_ = Hash<ObWyHash, true>::hash_batch;
      basic_funcs[X][Y][1].null_first_cmp_ = NULL;  // cmp lob locator not support
      basic_funcs[X][Y][1].null_last_cmp_ = NULL;
    }
  }
};


bool g_basic_funcs_array_inited = ObArrayConstIniter<ObMaxType, InitBasicFuncArray>::init();
bool g_basic_str_array_inited = Ob2DArrayConstIniter<CS_TYPE_MAX, 2, InitBasicStrFuncArray>::init();

ObExprBasicFuncs* ObDatumFuncs::get_basic_func(const ObObjType type,
                                               const ObCollationType cs_type,
                                                  const bool is_oracle_mode)
{
  ObExprBasicFuncs *res = NULL;
  if ((type >= ObNullType && type < ObMaxType)) {
    if (is_string_type(type)) {
      OB_ASSERT(cs_type > CS_TYPE_INVALID && cs_type < CS_TYPE_MAX);
      bool calc_end_space = is_varying_len_char_type(type, cs_type) && is_oracle_mode;
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

} // end namespace common


// register function serialization
using namespace common;
namespace sql
{
// NULLSAFE_TYPE_CMP_FUNCS is two dimension array, need to convert to index stable array first.
static void *g_ser_type_cmp_funcs[ObMaxType][ObMaxType][2];
static_assert(ObMaxType * ObMaxType * 2 == sizeof(NULLSAFE_TYPE_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
bool g_ser_type_cmp_funcs_init = ObFuncSerialization::convert_NxN_array(
    reinterpret_cast<void **>(g_ser_type_cmp_funcs),
    reinterpret_cast<void **>(NULLSAFE_TYPE_CMP_FUNCS),
    ObMaxType,
    2,
    0,
    2);

REG_SER_FUNC_ARRAY(OB_SFA_DATUM_NULLSAFE_CMP,
                   g_ser_type_cmp_funcs,
                   sizeof(g_ser_type_cmp_funcs) / sizeof(void *));

static_assert(CS_TYPE_MAX * 2 * 2 == sizeof(NULLSAFE_STR_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_NULLSAFE_STR_CMP,
                   NULLSAFE_STR_CMP_FUNCS,
                   sizeof(NULLSAFE_STR_CMP_FUNCS) / sizeof(void*));

// When new function add to ObExprBasicFuncs, EXPR_BASIC_FUNCS should split into
// multi arrays for register.
struct ExprBasicFuncSerPart1
{
  void from(ObExprBasicFuncs &funcs)
  {
    funcs_[0] = reinterpret_cast<void *>(funcs.default_hash_);
    funcs_[1] = reinterpret_cast<void *>(funcs.murmur_hash_);
    funcs_[2] = reinterpret_cast<void *>(funcs.xx_hash_);
    funcs_[3] = reinterpret_cast<void *>(funcs.wy_hash_);
    funcs_[4] = reinterpret_cast<void *>(funcs.null_first_cmp_);
    funcs_[5] = reinterpret_cast<void *>(funcs.null_last_cmp_);
  }

  void *funcs_[6];
};
struct ExprBasicFuncSerPart2
{
  void *funcs_[6];
  void from(ObExprBasicFuncs &funcs)
  {
    funcs_[0] = reinterpret_cast<void *>(funcs.default_hash_batch_);
    funcs_[1] = reinterpret_cast<void *>(funcs.murmur_hash_batch_);
    funcs_[2] = reinterpret_cast<void *>(funcs.xx_hash_batch_);
    funcs_[3] = reinterpret_cast<void *>(funcs.wy_hash_batch_);
    // reserved
    // funcs_[4]
    // funcs_[5]
  }
};

static ExprBasicFuncSerPart1 EXPR_BASIC_FUNCS_PART1[ObMaxType];
static ExprBasicFuncSerPart2 EXPR_BASIC_FUNCS_PART2[ObMaxType];
static ExprBasicFuncSerPart1 EXPR_BASIC_STR_FUNCS_PART1[CS_TYPE_MAX][2][2];
static ExprBasicFuncSerPart2 EXPR_BASIC_STR_FUNCS_PART2[CS_TYPE_MAX][2][2];

bool split_basic_func_for_ser(void)
{
  for (int64_t i = 0; i < sizeof(EXPR_BASIC_FUNCS)/sizeof(ObExprBasicFuncs); i++) {
    EXPR_BASIC_FUNCS_PART1[i].from(EXPR_BASIC_FUNCS[i]);
    EXPR_BASIC_FUNCS_PART2[i].from(EXPR_BASIC_FUNCS[i]);
  }
  for (int64_t i = 0; i < sizeof(EXPR_BASIC_STR_FUNCS)/sizeof(ObExprBasicFuncs); i++) {
    reinterpret_cast<ExprBasicFuncSerPart1 *>(EXPR_BASIC_STR_FUNCS_PART1)[i].from(
        reinterpret_cast<ObExprBasicFuncs *>(EXPR_BASIC_STR_FUNCS)[i]);
    reinterpret_cast<ExprBasicFuncSerPart2 *>(EXPR_BASIC_STR_FUNCS_PART2)[i].from(
        reinterpret_cast<ObExprBasicFuncs *>(EXPR_BASIC_STR_FUNCS)[i]);
  }
  return true;
}
bool g_split_basic_func_for_ser = split_basic_func_for_ser();

static_assert(sizeof(sql::ObExprBasicFuncs) == 10 * sizeof(void *)
              && ObMaxType * 10 == sizeof(EXPR_BASIC_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_EXPR_BASIC_PART1,
                   EXPR_BASIC_FUNCS_PART1,
                   sizeof(EXPR_BASIC_FUNCS_PART1) / sizeof(void *));

REG_SER_FUNC_ARRAY(OB_SFA_EXPR_BASIC_PART2,
                   EXPR_BASIC_FUNCS_PART2,
                   sizeof(EXPR_BASIC_FUNCS_PART2) / sizeof(void *));

static_assert(CS_TYPE_MAX * 2 * 2 * 10 == sizeof(EXPR_BASIC_STR_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_EXPR_STR_BASIC_PART1,
                   EXPR_BASIC_STR_FUNCS_PART1,
                   sizeof(EXPR_BASIC_STR_FUNCS_PART1) / sizeof(void *));
REG_SER_FUNC_ARRAY(OB_SFA_EXPR_STR_BASIC_PART2,
                   EXPR_BASIC_STR_FUNCS_PART2,
                   sizeof(EXPR_BASIC_STR_FUNCS_PART2) / sizeof(void *));

} // end namespace sql
} // end namespace oceanbase
