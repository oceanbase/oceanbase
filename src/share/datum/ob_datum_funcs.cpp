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
#include "share/ob_cluster_version.h"
#include "share/vector/vector_op_util.h"
#include "share/vector/expr_cmp_func.h"

namespace oceanbase {
using namespace sql;
namespace common {

template <ObObjType L_T, ObObjType R_T, bool NULL_FIRST>
struct ObNullSafeDatumTypeCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      cmp_ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      cmp_ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDatumTypeCmp<L_T, R_T>::cmp(l, r, cmp_ret);
    }
    return ret;
  }
};

template <ObObjTypeClass L_TC, ObObjTypeClass R_TC, bool NULL_FIRST>
struct ObNullSafeDatumTCCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      cmp_ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      cmp_ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDatumTCCmp<L_TC, R_TC>::cmp(l, r, cmp_ret);
    }
    return ret;
  }
};

template <ObCollationType CS_TYPE, bool WITH_END_SPACE, bool NULL_FIRST>
struct ObNullSafeDatumStrCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      cmp_ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      cmp_ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDatumStrCmp<CS_TYPE, WITH_END_SPACE>::cmp(l, r, cmp_ret);
    }
    return ret;
  }
};

template <ObCollationType CS_TYPE, bool WITH_END_SPACE, bool NULL_FIRST>
struct ObNullSafeDatumTextCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      cmp_ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      cmp_ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDatumTextCmp<CS_TYPE, WITH_END_SPACE>::cmp(l, r, cmp_ret);
    }
    return ret;
  }
};

template <ObCollationType CS_TYPE, bool WITH_END_SPACE, bool NULL_FIRST>
struct ObNullSafeDatumTextStringCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      cmp_ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      cmp_ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDatumTextStringCmp<CS_TYPE, WITH_END_SPACE>::cmp(l, r, cmp_ret);
    }
    return ret;
  }
};

template <ObCollationType CS_TYPE, bool WITH_END_SPACE, bool NULL_FIRST>
struct ObNullSafeDatumStringTextCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      cmp_ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      cmp_ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDatumStringTextCmp<CS_TYPE, WITH_END_SPACE>::cmp(l, r, cmp_ret);
    }
    return ret;
  }
};

template <bool NULL_FIRST, bool HAS_LOB_HEADER>
struct ObNullSafeDatumJsonCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      cmp_ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      cmp_ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDatumJsonCmp<HAS_LOB_HEADER>::cmp(l, r, cmp_ret);
    }
    return ret;
  }
};

template <bool NULL_FIRST, bool HAS_LOB_HEADER>
struct ObNullSafeDatumGeoCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      cmp_ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      cmp_ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDatumGeoCmp<HAS_LOB_HEADER>::cmp(l, r, cmp_ret);
    }
    return ret;
  }
};

template <bool NULL_FIRST, bool HAS_LOB_HEADER>
struct ObNullSafeDatumUDTCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret) {
    return datum_cmp::ObDatumUDTCmp<HAS_LOB_HEADER>::cmp(l, r, cmp_ret);
  }
};

template <ObScale SCALE, bool NULL_FIRST>
struct ObNullSafeFixedDoubleCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      cmp_ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      cmp_ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObFixedDoubleCmp<SCALE>::cmp(l, r, cmp_ret);
    }
    return ret;
  }
};

template<ObDecimalIntWideType width1, ObDecimalIntWideType width2, bool NULL_FIRST>
struct ObNullSafeDecintCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret)
  {
    int ret = OB_SUCCESS;
    cmp_ret = 0;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      cmp_ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      cmp_ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDecintCmp<width1, width2>::cmp(l, r, cmp_ret);
    }
    return OB_SUCCESS;
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
static ObDatumCmpFuncType NULLSAFE_TEXT_CMP_FUNCS[CS_TYPE_MAX][2][2];
static ObDatumCmpFuncType NULLSAFE_TEXT_STR_CMP_FUNCS[CS_TYPE_MAX][2][2];
static ObDatumCmpFuncType NULLSAFE_STR_TEXT_CMP_FUNCS[CS_TYPE_MAX][2][2];

// int string compare function array
template <int IDX>
struct InitStrCmpArray
{
  template <bool... args>
  using StrCmp = ObNullSafeDatumStrCmp<static_cast<ObCollationType>(IDX), args...>;
  using Def = datum_cmp::ObDatumStrCmp<static_cast<ObCollationType>(IDX), false>;

  template <bool... args>
  using TextCmp = ObNullSafeDatumTextCmp<static_cast<ObCollationType>(IDX), args...>;
  using TextDef = datum_cmp::ObDatumTextCmp<static_cast<ObCollationType>(IDX), false>;

  template <bool... args>
  using TextStringCmp = ObNullSafeDatumTextStringCmp<static_cast<ObCollationType>(IDX), args...>;
  using TextStringDef = datum_cmp::ObDatumTextStringCmp<static_cast<ObCollationType>(IDX), false>;

  template <bool... args>
  using StringTextCmp = ObNullSafeDatumStringTextCmp<static_cast<ObCollationType>(IDX), args...>;
  using StringTextDef = datum_cmp::ObDatumStringTextCmp<static_cast<ObCollationType>(IDX), false>;

  static void init_array()
  {
    auto &funcs = NULLSAFE_STR_CMP_FUNCS;
    funcs[IDX][0][0] = Def::defined_ ? &StrCmp<0, 0>::cmp : NULL;
    funcs[IDX][0][1] = Def::defined_ ? &StrCmp<0, 1>::cmp : NULL;
    funcs[IDX][1][0] = Def::defined_ ? &StrCmp<1, 0>::cmp : NULL;
    funcs[IDX][1][1] = Def::defined_ ? &StrCmp<1, 1>::cmp : NULL;

    // int texts compare function array
    auto &text_funcs = NULLSAFE_TEXT_CMP_FUNCS;
    text_funcs[IDX][0][0] = TextDef::defined_ ? &TextCmp<0, 0>::cmp : NULL;
    text_funcs[IDX][0][1] = TextDef::defined_ ? &TextCmp<0, 1>::cmp : NULL;
    text_funcs[IDX][1][0] = TextDef::defined_ ? &TextCmp<1, 0>::cmp : NULL;
    text_funcs[IDX][1][1] = TextDef::defined_ ? &TextCmp<1, 1>::cmp : NULL;

    // int text compare string function array
    auto &text_string_funcs = NULLSAFE_TEXT_STR_CMP_FUNCS;
    text_string_funcs[IDX][0][0] = TextStringDef::defined_ ? &TextStringCmp<0, 0>::cmp : NULL;
    text_string_funcs[IDX][0][1] = TextStringDef::defined_ ? &TextStringCmp<0, 1>::cmp : NULL;
    text_string_funcs[IDX][1][0] = TextStringDef::defined_ ? &TextStringCmp<1, 0>::cmp : NULL;
    text_string_funcs[IDX][1][1] = TextStringDef::defined_ ? &TextStringCmp<1, 1>::cmp : NULL;

    // int text compare string function array
    auto &string_text_funcs = NULLSAFE_STR_TEXT_CMP_FUNCS;
    string_text_funcs[IDX][0][0] = StringTextDef::defined_ ? &StringTextCmp<0, 0>::cmp : NULL;
    string_text_funcs[IDX][0][1] = StringTextDef::defined_ ? &StringTextCmp<0, 1>::cmp : NULL;
    string_text_funcs[IDX][1][0] = StringTextDef::defined_ ? &StringTextCmp<1, 0>::cmp : NULL;
    string_text_funcs[IDX][1][1] = StringTextDef::defined_ ? &StringTextCmp<1, 1>::cmp : NULL;
  }
};

bool g_str_cmp_array_inited = ObArrayConstIniter<CS_TYPE_MAX, InitStrCmpArray>::init();

static ObDatumCmpFuncType NULLSAFE_JSON_CMP_FUNCS[2][2];

template<int IDX>
struct InitJsonCmpArray
{
  template <bool... args>
  using Cmp = ObNullSafeDatumJsonCmp<args...>;
  using Def = datum_cmp::ObDatumJsonCmp<false>;

  static void init_array()
  {
    auto &funcs = NULLSAFE_JSON_CMP_FUNCS;
    funcs[0][0] = Def::defined_ ? &Cmp<0, 0>::cmp : NULL;
    funcs[0][1] = Def::defined_ ? &Cmp<0, 1>::cmp : NULL;
    funcs[1][0] = Def::defined_ ? &Cmp<1, 1>::cmp : NULL;
    funcs[1][1] = Def::defined_ ? &Cmp<1, 1>::cmp : NULL;
  }
};

bool g_json_cmp_array_inited = ObArrayConstIniter<1, InitJsonCmpArray>::init();

static ObDatumCmpFuncType NULLSAFE_GEO_CMP_FUNCS[2][2];

template<int IDX>
struct InitGeoCmpArray
{
  template <bool... args>
  using Cmp = ObNullSafeDatumGeoCmp<args...>;
  using Def = datum_cmp::ObDatumGeoCmp<false>;

  static void init_array()
  {
    auto &funcs = NULLSAFE_GEO_CMP_FUNCS;
    funcs[0][0] = Def::defined_ ? &Cmp<0, 0>::cmp : NULL;
    funcs[0][1] = Def::defined_ ? &Cmp<0, 1>::cmp : NULL;
    funcs[1][0] = Def::defined_ ? &Cmp<1, 1>::cmp : NULL;
    funcs[1][1] = Def::defined_ ? &Cmp<1, 1>::cmp : NULL;
  }
};

bool g_geo_cmp_array_inited = ObArrayConstIniter<1, InitGeoCmpArray>::init();

static ObDatumCmpFuncType FIXED_DOUBLE_CMP_FUNCS[OB_NOT_FIXED_SCALE][2];
template <int X>
struct InitFixedDoubleCmpArray
{
  static void init_array()
  {
    auto &funcs = FIXED_DOUBLE_CMP_FUNCS;
    funcs[X][0] = ObNullSafeFixedDoubleCmp<static_cast<ObScale>(X), false>::cmp;
    funcs[X][1] = ObNullSafeFixedDoubleCmp<static_cast<ObScale>(X), true>::cmp;
  }
};

bool g_fixed_double_cmp_array_inited =
  ObArrayConstIniter<OB_NOT_FIXED_SCALE, InitFixedDoubleCmpArray>::init();

static ObDatumCmpFuncType DECINT_CMP_FUNCS[DECIMAL_INT_MAX][DECIMAL_INT_MAX][2];

template<int width1, int width2>
struct InitDecintCmpArray
{
  static void init_array()
  {
    auto &funcs = DECINT_CMP_FUNCS;
    funcs[width1][width2][0] =
      ObNullSafeDecintCmp<static_cast<ObDecimalIntWideType>(width1),
                          static_cast<ObDecimalIntWideType>(width2), false>::cmp;
    funcs[width1][width2][1] = ObNullSafeDecintCmp<static_cast<ObDecimalIntWideType>(width1),
                          static_cast<ObDecimalIntWideType>(width2), true>::cmp;
  }
};

bool g_decint_cmp_array_inited =
  Ob2DArrayConstIniter<DECIMAL_INT_MAX, DECIMAL_INT_MAX, InitDecintCmpArray>::init();

ObDatumCmpFuncType ObDatumFuncs::get_nullsafe_cmp_func(
    const ObObjType type1, const ObObjType type2, const ObCmpNullPos null_pos,
    const ObCollationType cs_type, const ObScale max_scale, const bool is_oracle_mode,
    const bool has_lob_header, const ObPrecision prec1, const ObPrecision prec2) {
  OB_ASSERT(type1 >= ObNullType && type1 < ObMaxType);
  OB_ASSERT(type2 >= ObNullType && type2 < ObMaxType);
  OB_ASSERT(cs_type > CS_TYPE_INVALID && cs_type < CS_TYPE_MAX);
  OB_ASSERT(null_pos >= NULL_LAST && null_pos < MAX_NULL_POS);

  ObDatumCmpFuncType func_ptr = NULL;
  int null_pos_idx = NULL_LAST == null_pos ? 0 : 1;
  if (is_string_type(type1) && is_string_type(type2)) {
    int64_t calc_with_end_space_idx =
        (is_calc_with_end_space(type1, type2, is_oracle_mode, cs_type, cs_type) ? 1 : 0);
    if (has_lob_header && (ob_is_large_text(type1) || ob_is_large_text(type2))) {
      if (ob_is_large_text(type1) && ob_is_large_text(type2)) {
        func_ptr = NULLSAFE_TEXT_CMP_FUNCS[cs_type][calc_with_end_space_idx][null_pos_idx];
      } else if (ob_is_large_text(type1)) { // type2 not large text
        func_ptr = NULLSAFE_TEXT_STR_CMP_FUNCS[cs_type][calc_with_end_space_idx][null_pos_idx];
      } else if (ob_is_large_text(type2)) { // type1 not large text
        func_ptr = NULLSAFE_STR_TEXT_CMP_FUNCS[cs_type][calc_with_end_space_idx][null_pos_idx];
      }
    } else { // no lob header or tinytext use original str cmp func
      func_ptr = NULLSAFE_STR_CMP_FUNCS[cs_type][calc_with_end_space_idx][null_pos_idx];
    }
  } else if (is_json(type1) && is_json(type2)) {
    func_ptr = NULLSAFE_JSON_CMP_FUNCS[null_pos_idx][has_lob_header];
  } else if (!is_oracle_mode && ob_is_double_type(type1) && ob_is_double_type(type1)
       && max_scale > SCALE_UNKNOWN_YET && max_scale < OB_NOT_FIXED_SCALE) {
    func_ptr = FIXED_DOUBLE_CMP_FUNCS[max_scale][null_pos_idx];
  } else if (is_geometry(type1) && is_geometry(type2)) {
    func_ptr = NULLSAFE_GEO_CMP_FUNCS[null_pos_idx][has_lob_header];
  } else if (ob_is_decimal_int(type1) && ob_is_decimal_int(type2) && prec1 != PRECISION_UNKNOWN_YET
             && prec2 != PRECISION_UNKNOWN_YET) {
    ObDecimalIntWideType lw = get_decimalint_type(prec1);
    ObDecimalIntWideType rw = get_decimalint_type(prec2);
    OB_ASSERT(lw >= 0 && lw < DECIMAL_INT_MAX);
    OB_ASSERT(rw >= 0 && rw < DECIMAL_INT_MAX);
    func_ptr = DECINT_CMP_FUNCS[lw][rw][null_pos_idx];
  } else if (ob_is_user_defined_sql_type(type1) || ob_is_user_defined_sql_type(type2)) {
    func_ptr = NULL;
  } else {
    func_ptr = NULLSAFE_TYPE_CMP_FUNCS[type1][type2][null_pos_idx];
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

bool ObDatumFuncs::is_geometry(const ObObjType type)
{
  const ObObjTypeClass tc = OBJ_TYPE_TO_CLASS[type];
  return (tc == ObGeometryTC);
}

/**
 * This function is primarily responsible for handling inconsistent hash computations
 * for null types and the null values of those types, such as string, float, double, etc.
 * It ensures that the hashing process treats null values and null type representations
 * consistently across such data types, avoiding discrepancies in hash results.
 */
bool ObDatumFuncs::is_null_aware_hash_type(const ObObjType type)
{
  const ObObjTypeClass tc = OBJ_TYPE_TO_CLASS[type];
  return is_string_type(type) || is_json(type) || is_geometry(type) ||
            (tc == ObUserDefinedSQLTC) || (tc == ObFloatTC) || (tc == ObDoubleTC);
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
  static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    return ObjHashCalculator<type, T, ObDatum>::calc_hash_value(datum, seed, res);
  }
  static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    res = T::hash(datum.ptr_, datum.len_, seed);
    return OB_SUCCESS;
  }
};

template <typename T, bool HAS_LOB_HEADER>
struct DatumJsonHashCalculator : public DefHashMethod<T>
{
  static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    int ret = OB_SUCCESS;
    common::ObString j_bin_str;
    res = 0;
    common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObTextStringIter str_iter(ObJsonType, CS_TYPE_BINARY, datum.get_string(), HAS_LOB_HEADER);
    if (datum.is_null()) {
      res = seed;
    } else if (OB_FAIL(str_iter.init(0, NULL, &allocator))) {
      LOG_WARN("Lob: str iter init failed ", K(ret), K(str_iter));
    } else if (OB_FAIL(str_iter.get_full_data(j_bin_str))) {
      LOG_WARN("Lob: str iter get full data failed ", K(ret), K(str_iter));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length(), &allocator);
      ObIJsonBase *j_base = &j_bin;
      if (j_bin_str.length() == 0) {
        res = seed;
      } else if (OB_FAIL(j_bin.reset_iter())) {
        LOG_WARN("Lob: fail to reset json bin iter", K(ret), K(j_bin_str));
      } else if (OB_FAIL(j_base->calc_json_hash_value(seed, T::hash, res))) {
        LOG_WARN("Lob: fail to calc hash", K(ret), K(*j_base));
      }
    }
    return ret;
  }

  static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res) {
    int ret = OB_SUCCESS;
    if (datum.is_null()) {
      res = seed;
    } else {
      ret = calc_datum_hash(datum, seed, res);
    }
    return ret;
  }
};

template <typename T, bool HAS_LOB_HEADER>
struct DatumGeoHashCalculator : public DefHashMethod<T>
{
  static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    return datum_lob_locator_hash(datum, CS_TYPE_UTF8MB4_BIN, seed, T::is_varchar_hash ? T::hash : NULL, res);
  }

  static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    return datum_lob_locator_hash(datum, CS_TYPE_UTF8MB4_BIN, seed, T::is_varchar_hash ? T::hash : NULL, res);
  }
};

template <typename T, bool HAS_LOB_HEADER>
struct DatumUDTHashCalculator : public DefHashMethod<T>
{
  static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    return datum_lob_locator_hash(datum, CS_TYPE_UTF8MB4_BIN, seed, T::is_varchar_hash ? T::hash : NULL, res);
  }

  static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    return datum_lob_locator_hash(datum, CS_TYPE_UTF8MB4_BIN, seed, T::is_varchar_hash ? T::hash : NULL, res);
  }
};

#define DEF_DATUM_TIMESTAMP_HASH_FUNCS(OBJTYPE, TYPE, DESC, VTYPE)              \
  template <typename T>                                                         \
  struct DatumHashCalculator<OBJTYPE, T> : public DefHashMethod<T>              \
  {                                                                             \
    static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)  \
    {                                                                           \
      ObOTimestampData tmp_data = datum.get_##TYPE();                           \
      res = T::hash(&tmp_data.time_us_, static_cast<int32_t>(sizeof(int64_t)), seed);         \
      res = T::hash(&tmp_data.time_ctx_.DESC, static_cast<int32_t>(sizeof(VTYPE)), res);      \
      return OB_SUCCESS;                                                                      \
    }                                                                                         \
    static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res)   \
    {                                                                                         \
      res = T::hash(datum.ptr_, datum.len_, seed);                                            \
      return OB_SUCCESS;                                                                      \
    }                                                                                         \
  };

DEF_DATUM_TIMESTAMP_HASH_FUNCS(ObTimestampTZType, otimestamp_tz, desc_, uint32_t);
DEF_DATUM_TIMESTAMP_HASH_FUNCS(ObTimestampLTZType, otimestamp_tiny, time_desc_, uint16_t);
DEF_DATUM_TIMESTAMP_HASH_FUNCS(ObTimestampNanoType, otimestamp_tiny, time_desc_, uint16_t);

// template specialization to deal with real type and json type
// calc_datum_hash_v2 will fallback to calc_datum_hash
#define DEF_DATUM_SPECIAL_HASH_FUNCS(OBJTYPE)                                               \
  template <typename T>                                                                     \
  struct DatumHashCalculator<OBJTYPE, T> : public DefHashMethod<T>                          \
  {                                                                                         \
    static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)    \
    {                                                                                       \
      return ObjHashCalculator<OBJTYPE, T, ObDatum>::calc_hash_value(datum, seed, res);     \
    }                                                                                       \
    static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res) {         \
      int ret = OB_SUCCESS;                                                                 \
      if (datum.is_null()) {                                                                \
        res = seed;                                                                         \
      } else {                                                                              \
        ret = calc_datum_hash(datum, seed, res);                                            \
      }                                                                                     \
      return ret;                                                                           \
    }                                                                                       \
  };

DEF_DATUM_SPECIAL_HASH_FUNCS(ObFloatType);
DEF_DATUM_SPECIAL_HASH_FUNCS(ObUFloatType);
DEF_DATUM_SPECIAL_HASH_FUNCS(ObDoubleType);
DEF_DATUM_SPECIAL_HASH_FUNCS(ObUDoubleType);
DEF_DATUM_SPECIAL_HASH_FUNCS(ObJsonType);
DEF_DATUM_SPECIAL_HASH_FUNCS(ObGeometryType);

OB_INLINE static uint64_t datum_varchar_hash(const ObDatum &datum,
                                             const ObCollationType cs_type,
                                             const bool calc_end_space,
                                             const uint64_t seed,
                                             hash_algo algo)
{
  return ObCharset::hash(cs_type, datum.ptr_, datum.len_, seed, calc_end_space, algo);
}

OB_INLINE static int datum_lob_locator_get_string(const ObDatum &datum,
                                                   ObIAllocator &allocator,
                                                   ObString& inrow_data)
{
  int ret = OB_SUCCESS;
  ObString raw_data = datum.get_string();
  ObLobLocatorV2 loc(raw_data, true);
  if (loc.is_lob_locator_v1()) {
    const ObLobLocator &lob_locator_v1 = datum.get_lob_locator();
    inrow_data.assign_ptr(lob_locator_v1.get_payload_ptr(), lob_locator_v1.payload_size_);
  } else if (loc.is_valid()) {
    ObTextStringIter text_iter(ObLongTextType, CS_TYPE_BINARY, datum.get_string(), true);
    if (OB_FAIL(text_iter.init(0, NULL, &allocator))) {
      LOG_WARN("Lob: str iter init failed ", K(ret), K(text_iter));
    } else if (OB_FAIL(text_iter.get_full_data(inrow_data))) {
      LOG_WARN("Lob: str iter get full data failed ", K(ret), K(text_iter));
    }
  } else { // not v1 or v2 lob
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Lob: str iter get full data failed ", K(ret), K(datum));
  }
  return ret;
}

OB_INLINE static int datum_lob_locator_hash(const ObDatum &datum,
                                            const ObCollationType cs_type,
                                            const uint64_t seed,
                                            hash_algo algo,
                                            uint64_t &res)
{
  int ret = OB_SUCCESS;
  if (datum.is_null()) {
    res = seed;
  } else {
    ObString inrow_data = datum.get_string();
    common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    // all lob tc can use longtext type for lob iter
    if (OB_FAIL(datum_lob_locator_get_string(datum, allocator, inrow_data))) {
      LOG_WARN("Lob: get string failed ", K(ret), K(datum));
    } else {
      res = ObCharset::hash(cs_type, inrow_data.ptr(), inrow_data.length(), seed, algo);
    }
  }
  return ret;
}

template <bool calc_end_space, typename T>
static uint64_t datum_varchar_hash_utf8mb4_bin(
       const char* str_ptr, const int32_t str_len, uint64_t seed)
{
  uint64_t ret = 0;
  if (calc_end_space) {
    ret = T::hash((void*)str_ptr, str_len, seed);
  } else {
    const uchar *key = reinterpret_cast<const uchar *>(str_ptr);
    const uchar *pos = key;
    int length = str_len;
    key = skip_trailing_space(key, str_len, 0);
    length = (int)(key - pos);
    ret = T::hash((void*)pos, length, seed);
  }
  return ret;
}

template <ObCollationType cs_type, bool calc_end_space, typename T, bool is_lob_locator>
struct DatumStrHashCalculator : public DefHashMethod<T>
{
  static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    res = datum_varchar_hash(
          datum, cs_type, calc_end_space, seed, T::is_varchar_hash ? T::hash : NULL);
    return OB_SUCCESS;
  }
  static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    res = datum_varchar_hash(
          datum, cs_type, calc_end_space, seed, T::is_varchar_hash ? T::hash : NULL);
    return OB_SUCCESS;
  }
};

template <ObCollationType cs_type, bool calc_end_space, typename T>
struct DatumStrHashCalculator<cs_type, calc_end_space, T, true /* is_lob_locator */>
       : public DefHashMethod<T>
{
  static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    return datum_lob_locator_hash(datum, cs_type, seed, T::is_varchar_hash ? T::hash : NULL, res);
  }
  static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    return datum_lob_locator_hash(datum, cs_type, seed, T::is_varchar_hash ? T::hash : NULL, res);
  }
};

template <bool calc_end_space, typename T>
struct DatumStrHashCalculator<CS_TYPE_UTF8MB4_BIN, calc_end_space, T, false /* is_lob_locator */>
       : public DefHashMethod<T>
{
  static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    res = datum_varchar_hash(
          datum, CS_TYPE_UTF8MB4_BIN, calc_end_space, seed, T::is_varchar_hash ? T::hash : NULL);
    return OB_SUCCESS;
  }
  static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {

    res = datum_varchar_hash_utf8mb4_bin<calc_end_space, T>(datum.ptr_, datum.len_, seed);
    return OB_SUCCESS;
  }
};

template <bool calc_end_space, typename T>
struct DatumStrHashCalculator<CS_TYPE_UTF8MB4_BIN, calc_end_space, T, true /* is_lob_locator */>
       : public DefHashMethod<T>
{
  static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    return datum_lob_locator_hash(datum, CS_TYPE_UTF8MB4_BIN, seed, T::is_varchar_hash ? T::hash : NULL, res);
  }
  static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    int ret = OB_SUCCESS;
    ObString inrow_data = datum.get_string();
    const ObLobCommon &lob = datum.get_lob_data();
    if (datum.len_ != 0 && !lob.is_mem_loc_ && lob.in_row_) {
      res = datum_varchar_hash_utf8mb4_bin<calc_end_space, T>(
              lob.get_inrow_data_ptr(), static_cast<int32_t>(lob.get_byte_size(datum.len_)), seed);
    } else {
      common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      // all lob tc can use longtext type for lob iter
      if (OB_FAIL(datum_lob_locator_get_string(datum, allocator, inrow_data))) {
        LOG_WARN("Lob: get string failed ", K(ret), K(datum));
      } else {
        res = datum_varchar_hash_utf8mb4_bin<calc_end_space, T>(
              inrow_data.ptr(), inrow_data.length(), seed);
      }
    }
    return ret;
  }
};

template <ObScale SCALE, typename T>
struct DatumFixedDoubleHashCalculator : public DefHashMethod<T>
{
  static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    // format fixed double to string first, then calc hash value of the string
    const double d_val = datum.get_double();
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t length = ob_fcvt(d_val, static_cast<int>(SCALE), sizeof(buf) - 1, buf, NULL);
    res = T::hash(buf, static_cast<int32_t>(length), seed);
    return OB_SUCCESS;
  }

  static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    int ret = OB_SUCCESS;
    if (datum.is_null()) {
      res = seed;
    } else {
      ret = calc_datum_hash(datum, seed, res);
    }
    return ret;
  }
};

template <typename DatumHashFunc>
struct DefHashFunc
{
  static int null_hash(const uint64_t seed, uint64_t &res)
  {
    const int null_type = ObNullType;
    res = DatumHashFunc::HashMethod::hash(&null_type, sizeof(null_type), seed);
    return OB_SUCCESS;
  }

  static int hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    int ret = OB_SUCCESS;
    if (datum.is_null()) {
      ret = null_hash(seed, res);
    } else {
      ret = DatumHashFunc::calc_datum_hash(datum, seed, res);
    }
    return ret;
  }

  OB_INLINE static int hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    return DatumHashFunc::calc_datum_hash_v2(datum, seed, res);
  }

  template <typename DATUM_VEC, typename SEED_VEC>
  static void do_hash_batch(uint64_t *hash_values,
                         const DATUM_VEC &datum_vec,
                         const ObBitVector &skip,
                         const int64_t size,
                         const SEED_VEC &seed_vec)
  {
    ObBitVector::flip_foreach(skip, size,
      [&](int64_t idx) __attribute__((always_inline)) {
        int ret = OB_SUCCESS;
        ret = hash(datum_vec[idx], seed_vec[idx], hash_values[idx]);
        return ret;
      }
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

  template <typename DATUM_VEC, typename SEED_VEC>
  static void do_hash_v2_batch(uint64_t *hash_values,
                         const DATUM_VEC &datum_vec,
                         const ObBitVector &skip,
                         const int64_t size,
                         const SEED_VEC &seed_vec)
  {
    ObBitVector::flip_foreach(skip, size,
      [&](int64_t idx) __attribute__((always_inline)) {
        int ret = OB_SUCCESS;
        ret = hash_v2(datum_vec[idx], seed_vec[idx], hash_values[idx]);
        return ret;
      }
    );
  }

  static void hash_v2_batch(uint64_t *hash_values,
                         ObDatum *datums,
                         const bool is_batch_datum,
                         const ObBitVector &skip,
                         const int64_t size,
                         const uint64_t *seeds,
                         const bool is_batch_seed)
  {
    if (is_batch_datum && !is_batch_seed) {
      do_hash_v2_batch(hash_values, VectorIter<const ObDatum, true>(datums), skip, size,
                    VectorIter<const uint64_t, false>(seeds));
    } else if (is_batch_datum && is_batch_seed) {
      do_hash_v2_batch(hash_values, VectorIter<const ObDatum, true>(datums), skip, size,
                    VectorIter<const uint64_t, true>(seeds));
    } else if (!is_batch_datum && is_batch_seed) {
      do_hash_v2_batch(hash_values, VectorIter<const ObDatum, false>(datums), skip, size,
                    VectorIter<const uint64_t, true>(seeds));
    } else {
      do_hash_v2_batch(hash_values, VectorIter<const ObDatum, false>(datums), skip, size,
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
    basic_funcs[X].murmur_hash_v2_ = Hash<ObMurmurHash>::hash_v2;
    basic_funcs[X].murmur_hash_v2_batch_ = Hash<ObMurmurHash>::hash_v2_batch;
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
  template <bool null_first>
  using TextCmp = ObNullSafeDatumTextCmp<static_cast<ObCollationType>(X),
                                         static_cast<bool>(Y), null_first>;
  using Def = datum_cmp::ObDatumStrCmp<
      static_cast<ObCollationType>(X),
      static_cast<bool>(Y)>;
    using DefText = datum_cmp::ObDatumTextCmp<
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
      basic_funcs[X][Y][0].murmur_hash_v2_ = Hash<ObMurmurHash, false>::hash_v2;
      basic_funcs[X][Y][0].murmur_hash_v2_batch_ = Hash<ObMurmurHash, false>::hash_v2_batch;

      basic_funcs[X][Y][1].default_hash_ = Hash<ObDefaultHash, true>::hash;
      basic_funcs[X][Y][1].default_hash_batch_ = Hash<ObDefaultHash, true>::hash_batch;
      basic_funcs[X][Y][1].murmur_hash_ = Hash<ObMurmurHash, true>::hash;
      basic_funcs[X][Y][1].murmur_hash_batch_ = Hash<ObMurmurHash, true>::hash_batch;
      basic_funcs[X][Y][1].xx_hash_ = Hash<ObXxHash, true>::hash;
      basic_funcs[X][Y][1].xx_hash_batch_ = Hash<ObXxHash, true>::hash_batch;
      basic_funcs[X][Y][1].wy_hash_ = Hash<ObWyHash, true>::hash;
      basic_funcs[X][Y][1].wy_hash_batch_ = Hash<ObWyHash, true>::hash_batch;

      // Notice: ObLobType cannot compare, but is_locator = 1 used for other lob types
      basic_funcs[X][Y][1].null_first_cmp_ = DefText::defined_ ? &TextCmp<1>::cmp : NULL;
      basic_funcs[X][Y][1].null_last_cmp_ = DefText::defined_ ? &TextCmp<0>::cmp : NULL;
      basic_funcs[X][Y][1].murmur_hash_v2_ = Hash<ObMurmurHash, true>::hash_v2;
      basic_funcs[X][Y][1].murmur_hash_v2_batch_ = Hash<ObMurmurHash, true>::hash_v2_batch;
    }
  }
};

static ObExprBasicFuncs EXPR_BASIC_JSON_FUNCS[2];

template<int IDX>
struct InitBasicJsonFuncArray
{
  template <typename T, bool HAS_LOB_HEADER>
  using Hash = DefHashFunc<DatumJsonHashCalculator<T, HAS_LOB_HEADER>>;
  template <bool NULL_FIRST>
  using TCCmp = ObNullSafeDatumTCCmp<ObJsonTC, ObJsonTC, NULL_FIRST>;
  using TCDef = datum_cmp::ObDatumTCCmp<ObJsonTC, ObJsonTC>;
  template <bool NULL_FIRST, bool HAS_LOB_HEADER>
  using TypeCmp = ObNullSafeDatumJsonCmp<NULL_FIRST, HAS_LOB_HEADER>;
  using TypeDef = datum_cmp::ObDatumJsonCmp<false>;

  static void init_array()
  {
    auto &basic_funcs = EXPR_BASIC_JSON_FUNCS;
    basic_funcs[0].default_hash_ = Hash<ObDefaultHash, false>::hash;
    basic_funcs[0].default_hash_batch_= Hash<ObDefaultHash, false>::hash_batch;
    basic_funcs[0].murmur_hash_ = Hash<ObMurmurHash, false>::hash;
    basic_funcs[0].murmur_hash_batch_ = Hash<ObMurmurHash, false>::hash_batch;
    basic_funcs[0].xx_hash_ = Hash<ObXxHash, false>::hash;
    basic_funcs[0].xx_hash_batch_ = Hash<ObXxHash, false>::hash_batch;
    basic_funcs[0].wy_hash_ = Hash<ObWyHash, false>::hash;
    basic_funcs[0].wy_hash_batch_ = Hash<ObWyHash, false>::hash_batch;
    basic_funcs[0].null_first_cmp_ = TypeDef::defined_
        ? &TypeCmp<1, 0>::cmp
        : TCDef::defined_ ? &TCCmp<1>::cmp : NULL;
    basic_funcs[0].null_last_cmp_ = TypeDef::defined_
        ? &TypeCmp<0, 0>::cmp
        : TCDef::defined_ ? &TCCmp<0>::cmp : NULL;
    basic_funcs[0].murmur_hash_v2_ = Hash<ObMurmurHash, false>::hash_v2;
    basic_funcs[0].murmur_hash_v2_batch_ = Hash<ObMurmurHash, false>::hash_v2_batch;

    basic_funcs[1].default_hash_ = Hash<ObDefaultHash, true>::hash;
    basic_funcs[1].default_hash_batch_= Hash<ObDefaultHash, true>::hash_batch;
    basic_funcs[1].murmur_hash_ = Hash<ObMurmurHash, true>::hash;
    basic_funcs[1].murmur_hash_batch_ = Hash<ObMurmurHash, true>::hash_batch;
    basic_funcs[1].xx_hash_ = Hash<ObXxHash, true>::hash;
    basic_funcs[1].xx_hash_batch_ = Hash<ObXxHash, true>::hash_batch;
    basic_funcs[1].wy_hash_ = Hash<ObWyHash, true>::hash;
    basic_funcs[1].wy_hash_batch_ = Hash<ObWyHash, true>::hash_batch;
    basic_funcs[1].null_first_cmp_ = TypeDef::defined_
        ? &TypeCmp<1, 1>::cmp
        : TCDef::defined_ ? &TCCmp<1>::cmp : NULL;
    basic_funcs[1].null_last_cmp_ = TypeDef::defined_
        ? &TypeCmp<0, 1>::cmp
        : TCDef::defined_ ? &TCCmp<0>::cmp : NULL;
    basic_funcs[1].murmur_hash_v2_ = Hash<ObMurmurHash, true>::hash_v2;
    basic_funcs[1].murmur_hash_v2_batch_ = Hash<ObMurmurHash, true>::hash_v2_batch;
  }
};

static ObExprBasicFuncs EXPR_BASIC_GEO_FUNCS[2];
template<int IDX>
struct InitBasicGeoFuncArray
{
  template <typename T, bool HAS_LOB_HEADER>
  using Hash = DefHashFunc<DatumGeoHashCalculator<T, HAS_LOB_HEADER>>;
  template <bool NULL_FIRST>
  using TCCmp = ObNullSafeDatumTCCmp<ObGeometryTC, ObGeometryTC, NULL_FIRST>;
  using TCDef = datum_cmp::ObDatumTCCmp<ObGeometryTC, ObGeometryTC>;
  template <bool NULL_FIRST, bool HAS_LOB_HEADER>
  using TypeCmp = ObNullSafeDatumGeoCmp<NULL_FIRST, HAS_LOB_HEADER>;
  using TypeDef = datum_cmp::ObDatumGeoCmp<false>;

  static void init_array()
  {
    auto &basic_funcs = EXPR_BASIC_GEO_FUNCS;
    basic_funcs[0].default_hash_ = Hash<ObDefaultHash, false>::hash;
    basic_funcs[0].default_hash_batch_= Hash<ObDefaultHash, false>::hash_batch;
    basic_funcs[0].murmur_hash_ = Hash<ObMurmurHash, false>::hash;
    basic_funcs[0].murmur_hash_batch_ = Hash<ObMurmurHash, false>::hash_batch;
    basic_funcs[0].xx_hash_ = Hash<ObXxHash, false>::hash;
    basic_funcs[0].xx_hash_batch_ = Hash<ObXxHash, false>::hash_batch;
    basic_funcs[0].wy_hash_ = Hash<ObWyHash, false>::hash;
    basic_funcs[0].wy_hash_batch_ = Hash<ObWyHash, false>::hash_batch;
    basic_funcs[0].null_first_cmp_ = TypeDef::defined_
        ? &TypeCmp<1, 0>::cmp
        : TCDef::defined_ ? &TCCmp<1>::cmp : NULL;
    basic_funcs[0].null_last_cmp_ = TypeDef::defined_
        ? &TypeCmp<0, 0>::cmp
        : TCDef::defined_ ? &TCCmp<0>::cmp : NULL;
    basic_funcs[0].murmur_hash_v2_ = Hash<ObMurmurHash, false>::hash_v2;
    basic_funcs[0].murmur_hash_v2_batch_ = Hash<ObMurmurHash, false>::hash_v2_batch;

    basic_funcs[1].default_hash_ = Hash<ObDefaultHash, true>::hash;
    basic_funcs[1].default_hash_batch_= Hash<ObDefaultHash, true>::hash_batch;
    basic_funcs[1].murmur_hash_ = Hash<ObMurmurHash, true>::hash;
    basic_funcs[1].murmur_hash_batch_ = Hash<ObMurmurHash, true>::hash_batch;
    basic_funcs[1].xx_hash_ = Hash<ObXxHash, true>::hash;
    basic_funcs[1].xx_hash_batch_ = Hash<ObXxHash, true>::hash_batch;
    basic_funcs[1].wy_hash_ = Hash<ObWyHash, true>::hash;
    basic_funcs[1].wy_hash_batch_ = Hash<ObWyHash, true>::hash_batch;
    basic_funcs[1].null_first_cmp_ = TypeDef::defined_
        ? &TypeCmp<1, 1>::cmp
        : TCDef::defined_ ? &TCCmp<1>::cmp : NULL;
    basic_funcs[1].null_last_cmp_ = TypeDef::defined_
        ? &TypeCmp<0, 1>::cmp
        : TCDef::defined_ ? &TCCmp<0>::cmp : NULL;
    basic_funcs[1].murmur_hash_v2_ = Hash<ObMurmurHash, true>::hash_v2;
    basic_funcs[1].murmur_hash_v2_batch_ = Hash<ObMurmurHash, true>::hash_v2_batch;
  }
};

static ObExprBasicFuncs FIXED_DOUBLE_BASIC_FUNCS[OB_NOT_FIXED_SCALE];
template <int X>
struct InitFixedDoubleBasicFuncArray
{
  template <typename T>
  using Hash = DefHashFunc<DatumFixedDoubleHashCalculator<static_cast<ObScale>(X), T>>;
  static void init_array()
  {
    auto &basic_funcs = FIXED_DOUBLE_BASIC_FUNCS;
    basic_funcs[X].default_hash_ = Hash<ObDefaultHash>::hash;
    basic_funcs[X].default_hash_batch_= Hash<ObDefaultHash>::hash_batch;
    basic_funcs[X].murmur_hash_ = Hash<ObMurmurHash>::hash;
    basic_funcs[X].murmur_hash_batch_ = Hash<ObMurmurHash>::hash_batch;
    basic_funcs[X].xx_hash_ = Hash<ObXxHash>::hash;
    basic_funcs[X].xx_hash_batch_ = Hash<ObXxHash>::hash_batch;
    basic_funcs[X].wy_hash_ = Hash<ObWyHash>::hash;
    basic_funcs[X].wy_hash_batch_ = Hash<ObWyHash>::hash_batch;
    basic_funcs[X].null_first_cmp_ = ObNullSafeFixedDoubleCmp<static_cast<ObScale>(X), true>::cmp;
    basic_funcs[X].null_last_cmp_ = ObNullSafeFixedDoubleCmp<static_cast<ObScale>(X), false>::cmp;
    basic_funcs[X].murmur_hash_v2_ = Hash<ObMurmurHash>::hash_v2;
    basic_funcs[X].murmur_hash_v2_batch_ = Hash<ObMurmurHash>::hash_v2_batch;
  }
};

static ObExprBasicFuncs EXPR_BASIC_UDT_FUNCS[1];
template <int X>
struct InitUDTBasicFuncArray
{
  // only for storage use, udt types are used as null bitmap in storage
  // storage will use murmur_hash_ and null_last_cmp_ maybe, so keep the origin basic func define and others
  // basic func return error code wheh is called: hash func return OB_NOT_SUPPORTED and cmp func return OB_ERR_NO_ORDER_MAP_SQL
  template <typename T>
  using StrHash = DefHashFunc<DatumStrHashCalculator<CS_TYPE_BINARY, false, T, false>>;
  template <typename T, bool HAS_LOB_HEADER>
  using Hash = DefHashFunc<DatumUDTHashCalculator<T, HAS_LOB_HEADER>>;
  template <bool NULL_FIRST, bool HAS_LOB_HEADER>
  using TypeCmp = ObNullSafeDatumUDTCmp<NULL_FIRST, HAS_LOB_HEADER>;
  template <bool null_first>
  using StrCmp = ObNullSafeDatumStrCmp<CS_TYPE_BINARY, false, null_first>;
  using Def = datum_cmp::ObDatumStrCmp<CS_TYPE_BINARY, false>;
  static void init_array()
  {
    auto &basic_funcs = EXPR_BASIC_UDT_FUNCS;
    basic_funcs[X].default_hash_ = Hash<ObDefaultHash, true>::hash;
    basic_funcs[X].default_hash_batch_= Hash<ObDefaultHash, true>::hash_batch;
    basic_funcs[X].murmur_hash_ = StrHash<ObMurmurHash>::hash;
    basic_funcs[X].murmur_hash_batch_ = Hash<ObMurmurHash, true>::hash_batch;
    basic_funcs[X].xx_hash_ = Hash<ObXxHash, true>::hash;
    basic_funcs[X].xx_hash_batch_ = Hash<ObXxHash, true>::hash_batch;
    basic_funcs[X].wy_hash_ = Hash<ObWyHash, true>::hash;
    basic_funcs[X].wy_hash_batch_ = Hash<ObWyHash, true>::hash_batch;
    basic_funcs[X].null_first_cmp_ = &TypeCmp<1, 1>::cmp;
    basic_funcs[X].null_last_cmp_ = Def::defined_ ? &StrCmp<0>::cmp : NULL;
    basic_funcs[X].murmur_hash_v2_ = Hash<ObMurmurHash, true>::hash_v2;
    basic_funcs[X].murmur_hash_v2_batch_ = Hash<ObMurmurHash, true>::hash_v2_batch;
  }
};

bool g_basic_funcs_array_inited = ObArrayConstIniter<ObMaxType, InitBasicFuncArray>::init();
bool g_basic_str_array_inited = Ob2DArrayConstIniter<CS_TYPE_MAX, 2, InitBasicStrFuncArray>::init();
bool g_basic_json_array_inited = ObArrayConstIniter<1, InitBasicJsonFuncArray>::init();
bool g_basic_geo_array_inited = ObArrayConstIniter<1, InitBasicGeoFuncArray>::init();

bool g_fixed_double_basic_func_array_inited =
  ObArrayConstIniter<OB_NOT_FIXED_SCALE, InitFixedDoubleBasicFuncArray>::init();

static ObExprBasicFuncs DECINT_BASIC_FUNCS[DECIMAL_INT_MAX];

template<int width>
struct InitDecintBasicFuncArray
{
  template<typename T>
  using Hash = DefHashFunc<DatumHashCalculator<ObDecimalIntType, T>>;

  static void init_array()
  {
    auto &basic_funcs = DECINT_BASIC_FUNCS;
    basic_funcs[width].default_hash_ = Hash<ObDefaultHash>::hash;
    basic_funcs[width].default_hash_batch_= Hash<ObDefaultHash>::hash_batch;
    basic_funcs[width].murmur_hash_ = Hash<ObMurmurHash>::hash;
    basic_funcs[width].murmur_hash_batch_ = Hash<ObMurmurHash>::hash_batch;
    basic_funcs[width].xx_hash_ = Hash<ObXxHash>::hash;
    basic_funcs[width].xx_hash_batch_ = Hash<ObXxHash>::hash_batch;
    basic_funcs[width].wy_hash_ = Hash<ObWyHash>::hash;
    basic_funcs[width].wy_hash_batch_ = Hash<ObWyHash>::hash_batch;
    basic_funcs[width].null_first_cmp_ =
      ObNullSafeDecintCmp<static_cast<ObDecimalIntWideType>(width),
                          static_cast<ObDecimalIntWideType>(width), true>::cmp;
    basic_funcs[width].null_last_cmp_ =
      ObNullSafeDecintCmp<static_cast<ObDecimalIntWideType>(width),
                          static_cast<ObDecimalIntWideType>(width), false>::cmp;
    basic_funcs[width].murmur_hash_v2_ = Hash<ObMurmurHash>::hash_v2;
    basic_funcs[width].murmur_hash_v2_batch_ = Hash<ObMurmurHash>::hash_v2_batch;
  }
};

bool g_decint_basic_func_array_inited =
  ObArrayConstIniter<DECIMAL_INT_MAX, InitDecintBasicFuncArray>::init();
bool g_basic_udt_array_inited = ObArrayConstIniter<1, InitUDTBasicFuncArray>::init();

// for observer 4.0 text tc compare always use ObNullSafeDatumStrCmp
// for observer 4.1 text tc compare use ObNullSafeDatumTextCmp
// Maybe error when cluster version is 4.1 but sqlplan generated in 4.0 ?
ObExprBasicFuncs* ObDatumFuncs::get_basic_func(const ObObjType type,
                                               const ObCollationType cs_type,
                                               const ObScale scale,
                                               const bool is_oracle_mode,
                                               const bool has_lob_locator,
                                               const ObPrecision precision)
{
  ObExprBasicFuncs *res = NULL;
  if ((type >= ObNullType && type < ObMaxType)) {
    if (is_string_type(type)) {
      OB_ASSERT(cs_type > CS_TYPE_INVALID && cs_type < CS_TYPE_MAX);
      bool calc_end_space = is_varying_len_char_type(type, cs_type) && is_oracle_mode;
      if (ob_is_large_text(type)) {
        res = &EXPR_BASIC_STR_FUNCS[cs_type][calc_end_space][has_lob_locator];
      } else {
        // string is always without lob locator
        res = &EXPR_BASIC_STR_FUNCS[cs_type][calc_end_space][false];
      }
    } else if (ob_is_lob_locator(type) || ob_is_roaringbitmap(type)) {
      OB_ASSERT(cs_type > CS_TYPE_INVALID && cs_type < CS_TYPE_MAX);
      bool calc_end_space = false;
      res = &EXPR_BASIC_STR_FUNCS[cs_type][calc_end_space][has_lob_locator];
    } else if (ob_is_json(type)) {
      res = &EXPR_BASIC_JSON_FUNCS[has_lob_locator];
    } else if (ob_is_geometry(type)) {
      res = &EXPR_BASIC_GEO_FUNCS[has_lob_locator];
    } else if (ob_is_user_defined_sql_type(type)) {
      res = &EXPR_BASIC_UDT_FUNCS[0];
    } else if (ob_is_collection_sql_type(type)) {
      res = &EXPR_BASIC_STR_FUNCS[cs_type][false][has_lob_locator];
    } else if (!is_oracle_mode && ob_is_double_type(type) &&
                scale > SCALE_UNKNOWN_YET && scale < OB_NOT_FIXED_SCALE) {
      res = &FIXED_DOUBLE_BASIC_FUNCS[scale];
    } else if (ob_is_decimal_int(type) && precision != PRECISION_UNKNOWN_YET) {
      ObDecimalIntWideType width = get_decimalint_type(precision);
      OB_ASSERT(width >= 0 && width < DECIMAL_INT_MAX);
      res = &DECINT_BASIC_FUNCS[width];
    } else {
      res = &EXPR_BASIC_FUNCS[type];
      // set row cmp funcs
      // FIXME: add precision here
    }
    sql::ObDatumMeta meta(type, cs_type, scale, precision);
    NullSafeRowCmpFunc null_first_cmp = nullptr, null_last_cmp = nullptr;
    // for inner enum, cmp function is null
    VectorCmpExprFuncsHelper::get_cmp_set(meta, meta, null_first_cmp, null_last_cmp);
    res->row_null_first_cmp_ = null_first_cmp;
    res->row_null_last_cmp_ = null_last_cmp;
  } else {
    LOG_WARN_RET(common::OB_INVALID_ARGUMENT, "invalid obj type", K(type));
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

static_assert(CS_TYPE_MAX * 2 * 2 == sizeof(NULLSAFE_TEXT_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_NULLSAFE_TEXT_CMP,
                   NULLSAFE_TEXT_CMP_FUNCS,
                   sizeof(NULLSAFE_TEXT_CMP_FUNCS) / sizeof(void*));

static_assert(CS_TYPE_MAX * 2 * 2 == sizeof(NULLSAFE_TEXT_STR_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_NULLSAFE_TEXT_STR_CMP,
                   NULLSAFE_TEXT_STR_CMP_FUNCS,
                   sizeof(NULLSAFE_TEXT_STR_CMP_FUNCS) / sizeof(void*));

static_assert(CS_TYPE_MAX * 2 * 2 == sizeof(NULLSAFE_STR_TEXT_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_NULLSAFE_STR_TEXT_CMP,
                   NULLSAFE_STR_TEXT_CMP_FUNCS,
                   sizeof(NULLSAFE_STR_TEXT_CMP_FUNCS) / sizeof(void*));

static_assert(2 * 2 == sizeof(NULLSAFE_JSON_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_NULLSAFE_JSON_CMP,
                   NULLSAFE_JSON_CMP_FUNCS,
                   sizeof(NULLSAFE_JSON_CMP_FUNCS) / sizeof(void*));

static_assert(2 * 2 == sizeof(NULLSAFE_GEO_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_NULLSAFE_GEO_CMP,
                   NULLSAFE_GEO_CMP_FUNCS,
                   sizeof(NULLSAFE_GEO_CMP_FUNCS) / sizeof(void*));


static_assert(OB_NOT_FIXED_SCALE * 2 == sizeof(FIXED_DOUBLE_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_FIXED_DOUBLE_NULLSAFE_CMP,
                   FIXED_DOUBLE_CMP_FUNCS,
                   sizeof(FIXED_DOUBLE_CMP_FUNCS) / sizeof(void *));

static_assert(DECIMAL_INT_MAX * DECIMAL_INT_MAX * 2 == sizeof(DECINT_CMP_FUNCS) / sizeof(void *),
              "unexpected size");

REG_SER_FUNC_ARRAY(OB_SFA_DECIMAL_INT_NULLSAFE_CMP, DECINT_CMP_FUNCS,
                   sizeof(DECINT_CMP_FUNCS) / sizeof(void *));

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
    funcs_[4] = reinterpret_cast<void *>(funcs.murmur_hash_v2_);
    funcs_[5] = reinterpret_cast<void *>(funcs.murmur_hash_v2_batch_);
  }
};

static ExprBasicFuncSerPart1 EXPR_BASIC_FUNCS_PART1[ObMaxType];
static ExprBasicFuncSerPart2 EXPR_BASIC_FUNCS_PART2[ObMaxType];
static ExprBasicFuncSerPart1 EXPR_BASIC_STR_FUNCS_PART1[CS_TYPE_MAX][2][2];
static ExprBasicFuncSerPart2 EXPR_BASIC_STR_FUNCS_PART2[CS_TYPE_MAX][2][2];
static ExprBasicFuncSerPart1 EXPR_BASIC_JSON_FUNCS_PART1[2];
static ExprBasicFuncSerPart2 EXPR_BASIC_JSON_FUNCS_PART2[2];
static ExprBasicFuncSerPart1 EXPR_BASIC_GEO_FUNCS_PART1[2];
static ExprBasicFuncSerPart2 EXPR_BASIC_GEO_FUNCS_PART2[2];
static ExprBasicFuncSerPart1 EXPR_BASIC_FIXED_DOUBLE_FUNCS_PART1[OB_NOT_FIXED_SCALE];
static ExprBasicFuncSerPart2 EXPR_BASIC_FIXED_DOUBLE_FUNCS_PART2[OB_NOT_FIXED_SCALE];
static ExprBasicFuncSerPart1 EXPR_BASIC_DECINT_FUNCS_PART1[DECIMAL_INT_MAX];
static ExprBasicFuncSerPart2 EXPR_BASIC_DECINT_FUNCS_PART2[DECIMAL_INT_MAX];

static ExprBasicFuncSerPart1 EXPR_BASIC_UDT_FUNCS_PART1[1];
static ExprBasicFuncSerPart2 EXPR_BASIC_UDT_FUNCS_PART2[1];

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
  for (int64_t i = 0; i < sizeof(EXPR_BASIC_JSON_FUNCS)/sizeof(ObExprBasicFuncs); i++) {
    EXPR_BASIC_JSON_FUNCS_PART1[i].from(EXPR_BASIC_JSON_FUNCS[i]);
    EXPR_BASIC_JSON_FUNCS_PART2[i].from(EXPR_BASIC_JSON_FUNCS[i]);
  }
  for (int64_t i = 0; i < sizeof(EXPR_BASIC_GEO_FUNCS)/sizeof(ObExprBasicFuncs); i++) {
    EXPR_BASIC_GEO_FUNCS_PART1[i].from(EXPR_BASIC_GEO_FUNCS[i]);
    EXPR_BASIC_GEO_FUNCS_PART2[i].from(EXPR_BASIC_GEO_FUNCS[i]);
  }
  for (int64_t i = 0; i < sizeof(FIXED_DOUBLE_BASIC_FUNCS)/sizeof(ObExprBasicFuncs); i++) {
    EXPR_BASIC_FIXED_DOUBLE_FUNCS_PART1[i].from(FIXED_DOUBLE_BASIC_FUNCS[i]);
    EXPR_BASIC_FIXED_DOUBLE_FUNCS_PART2[i].from(FIXED_DOUBLE_BASIC_FUNCS[i]);
  }

  for (int64_t i = 0; i < sizeof(DECINT_BASIC_FUNCS)/sizeof(ObExprBasicFuncs); i++) {
    EXPR_BASIC_DECINT_FUNCS_PART1[i].from(DECINT_BASIC_FUNCS[i]);
    EXPR_BASIC_DECINT_FUNCS_PART2[i].from(DECINT_BASIC_FUNCS[i]);
  }
  for (int64_t i = 0; i < sizeof(EXPR_BASIC_UDT_FUNCS)/sizeof(ObExprBasicFuncs); i++) {
    EXPR_BASIC_UDT_FUNCS_PART1[i].from(EXPR_BASIC_UDT_FUNCS[i]);
    EXPR_BASIC_UDT_FUNCS_PART2[i].from(EXPR_BASIC_UDT_FUNCS[i]);
  }
  return true;
}
bool g_split_basic_func_for_ser = split_basic_func_for_ser();

static const int EXPR_BASIC_FUNC_MEMBER_CNT  = sizeof(ObExprBasicFuncs) / sizeof(void *);

static_assert(ObMaxType * EXPR_BASIC_FUNC_MEMBER_CNT == sizeof(EXPR_BASIC_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_EXPR_BASIC_PART1,
                   EXPR_BASIC_FUNCS_PART1,
                   sizeof(EXPR_BASIC_FUNCS_PART1) / sizeof(void *));

REG_SER_FUNC_ARRAY(OB_SFA_EXPR_BASIC_PART2,
                   EXPR_BASIC_FUNCS_PART2,
                   sizeof(EXPR_BASIC_FUNCS_PART2) / sizeof(void *));

static_assert(CS_TYPE_MAX * 2 * 2 * EXPR_BASIC_FUNC_MEMBER_CNT
                == sizeof(EXPR_BASIC_STR_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_EXPR_STR_BASIC_PART1,
                   EXPR_BASIC_STR_FUNCS_PART1,
                   sizeof(EXPR_BASIC_STR_FUNCS_PART1) / sizeof(void *));
REG_SER_FUNC_ARRAY(OB_SFA_EXPR_STR_BASIC_PART2,
                   EXPR_BASIC_STR_FUNCS_PART2,
                   sizeof(EXPR_BASIC_STR_FUNCS_PART2) / sizeof(void *));

static_assert(2 * EXPR_BASIC_FUNC_MEMBER_CNT == sizeof(EXPR_BASIC_JSON_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_EXPR_JSON_BASIC_PART1,
                   EXPR_BASIC_JSON_FUNCS_PART1,
                   sizeof(EXPR_BASIC_JSON_FUNCS_PART1) / sizeof(void *));
REG_SER_FUNC_ARRAY(OB_SFA_EXPR_JSON_BASIC_PART2,
                   EXPR_BASIC_JSON_FUNCS_PART2,
                   sizeof(EXPR_BASIC_JSON_FUNCS_PART2) / sizeof(void *));

static_assert(2 * EXPR_BASIC_FUNC_MEMBER_CNT == sizeof(EXPR_BASIC_GEO_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_EXPR_GEO_BASIC_PART1,
                   EXPR_BASIC_GEO_FUNCS_PART1,
                   sizeof(EXPR_BASIC_GEO_FUNCS_PART1) / sizeof(void *));
REG_SER_FUNC_ARRAY(OB_SFA_EXPR_GEO_BASIC_PART2,
                   EXPR_BASIC_GEO_FUNCS_PART2,
                   sizeof(EXPR_BASIC_GEO_FUNCS_PART2) / sizeof(void *));

static_assert(OB_NOT_FIXED_SCALE * EXPR_BASIC_FUNC_MEMBER_CNT
                == sizeof(FIXED_DOUBLE_BASIC_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_FIXED_DOUBLE_BASIC_PART1,
                   EXPR_BASIC_FIXED_DOUBLE_FUNCS_PART1,
                   sizeof(EXPR_BASIC_FIXED_DOUBLE_FUNCS_PART1) / sizeof(void *));
REG_SER_FUNC_ARRAY(OB_SFA_FIXED_DOUBLE_BASIC_PART2,
                   EXPR_BASIC_FIXED_DOUBLE_FUNCS_PART2,
                   sizeof(EXPR_BASIC_FIXED_DOUBLE_FUNCS_PART2) / sizeof(void *));


REG_SER_FUNC_ARRAY(OB_SFA_DECIMAL_INT_BASIC_PART1, EXPR_BASIC_DECINT_FUNCS_PART1,
                   sizeof(EXPR_BASIC_DECINT_FUNCS_PART1) / sizeof(void *));

REG_SER_FUNC_ARRAY(OB_SFA_DECIMAL_INT_BASIC_PART2, EXPR_BASIC_DECINT_FUNCS_PART2,
                   sizeof(EXPR_BASIC_DECINT_FUNCS_PART2) / sizeof(void *));

static_assert(1 * EXPR_BASIC_FUNC_MEMBER_CNT == sizeof(EXPR_BASIC_UDT_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_EXPR_UDT_BASIC_PART1,
                   EXPR_BASIC_UDT_FUNCS_PART1,
                   sizeof(EXPR_BASIC_UDT_FUNCS_PART1) / sizeof(void *));
REG_SER_FUNC_ARRAY(OB_SFA_EXPR_UDT_BASIC_PART2,
                   EXPR_BASIC_UDT_FUNCS_PART2,
                   sizeof(EXPR_BASIC_UDT_FUNCS_PART2) / sizeof(void *));

} // end namespace sql
} // end namespace oceanbase
