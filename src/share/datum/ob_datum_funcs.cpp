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
#include "ob_datum_funcs_impl.h"
#include "share/vector/expr_cmp_func.h"

namespace oceanbase {
using namespace sql;
namespace common {

ObDatumCmpFuncType NULLSAFE_TYPE_CMP_FUNCS[ObMaxType][ObMaxType][2];

// bool g_type_cmp_array_inited = Ob2DArrayConstIniter<ObMaxType, ObMaxType, InitTypeCmpArray>::init();

ObDatumCmpFuncType NULLSAFE_TC_CMP_FUNCS[ObMaxTC][ObMaxTC][2];

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

// cs_type, tenant_mode, calc_with_end_space
// now only RawTC, StringTC, TextTC defined str cmp funcs
ObDatumCmpFuncType NULLSAFE_STR_CMP_FUNCS[CS_TYPE_MAX][2][2];
ObDatumCmpFuncType NULLSAFE_TEXT_CMP_FUNCS[CS_TYPE_MAX][2][2];
ObDatumCmpFuncType NULLSAFE_TEXT_STR_CMP_FUNCS[CS_TYPE_MAX][2][2];
ObDatumCmpFuncType NULLSAFE_STR_TEXT_CMP_FUNCS[CS_TYPE_MAX][2][2];

ObDatumCmpFuncType NULLSAFE_JSON_CMP_FUNCS[2][2];


bool g_json_cmp_array_inited = ObArrayConstIniter<1, InitJsonCmpArray>::init();

ObDatumCmpFuncType NULLSAFE_GEO_CMP_FUNCS[2][2];

bool g_geo_cmp_array_inited = ObArrayConstIniter<1, InitGeoCmpArray>::init();

ObDatumCmpFuncType NULLSAFE_COLLECTION_CMP_FUNCS[2][2];

bool g_collection_cmp_array_inited = ObArrayConstIniter<1, InitCollectionCmpArray>::init();

ObDatumCmpFuncType NULLSAFE_ROARINGBITMAP_CMP_FUNCS[2][2];

bool g_roaringbitmap_cmp_array_inited = ObArrayConstIniter<1, InitRoaringbitmapCmpArray>::init();

ObDatumCmpFuncType FIXED_DOUBLE_CMP_FUNCS[OB_NOT_FIXED_SCALE][2];

bool g_fixed_double_cmp_array_inited =
  ObArrayConstIniter<OB_NOT_FIXED_SCALE, InitFixedDoubleCmpArray>::init();

ObDatumCmpFuncType DECINT_CMP_FUNCS[DECIMAL_INT_MAX][DECIMAL_INT_MAX][2];

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
  } else if (is_collection(type1) && is_collection(type2)) {
    func_ptr = NULLSAFE_COLLECTION_CMP_FUNCS[null_pos_idx][has_lob_header];
  } else if (is_roaringbitmap(type1) && is_roaringbitmap(type2)) {
    func_ptr = NULLSAFE_ROARINGBITMAP_CMP_FUNCS[null_pos_idx][has_lob_header];
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

bool ObDatumFuncs::is_collection(const ObObjType type)
{
  const ObObjTypeClass tc = OBJ_TYPE_TO_CLASS[type];
  return (tc == ObCollectionSQLTC);
}


ObExprBasicFuncs EXPR_BASIC_FUNCS[ObMaxType];

// [CS_TYPE][CALC_END_SPACE][IS_LOB_LOCATOR]
ObExprBasicFuncs EXPR_BASIC_STR_FUNCS[CS_TYPE_MAX][2][2];

ObExprBasicFuncs EXPR_BASIC_JSON_FUNCS[2];

ObExprBasicFuncs EXPR_BASIC_GEO_FUNCS[2];
ObExprBasicFuncs EXPR_BASIC_COLLECTION_FUNCS[2];
ObExprBasicFuncs EXPR_BASIC_ROARINGBITMAP_FUNCS[2];

ObExprBasicFuncs FIXED_DOUBLE_BASIC_FUNCS[OB_NOT_FIXED_SCALE];
ObExprBasicFuncs EXPR_BASIC_UDT_FUNCS[1];



bool g_basic_json_array_inited = ObArrayConstIniter<1, InitBasicJsonFuncArray>::init();
bool g_basic_geo_array_inited = ObArrayConstIniter<1, InitBasicGeoFuncArray>::init();
bool g_basic_collection_array_inited = ObArrayConstIniter<1, InitCollectionBasicFuncArray>::init();
bool g_basic_roaringbitmap_array_inited = ObArrayConstIniter<1, InitBasicRoaringbitmapFuncArray>::init();

bool g_fixed_double_basic_func_array_inited =
  ObArrayConstIniter<OB_NOT_FIXED_SCALE, InitFixedDoubleBasicFuncArray>::init();

ObExprBasicFuncs DECINT_BASIC_FUNCS[DECIMAL_INT_MAX];

bool g_decint_basic_func_array_inited =
  ObArrayConstIniter<DECIMAL_INT_MAX, InitDecintBasicFuncArray>::init();
bool g_basic_udt_array_inited = ObArrayConstIniter<1, InitUDTBasicFuncArray>::init();

extern void __init_datum_funcs_all();
extern void __init_all_str_funcs();

static bool init_all_str_funcs()
{
  __init_datum_funcs_all();
  __init_all_str_funcs();
  int64_t g_fill_type_with_tc_cmp_func = fill_type_with_tc_cmp_func();
  return true;
}

bool g_all_str_funcs_intied = init_all_str_funcs();

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
    } else if (ob_is_lob_locator(type)) {
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
      res = &EXPR_BASIC_COLLECTION_FUNCS[has_lob_locator];
    } else if (ob_is_roaringbitmap(type)) {
      res = &EXPR_BASIC_ROARINGBITMAP_FUNCS[has_lob_locator];
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

bool ObDatumFuncs::is_roaringbitmap(const ObObjType type)
{
  const ObObjTypeClass tc = OBJ_TYPE_TO_CLASS[type];
  return (tc == ObRoaringBitmapTC);
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
  return is_string_type(type) || is_json(type) || is_geometry(type) || is_roaringbitmap(type) ||
            (tc == ObUserDefinedSQLTC) || (tc == ObFloatTC) || (tc == ObDoubleTC);
}

OB_SERIALIZE_MEMBER(ObCmpFunc, ser_cmp_func_);
OB_SERIALIZE_MEMBER(ObHashFunc, ser_hash_func_, ser_batch_hash_func_);

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

static_assert(2 * 2 == sizeof(NULLSAFE_COLLECTION_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_NULLSAFE_COLLECTION_CMP,
                   NULLSAFE_COLLECTION_CMP_FUNCS,
                   sizeof(NULLSAFE_COLLECTION_CMP_FUNCS) / sizeof(void*));
static_assert(2 * 2 == sizeof(NULLSAFE_ROARINGBITMAP_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_NULLSAFE_ROARINGBITMAP_CMP,
                   NULLSAFE_ROARINGBITMAP_CMP_FUNCS,
                   sizeof(NULLSAFE_ROARINGBITMAP_CMP_FUNCS) / sizeof(void*));

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
static ExprBasicFuncSerPart1 EXPR_BASIC_COLLECTION_FUNCS_PART1[2];
static ExprBasicFuncSerPart2 EXPR_BASIC_COLLECTION_FUNCS_PART2[2];

static ExprBasicFuncSerPart1 EXPR_BASIC_ROARINGBITMAP_FUNCS_PART1[2];
static ExprBasicFuncSerPart2 EXPR_BASIC_ROARINGBITMAP_FUNCS_PART2[2];
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
  for (int64_t i = 0; i < sizeof(EXPR_BASIC_COLLECTION_FUNCS)/sizeof(ObExprBasicFuncs); i++) {
    EXPR_BASIC_COLLECTION_FUNCS_PART1[i].from(EXPR_BASIC_COLLECTION_FUNCS[i]);
    EXPR_BASIC_COLLECTION_FUNCS_PART2[i].from(EXPR_BASIC_COLLECTION_FUNCS[i]);
  }
  for (int64_t i = 0; i < sizeof(EXPR_BASIC_ROARINGBITMAP_FUNCS)/sizeof(ObExprBasicFuncs); i++) {
    EXPR_BASIC_ROARINGBITMAP_FUNCS_PART1[i].from(EXPR_BASIC_ROARINGBITMAP_FUNCS[i]);
    EXPR_BASIC_ROARINGBITMAP_FUNCS_PART2[i].from(EXPR_BASIC_ROARINGBITMAP_FUNCS[i]);
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

static_assert(2 * EXPR_BASIC_FUNC_MEMBER_CNT == sizeof(EXPR_BASIC_COLLECTION_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_EXPR_COLLECTION_BASIC_PART1,
                   EXPR_BASIC_COLLECTION_FUNCS_PART1,
                   sizeof(EXPR_BASIC_COLLECTION_FUNCS_PART1) / sizeof(void *));
REG_SER_FUNC_ARRAY(OB_SFA_EXPR_COLLECTION_BASIC_PART2,
                   EXPR_BASIC_COLLECTION_FUNCS_PART2,
                   sizeof(EXPR_BASIC_COLLECTION_FUNCS_PART2) / sizeof(void *));
static_assert(2 * EXPR_BASIC_FUNC_MEMBER_CNT == sizeof(EXPR_BASIC_ROARINGBITMAP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_EXPR_ROARINGBITMAP_BASIC_PART1,
                   EXPR_BASIC_ROARINGBITMAP_FUNCS_PART1,
                   sizeof(EXPR_BASIC_ROARINGBITMAP_FUNCS_PART1) / sizeof(void *));
REG_SER_FUNC_ARRAY(OB_SFA_EXPR_ROARINGBITMAP_BASIC_PART2,
                   EXPR_BASIC_ROARINGBITMAP_FUNCS_PART2,
                   sizeof(EXPR_BASIC_ROARINGBITMAP_FUNCS_PART2) / sizeof(void *));

} // end namespace sql
} // end namespace oceanbase
