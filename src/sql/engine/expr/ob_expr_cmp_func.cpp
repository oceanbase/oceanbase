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

#include "ob_expr_cmp_func.ipp"

namespace oceanbase
{
namespace sql
{
using namespace common;


ObExpr::EvalBatchFunc EVAL_BATCH_NULL_EXTEND_CMP_FUNCS[CO_MAX];
ObExpr::EvalBatchFunc EVAL_BATCH_STR_CMP_FUNCS[CO_MAX];
ObExpr::EvalBatchFunc EVAL_BATCH_TEXT_CMP_FUNCS[CO_MAX];
ObExpr::EvalBatchFunc EVAL_BATCH_TEXT_STR_CMP_FUNCS[CO_MAX];
ObExpr::EvalBatchFunc EVAL_BATCH_STR_TEXT_CMP_FUNCS[CO_MAX];
ObExpr::EvalBatchFunc EVAL_BATCH_JSON_CMP_FUNCS[CO_MAX];
ObExpr::EvalBatchFunc EVAL_BATCH_GEO_CMP_FUNCS[CO_MAX];
ObExpr::EvalBatchFunc EVAL_BATCH_COLLECTION_CMP_FUNCS[CO_MAX];

ObExpr::EvalFunc EVAL_TYPE_CMP_FUNCS[ObMaxType][ObMaxType][CO_MAX];
ObExpr::EvalBatchFunc EVAL_BATCH_TYPE_CMP_FUNCS[ObMaxType][ObMaxType][CO_MAX];
ObDatumCmpFuncType DATUM_TYPE_CMP_FUNCS[ObMaxType][ObMaxType];

// TODO serialize
ObExpr::EvalFunc EVAL_TC_CMP_FUNCS[ObMaxTC][ObMaxTC][CO_MAX];
ObExpr::EvalBatchFunc EVAL_BATCH_TC_CMP_FUNCS[ObMaxTC][ObMaxTC][CO_MAX];
ObDatumCmpFuncType DATUM_TC_CMP_FUNCS[ObMaxTC][ObMaxTC];

ObExpr::EvalFunc EVAL_STR_CMP_FUNCS[CS_TYPE_MAX][CO_MAX][2];
ObDatumCmpFuncType DATUM_STR_CMP_FUNCS[CS_TYPE_MAX][2];
ObExpr::EvalFunc EVAL_TEXT_CMP_FUNCS[CS_TYPE_MAX][CO_MAX][2];
ObDatumCmpFuncType DATUM_TEXT_CMP_FUNCS[CS_TYPE_MAX][2];
ObExpr::EvalFunc EVAL_TEXT_STR_CMP_FUNCS[CS_TYPE_MAX][CO_MAX][2];
ObDatumCmpFuncType DATUM_TEXT_STR_CMP_FUNCS[CS_TYPE_MAX][2];
ObExpr::EvalFunc EVAL_STR_TEXT_CMP_FUNCS[CS_TYPE_MAX][CO_MAX][2];
ObDatumCmpFuncType DATUM_STR_TEXT_CMP_FUNCS[CS_TYPE_MAX][2];
ObExpr::EvalFunc EVAL_JSON_CMP_FUNCS[CO_MAX][2];
ObDatumCmpFuncType DATUM_JSON_CMP_FUNCS[2];
ObExpr::EvalFunc EVAL_GEO_CMP_FUNCS[CO_MAX][2];
ObDatumCmpFuncType DATUM_GEO_CMP_FUNCS[2];
ObExpr::EvalFunc EVAL_COLLECTION_CMP_FUNCS[CO_MAX][2];
ObDatumCmpFuncType DATUM_COLLECTION_CMP_FUNCS[2];

ObExpr::EvalFunc EVAL_FIXED_DOUBLE_CMP_FUNCS[OB_NOT_FIXED_SCALE][CO_MAX];
ObExpr::EvalBatchFunc EVAL_BATCH_FIXED_DOUBLE_CMP_FUNCS[OB_NOT_FIXED_SCALE][CO_MAX];
ObDatumCmpFuncType DATUM_FIXED_DOUBLE_CMP_FUNCS[OB_NOT_FIXED_SCALE];

ObExpr::EvalFunc EVAL_DECINT_CMP_FUNCS[DECIMAL_INT_MAX][DECIMAL_INT_MAX][CO_MAX];
ObExpr::EvalBatchFunc EVAL_BATCH_DECINT_CMP_FUNCS[DECIMAL_INT_MAX][DECIMAL_INT_MAX][CO_MAX];

ObDatumCmpFuncType DATUM_DECINT_CMP_FUNCS[DECIMAL_INT_MAX][DECIMAL_INT_MAX];

ObExpr::EvalFunc EVAL_VEC_CMP_FUNCS[CO_MAX];
ObExpr::EvalBatchFunc EVAL_BATCH_VEC_CMP_FUNCS[CO_MAX];

// int g_init_type_ret = Ob2DArrayConstIniter<ObMaxType, ObMaxType, TypeExprCmpFuncIniter>::init();
// int g_init_tc_ret = Ob2DArrayConstIniter<ObMaxTC, ObMaxTC, TCExprCmpFuncIniter>::init();
// int g_init_str_ret = Ob2DArrayConstIniter<CS_TYPE_MAX, CO_MAX, StrExprFuncIniter>::init();
// int g_init_datum_str_ret = ObArrayConstIniter<CS_TYPE_MAX, DatumStrExprCmpIniter>::init();
// int g_init_text_ret = Ob2DArrayConstIniter<CS_TYPE_MAX, CO_MAX, TextExprFuncIniter>::init();
// int g_init_datum_text_ret = ObArrayConstIniter<CS_TYPE_MAX, DatumTextExprCmpIniter>::init();
// int g_init_text_str_ret = Ob2DArrayConstIniter<CS_TYPE_MAX, CO_MAX, TextStrExprFuncIniter>::init();
// int g_init_datum_text_str_ret = ObArrayConstIniter<CS_TYPE_MAX, DatumTextStrExprCmpIniter>::init();
// int g_init_str_text_ret = Ob2DArrayConstIniter<CS_TYPE_MAX, CO_MAX, StrTextExprFuncIniter>::init();
// int g_init_str_datum_text_ret = ObArrayConstIniter<CS_TYPE_MAX, DatumStrTextExprCmpIniter>::init();

static int64_t fill_type_with_tc_eval_func(void)
{
  int64_t cnt = 0;
  for (int64_t i = 0; i < ObMaxType; i++) {
    ObObjTypeClass i_tc = ob_obj_type_class((ObObjType)i);
    for (int64_t j = 0; j < ObMaxType; j++) {
      ObObjTypeClass j_tc = ob_obj_type_class((ObObjType)j);
      if (NULL == EVAL_TYPE_CMP_FUNCS[i][j][0]) {
        const int64_t size = sizeof(void *) * CO_MAX;
        memcpy(&EVAL_TYPE_CMP_FUNCS[i][j][0],
               &EVAL_TC_CMP_FUNCS[i_tc][j_tc][0],
               size);
        memcpy(&EVAL_BATCH_TYPE_CMP_FUNCS[i][j][0],
               &EVAL_BATCH_TC_CMP_FUNCS[i_tc][j_tc][0],
               size);
        cnt++;
      }
      if (NULL == DATUM_TYPE_CMP_FUNCS[i][j]) {
        DATUM_TYPE_CMP_FUNCS[i][j] = DATUM_TC_CMP_FUNCS[i_tc][j_tc];
        cnt++;
      }
    }
  }
  return cnt;
}

extern void __init_all_expr_cmp_funcs();
extern void __init_all_str_expr_cmp_func();

static int64_t init_all_funcs()
{
  int g_init_extra_expr_ret = ObArrayConstIniter<CO_MAX, ExtraExprCmpIniter>::init();
  __init_all_expr_cmp_funcs();
  __init_all_str_expr_cmp_func();
  int g_init_json_ret = ObArrayConstIniter<CO_MAX, JsonExprFuncIniter>::init();
  int g_init_json_datum_ret = ObArrayConstIniter<1, DatumJsonExprCmpIniter>::init();
  int g_init_geo_ret = ObArrayConstIniter<CO_MAX, GeoExprFuncIniter>::init();
  int g_init_geo_datum_ret = ObArrayConstIniter<1, DatumGeoExprCmpIniter>::init();

  int g_init_collection_ret = ObArrayConstIniter<CO_MAX, CollectionExprFuncIniter>::init();
  int g_init_collection_datum_ret = ObArrayConstIniter<1, DatumCollectionExprCmpIniter>::init();
  int g_init_fixed_double_ret =
    ObArrayConstIniter<OB_NOT_FIXED_SCALE, FixedDoubleCmpFuncIniter>::init();

  int g_init_decint_cmp_ret =
    Ob2DArrayConstIniter<DECIMAL_INT_MAX, DECIMAL_INT_MAX, DecintCmpFuncIniter>::init();

  return fill_type_with_tc_eval_func();
}

int64_t g_init_all_funcs = init_all_funcs();

ObExpr::EvalFunc ObExprCmpFuncsHelper::get_eval_expr_cmp_func(const ObObjType type1,
                                                              const ObObjType type2,
                                                              const ObScale scale1,
                                                              const ObScale scale2,
                                                              const ObPrecision prec1,
                                                              const ObPrecision prec2,
                                                              const ObCmpOp cmp_op,
                                                              const bool is_oracle_mode,
                                                              const ObCollationType cs_type,
                                                              const bool has_lob_header)
{
  OB_ASSERT(type1 >= ObNullType && type1 < ObMaxType);
  OB_ASSERT(type2 >= ObNullType && type2 < ObMaxType);
  OB_ASSERT(cmp_op >= CO_EQ && cmp_op <= CO_MAX);

  ObObjTypeClass tc1 = ob_obj_type_class(type1);
  ObObjTypeClass tc2 = ob_obj_type_class(type2);
  ObExpr::EvalFunc func_ptr = NULL;
  if (OB_UNLIKELY(ob_is_invalid_cmp_op(cmp_op)) ||
      OB_UNLIKELY(ob_is_invalid_obj_tc(tc1) ||
      OB_UNLIKELY(ob_is_invalid_obj_tc(tc2)))) {
    func_ptr = NULL;
  } else if (tc1 == ObJsonTC && tc2 == ObJsonTC) {
    func_ptr = EVAL_JSON_CMP_FUNCS[cmp_op][has_lob_header];
  } else if (tc1 == ObGeometryTC && tc2 == ObGeometryTC) {
    func_ptr = EVAL_GEO_CMP_FUNCS[cmp_op][has_lob_header];
  } else if (tc1 == ObCollectionSQLTC && tc2 == ObCollectionSQLTC) {
    func_ptr = EVAL_COLLECTION_CMP_FUNCS[cmp_op][has_lob_header];
  } else if (IS_FIXED_DOUBLE) {
    func_ptr = EVAL_FIXED_DOUBLE_CMP_FUNCS[MAX(scale1, scale2)][cmp_op];
  } else if (tc1 == ObDecimalIntTC && tc2 == ObDecimalIntTC) {
    ObDecimalIntWideType lw = get_decimalint_type(prec1);
    ObDecimalIntWideType rw = get_decimalint_type(prec2);
    OB_ASSERT(lw < DECIMAL_INT_MAX && lw >= 0);
    OB_ASSERT(rw < DECIMAL_INT_MAX && rw >= 0);
    func_ptr = EVAL_DECINT_CMP_FUNCS[lw][rw][cmp_op];
  } else if (tc1 == ObCollectionSQLTC && tc2 == ObCollectionSQLTC) {
    func_ptr = EVAL_VEC_CMP_FUNCS[cmp_op];
  } else if (tc1 == ObUserDefinedSQLTC || tc2 == ObUserDefinedSQLTC) {
    func_ptr = NULL; //?
  } else if (!ObDatumFuncs::is_string_type(type1) || !ObDatumFuncs::is_string_type(type2)) {
    func_ptr = EVAL_TYPE_CMP_FUNCS[type1][type2][cmp_op];
  } else {
    OB_ASSERT(cs_type > CS_TYPE_INVALID && cs_type < CS_TYPE_MAX);
    int64_t calc_with_end_space_idx = (is_calc_with_end_space(type1, type2, is_oracle_mode,
                                                              cs_type, cs_type) ? 1 : 0);
    if (has_lob_header && (ob_is_large_text(type1) || ob_is_large_text(type2))) {
      if (ob_is_large_text(type1) && ob_is_large_text(type2)) {
        func_ptr = EVAL_TEXT_CMP_FUNCS[cs_type][cmp_op][calc_with_end_space_idx];
      } else if (ob_is_large_text(type1)) { // type2 not large text
        func_ptr = EVAL_TEXT_STR_CMP_FUNCS[cs_type][cmp_op][calc_with_end_space_idx];
      } else { // type1 not large text
        func_ptr = EVAL_STR_TEXT_CMP_FUNCS[cs_type][cmp_op][calc_with_end_space_idx];
      }
    } else { // no lob header or tinytext use original str cmp func
      func_ptr = EVAL_STR_CMP_FUNCS[cs_type][cmp_op][calc_with_end_space_idx];
    }
  }
  return func_ptr;
}

ObExpr::EvalBatchFunc ObExprCmpFuncsHelper::get_eval_batch_expr_cmp_func(
    const ObObjType type1,
    const ObObjType type2,
    const ObScale scale1,
    const ObScale scale2,
    const ObPrecision prec1,
    const ObPrecision prec2,
    const ObCmpOp cmp_op,
    const bool is_oracle_mode,
    const ObCollationType cs_type,
    const bool has_lob_header)
{
  OB_ASSERT(type1 >= ObNullType && type1 < ObMaxType);
  OB_ASSERT(type2 >= ObNullType && type2 < ObMaxType);
  OB_ASSERT(cmp_op >= CO_EQ && cmp_op <= CO_MAX);

  ObObjTypeClass tc1 = ob_obj_type_class(type1);
  ObObjTypeClass tc2 = ob_obj_type_class(type2);
  ObExpr::EvalBatchFunc func_ptr = NULL;
  if (OB_UNLIKELY(ob_is_invalid_cmp_op(cmp_op)) ||
      OB_UNLIKELY(ob_is_invalid_obj_tc(tc1) ||
      OB_UNLIKELY(ob_is_invalid_obj_tc(tc2)))) {
    func_ptr = NULL;
  } else if (type1 == ObJsonType && type2 == ObJsonType) {
    if (NULL != EVAL_JSON_CMP_FUNCS[cmp_op][has_lob_header]) {
      func_ptr = EVAL_BATCH_JSON_CMP_FUNCS[cmp_op];
    }
  } else if (tc1 == ObGeometryTC && tc2 == ObGeometryTC) {
    if (NULL != EVAL_GEO_CMP_FUNCS[cmp_op][has_lob_header]) {
      func_ptr = EVAL_BATCH_GEO_CMP_FUNCS[cmp_op];
    }
  } else if (tc1 == ObCollectionSQLTC && tc2 == ObCollectionSQLTC) {
    func_ptr = EVAL_BATCH_COLLECTION_CMP_FUNCS[cmp_op];
  } else if (IS_FIXED_DOUBLE) {
    func_ptr = EVAL_BATCH_FIXED_DOUBLE_CMP_FUNCS[MAX(scale1, scale2)][cmp_op];
  } else if (ob_is_decimal_int(type1) && ob_is_decimal_int(type2)) {
    ObDecimalIntWideType lw = get_decimalint_type(prec1);
    ObDecimalIntWideType rw = get_decimalint_type(prec2);
    OB_ASSERT(lw < DECIMAL_INT_MAX && lw >= 0);
    OB_ASSERT(rw < DECIMAL_INT_MAX && rw >= 0);
    func_ptr = EVAL_BATCH_DECINT_CMP_FUNCS[lw][rw][cmp_op];
  } else if (tc1 == ObUserDefinedSQLTC || tc2 == ObUserDefinedSQLTC) {
    func_ptr = NULL; //?
  } else if (!ObDatumFuncs::is_string_type(type1) || !ObDatumFuncs::is_string_type(type2)) {
    func_ptr = EVAL_BATCH_TYPE_CMP_FUNCS[type1][type2][cmp_op];
  } else {
    OB_ASSERT(cs_type > CS_TYPE_INVALID && cs_type < CS_TYPE_MAX);
    int64_t calc_with_end_space_idx = (is_calc_with_end_space(type1, type2, is_oracle_mode,
                                                              cs_type, cs_type) ? 1 : 0);
    if (has_lob_header && (ob_is_large_text(type1) || ob_is_large_text(type2))) {
      if (ob_is_large_text(type1) && ob_is_large_text(type2)) {
        if (NULL != EVAL_TEXT_CMP_FUNCS[cs_type][cmp_op][calc_with_end_space_idx]) {
          func_ptr = EVAL_BATCH_TEXT_CMP_FUNCS[cmp_op];
        }
      } else if (ob_is_large_text(type1)) { // type2 not large text
        if (NULL != EVAL_TEXT_STR_CMP_FUNCS[cs_type][cmp_op][calc_with_end_space_idx]) {
          func_ptr = EVAL_BATCH_TEXT_STR_CMP_FUNCS[cmp_op];
        }
      } else { // type1 not large text
        if (NULL != EVAL_STR_TEXT_CMP_FUNCS[cs_type][cmp_op][calc_with_end_space_idx]) {
          func_ptr = EVAL_BATCH_STR_TEXT_CMP_FUNCS[cmp_op];
        }
      }
    } else { // no lob header or tinytext use original str cmp func
      if (NULL != EVAL_STR_CMP_FUNCS[cs_type][cmp_op][calc_with_end_space_idx]) {
        func_ptr = EVAL_BATCH_STR_CMP_FUNCS[cmp_op];
      }
    }
  }
  return func_ptr;
}

DatumCmpFunc ObExprCmpFuncsHelper::get_datum_expr_cmp_func(const ObObjType type1,
                                           const ObObjType type2,
                                           const ObScale scale1,
                                           const ObScale scale2,
                                           const ObPrecision prec1,
                                           const ObPrecision prec2,
                                           const bool is_oracle_mode,
                                           const ObCollationType cs_type,
                                           const bool has_lob_header)
{
  OB_ASSERT(type1 >= ObNullType && type1 < ObMaxType);
  OB_ASSERT(type2 >= ObNullType && type2 < ObMaxType);

  ObObjTypeClass tc1 = ob_obj_type_class(type1);
  ObObjTypeClass tc2 = ob_obj_type_class(type2);
  ObDatumCmpFuncType func_ptr = NULL;
  if (type1 == ObJsonType && type2 == ObJsonType) {
    func_ptr = DATUM_JSON_CMP_FUNCS[has_lob_header];
  } else if (type1 == ObGeometryType && type2 == ObGeometryType) {
    func_ptr = DATUM_GEO_CMP_FUNCS[has_lob_header];
  } else if (type1 == ObCollectionSQLType && type2 == ObCollectionSQLType) {
    func_ptr = DATUM_COLLECTION_CMP_FUNCS[has_lob_header];
  } else if (IS_FIXED_DOUBLE) {
    func_ptr = DATUM_FIXED_DOUBLE_CMP_FUNCS[MAX(scale1, scale2)];
  } else if (ob_is_decimal_int(type1) && ob_is_decimal_int(type2)) {
    ObDecimalIntWideType lw = get_decimalint_type(prec1);
    ObDecimalIntWideType rw = get_decimalint_type(prec2);
    OB_ASSERT(lw < DECIMAL_INT_MAX && lw >= 0);
    OB_ASSERT(rw < DECIMAL_INT_MAX && rw >= 0);
    func_ptr = DATUM_DECINT_CMP_FUNCS[lw][rw];
  } else if (tc1 == ObUserDefinedSQLTC || tc2 == ObUserDefinedSQLTC) {
    func_ptr = NULL; //?
  } else if (!ObDatumFuncs::is_string_type(type1) || !ObDatumFuncs::is_string_type(type2)) {
    func_ptr = DATUM_TYPE_CMP_FUNCS[type1][type2];
    if (NULL == func_ptr) {
      func_ptr = DATUM_TC_CMP_FUNCS[tc1][tc2];
    }
  } else {
    OB_ASSERT(cs_type > CS_TYPE_INVALID && cs_type < CS_TYPE_MAX);
    int64_t calc_with_end_space_idx = (is_calc_with_end_space(type1, type2, is_oracle_mode,
                                                              cs_type, cs_type) ? 1 : 0);
    if (has_lob_header && (ob_is_large_text(type1) || ob_is_large_text(type2))) {
      if (ob_is_large_text(type1) && ob_is_large_text(type2)) {
        func_ptr = DATUM_TEXT_CMP_FUNCS[cs_type][calc_with_end_space_idx];
      } else if (ob_is_large_text(type1)) { // type2 not large text
        func_ptr = DATUM_TEXT_STR_CMP_FUNCS[cs_type][calc_with_end_space_idx];
      } else { // type1 not large text
        func_ptr = DATUM_STR_TEXT_CMP_FUNCS[cs_type][calc_with_end_space_idx];
      }
    } else { // no lob header or tinytext use original str cmp func
      func_ptr = DATUM_STR_CMP_FUNCS[cs_type][calc_with_end_space_idx];
    }
  }
  return func_ptr;
}

// register function serialization
// Need too convert two dimension arrayto index stable one dimension array first.

// register type * type evaluate functions
static_assert(7 == CO_MAX, "unexpected size");
void *g_ser_eval_type_cmp_funcs[ObMaxType * ObMaxType * 7];
static_assert(sizeof(g_ser_eval_type_cmp_funcs) == sizeof(EVAL_TYPE_CMP_FUNCS),
              "unexpected size");
bool g_ser_eval_type_cmp_funcs_init = ObFuncSerialization::convert_NxN_array(
    g_ser_eval_type_cmp_funcs,
    reinterpret_cast<void **>(EVAL_TYPE_CMP_FUNCS),
    ObMaxType,
    7,
    0,
    7);
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_EVAL,
                   g_ser_eval_type_cmp_funcs,
                   sizeof(g_ser_eval_type_cmp_funcs) / sizeof(void *));


static_assert(7 == CO_MAX, "unexpected size");
void *g_ser_eval_batch_type_cmp_funcs[ObMaxType * ObMaxType * 7];
static_assert(sizeof(g_ser_eval_batch_type_cmp_funcs) == sizeof(EVAL_BATCH_TYPE_CMP_FUNCS),
              "unexpected size");
bool g_ser_eval_batch_type_cmp_funcs_init = ObFuncSerialization::convert_NxN_array(
    g_ser_eval_batch_type_cmp_funcs,
    reinterpret_cast<void **>(EVAL_BATCH_TYPE_CMP_FUNCS),
    ObMaxType,
    7,
    0,
    7);
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_EVAL_BATCH,
                   g_ser_eval_batch_type_cmp_funcs,
                   sizeof(g_ser_eval_batch_type_cmp_funcs) / sizeof(void *));

void *g_ser_datum_type_cmp_funcs[ObMaxType * ObMaxType];
static_assert(sizeof(g_ser_datum_type_cmp_funcs) == sizeof(DATUM_TYPE_CMP_FUNCS),
              "unexpected size");
bool g_ser_datum_cmp_funcs_init = ObFuncSerialization::convert_NxN_array(
    g_ser_datum_type_cmp_funcs,
    reinterpret_cast<void **>(DATUM_TYPE_CMP_FUNCS),
    ObMaxType);
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_CMP,
                   g_ser_datum_type_cmp_funcs,
                   sizeof(g_ser_datum_type_cmp_funcs) / sizeof(void *));


static_assert(7 == CO_MAX && CS_TYPE_MAX * 7 * 2 == sizeof(EVAL_STR_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_EVAL_STR,
                   EVAL_STR_CMP_FUNCS,
                   sizeof(EVAL_STR_CMP_FUNCS) / sizeof(void *));

REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_STR_EVAL_BATCH,
                   EVAL_BATCH_STR_CMP_FUNCS,
                   sizeof(EVAL_BATCH_STR_CMP_FUNCS) / sizeof(void *));

static_assert(CS_TYPE_MAX * 2 == sizeof(DATUM_STR_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_CMP_STR,
                   DATUM_STR_CMP_FUNCS,
                   sizeof(DATUM_STR_CMP_FUNCS) / sizeof(void *));

static_assert(7 == CO_MAX && CS_TYPE_MAX * 7 * 2 == sizeof(EVAL_TEXT_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_EVAL_TEXT,
                   EVAL_TEXT_CMP_FUNCS,
                   sizeof(EVAL_TEXT_CMP_FUNCS) / sizeof(void *));

REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_TEXT_EVAL_BATCH,
                   EVAL_BATCH_TEXT_CMP_FUNCS,
                   sizeof(EVAL_BATCH_TEXT_CMP_FUNCS) / sizeof(void *));

static_assert(CS_TYPE_MAX * 2 == sizeof(DATUM_TEXT_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_CMP_TEXT,
                   DATUM_TEXT_CMP_FUNCS,
                   sizeof(DATUM_TEXT_CMP_FUNCS) / sizeof(void *));

static_assert(7 == CO_MAX && CS_TYPE_MAX * 7 * 2 == sizeof(EVAL_TEXT_STR_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_EVAL_TEXT_STR,
                   EVAL_TEXT_STR_CMP_FUNCS,
                   sizeof(EVAL_TEXT_STR_CMP_FUNCS) / sizeof(void *));

REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_TEXT_STR_EVAL_BATCH,
                   EVAL_BATCH_TEXT_STR_CMP_FUNCS,
                   sizeof(EVAL_BATCH_TEXT_STR_CMP_FUNCS) / sizeof(void *));

static_assert(CS_TYPE_MAX * 2 == sizeof(DATUM_TEXT_STR_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_CMP_TEXT_STR,
                   DATUM_TEXT_STR_CMP_FUNCS,
                   sizeof(DATUM_TEXT_STR_CMP_FUNCS) / sizeof(void *));

static_assert(7 == CO_MAX && CS_TYPE_MAX * 7 * 2 == sizeof(EVAL_STR_TEXT_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_EVAL_STR_TEXT,
                   EVAL_STR_TEXT_CMP_FUNCS,
                   sizeof(EVAL_STR_TEXT_CMP_FUNCS) / sizeof(void *));

REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_STR_TEXT_EVAL_BATCH,
                   EVAL_BATCH_STR_TEXT_CMP_FUNCS,
                   sizeof(EVAL_BATCH_STR_TEXT_CMP_FUNCS) / sizeof(void *));

static_assert(CS_TYPE_MAX * 2 == sizeof(DATUM_STR_TEXT_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_CMP_STR_TEXT,
                   DATUM_STR_TEXT_CMP_FUNCS,
                   sizeof(DATUM_STR_TEXT_CMP_FUNCS) / sizeof(void *));

static_assert(7 == CO_MAX && CO_MAX * 2 == sizeof(EVAL_JSON_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_EVAL_JSON,
                   EVAL_JSON_CMP_FUNCS,
                   sizeof(EVAL_JSON_CMP_FUNCS) / sizeof(void *));

static_assert(7 == CO_MAX && CO_MAX == sizeof(EVAL_BATCH_JSON_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_JSON_EVAL_BATCH,
                   EVAL_BATCH_JSON_CMP_FUNCS,
                   sizeof(EVAL_BATCH_JSON_CMP_FUNCS) / sizeof(void *));

static_assert(2 == sizeof(DATUM_JSON_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_CMP_JSON,
                   DATUM_JSON_CMP_FUNCS,
                   sizeof(DATUM_JSON_CMP_FUNCS) / sizeof(void *));

// Geo cmp functions reg
static_assert(7 == CO_MAX && CO_MAX * 2 == sizeof(EVAL_GEO_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_EVAL_GEO,
                   EVAL_GEO_CMP_FUNCS,
                   sizeof(EVAL_GEO_CMP_FUNCS) / sizeof(void *));

static_assert(7 == CO_MAX && CO_MAX == sizeof(EVAL_BATCH_GEO_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_GEO_EVAL_BATCH,
                   EVAL_BATCH_GEO_CMP_FUNCS,
                   sizeof(EVAL_BATCH_GEO_CMP_FUNCS) / sizeof(void *));

static_assert(2 == sizeof(DATUM_GEO_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_CMP_GEO,
                   DATUM_GEO_CMP_FUNCS,
                   sizeof(DATUM_GEO_CMP_FUNCS) / sizeof(void *));

// Collection cmp functions reg
static_assert(7 == CO_MAX && CO_MAX * 2 == sizeof(EVAL_COLLECTION_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_COLLECTION_EVAL,
                   EVAL_COLLECTION_CMP_FUNCS,
                   sizeof(EVAL_COLLECTION_CMP_FUNCS) / sizeof(void *));

static_assert(7 == CO_MAX && CO_MAX == sizeof(EVAL_BATCH_COLLECTION_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_RELATION_EXPR_COLLECTION_EVAL_BATCH,
                   EVAL_BATCH_COLLECTION_CMP_FUNCS,
                   sizeof(EVAL_BATCH_COLLECTION_CMP_FUNCS) / sizeof(void *));

static_assert(2 == sizeof(DATUM_COLLECTION_CMP_FUNCS) / sizeof(void *),
              "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_CMP_COLLECTION,
                   DATUM_COLLECTION_CMP_FUNCS,
                   sizeof(DATUM_COLLECTION_CMP_FUNCS) / sizeof(void *));

// Fixed double cmp functions reg
static_assert(
  OB_NOT_FIXED_SCALE * CO_MAX == sizeof(EVAL_FIXED_DOUBLE_CMP_FUNCS) / sizeof(void *),
  "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_FIXED_DOUBLE_CMP_EVAL,
                   EVAL_FIXED_DOUBLE_CMP_FUNCS,
                   sizeof(EVAL_FIXED_DOUBLE_CMP_FUNCS) / sizeof(void *));

static_assert(
  OB_NOT_FIXED_SCALE * CO_MAX == sizeof(EVAL_BATCH_FIXED_DOUBLE_CMP_FUNCS) / sizeof(void *),
  "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_FIXED_DOUBLE_CMP_EVAL_BATCH,
                   EVAL_BATCH_FIXED_DOUBLE_CMP_FUNCS,
                   sizeof(EVAL_BATCH_FIXED_DOUBLE_CMP_FUNCS) / sizeof(void *));

static_assert(
  OB_NOT_FIXED_SCALE == sizeof(DATUM_FIXED_DOUBLE_CMP_FUNCS) / sizeof(void *),
  "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_DATUM_FIXED_DOUBLE_CMP,
                   DATUM_FIXED_DOUBLE_CMP_FUNCS,
                   sizeof(DATUM_FIXED_DOUBLE_CMP_FUNCS) / sizeof(void *));

REG_SER_FUNC_ARRAY(OB_SFA_DECIMAL_INT_CMP_EVAL, EVAL_DECINT_CMP_FUNCS,
                   sizeof(EVAL_DECINT_CMP_FUNCS) / sizeof(void *));

REG_SER_FUNC_ARRAY(OB_SFA_DECIMAL_INT_CMP_EVAL_BATCH, EVAL_BATCH_DECINT_CMP_FUNCS,
                   sizeof(EVAL_BATCH_DECINT_CMP_FUNCS) / sizeof(void *))

REG_SER_FUNC_ARRAY(OB_SFA_DECIMAL_INT_CMP, DATUM_DECINT_CMP_FUNCS,
                   sizeof(DATUM_DECINT_CMP_FUNCS) / sizeof(void *));

} // end namespace sql;
} // end namespace oceanbase
