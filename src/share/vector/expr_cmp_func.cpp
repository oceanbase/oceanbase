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

#include "expr_cmp_func.h"

#define NULL_FIRST_IDX 0
#define NULL_LAST_IDX  1

namespace oceanbase
{
namespace common
{
using namespace sql;

NullSafeRowCmpFunc NULLSAFE_ROW_CMP_FUNCS[MAX_VEC_TC][MAX_VEC_TC][2];
RowCmpFunc ROW_CMP_FUNCS[MAX_VEC_TC][MAX_VEC_TC];
sql::ObExpr::EvalVectorFunc EVAL_VECTOR_EXPR_CMP_FUNCS[MAX_VEC_TC][MAX_VEC_TC][CO_MAX];

void VectorCmpExprFuncsHelper::get_cmp_set(const sql::ObDatumMeta &l_meta,
                                           const sql::ObDatumMeta &r_meta,
                                           sql::NullSafeRowCmpFunc &null_first_cmp,
                                           sql::NullSafeRowCmpFunc &null_last_cmp)
{
  VecValueTypeClass l_tc = get_vec_value_tc(l_meta.type_, l_meta.scale_, l_meta.precision_);
  VecValueTypeClass r_tc = get_vec_value_tc(r_meta.type_, r_meta.scale_, r_meta.precision_);
  null_first_cmp = NULLSAFE_ROW_CMP_FUNCS[l_tc][r_tc][NULL_FIRST_IDX];
  null_last_cmp = NULLSAFE_ROW_CMP_FUNCS[l_tc][r_tc][NULL_LAST_IDX];
}

RowCmpFunc VectorCmpExprFuncsHelper::get_row_cmp_func(const sql::ObDatumMeta &l_meta,
                                                      const sql::ObDatumMeta &r_meta)
{
  VecValueTypeClass l_tc = get_vec_value_tc(l_meta.type_, l_meta.scale_, l_meta.precision_);
  VecValueTypeClass r_tc = get_vec_value_tc(r_meta.type_, r_meta.scale_, r_meta.precision_);
  return ROW_CMP_FUNCS[l_tc][r_tc];
}

extern void __expr_cmp_func_compilation0();
extern void __expr_cmp_func_compilation1();
extern void __expr_cmp_func_compilation2();
extern void __expr_cmp_func_compilation3();
extern void __expr_cmp_func_compilation4();
extern void __expr_cmp_func_compilation5();
extern void __expr_cmp_func_compilation6();
extern void __expr_cmp_func_compilation7();

static bool init_all_expr_cmp_funcs()
{
  __expr_cmp_func_compilation0();
  __expr_cmp_func_compilation1();
  __expr_cmp_func_compilation2();
  __expr_cmp_func_compilation3();
  __expr_cmp_func_compilation4();
  __expr_cmp_func_compilation5();
  __expr_cmp_func_compilation6();
  __expr_cmp_func_compilation7();
  return true;
}

static bool g_init_all_expr_cmp_funcs = init_all_expr_cmp_funcs();

} // end namespace common
} // end namespace oceanbase

namespace oceanbase
{
namespace common
{

sql::ObExpr::EvalVectorFunc VectorCmpExprFuncsHelper::get_eval_vector_expr_cmp_func(
  const sql::ObDatumMeta &l_meta, const sql::ObDatumMeta &r_meta, const common::ObCmpOp cmp_op)
{
  LOG_DEBUG("eval vector expr_cmp_func", K(l_meta), K(r_meta), K(cmp_op));
  VecValueTypeClass l_tc = get_vec_value_tc(l_meta.type_, l_meta.scale_, l_meta.precision_);
  VecValueTypeClass r_tc = get_vec_value_tc(r_meta.type_, r_meta.scale_, r_meta.precision_);
  return EVAL_VECTOR_EXPR_CMP_FUNCS[l_tc][r_tc][cmp_op];
}

} // end namespace common

namespace sql
{
void *g_ser_eval_vector_expr_cmp_funcs[MAX_VEC_TC * MAX_VEC_TC * 7];
void *g_ser_nullsafe_rowcmp_funcs[MAX_VEC_TC * MAX_VEC_TC * 2];
void *g_ser_rowcmp_funcs[MAX_VEC_TC * MAX_VEC_TC];

static_assert(sizeof(g_ser_eval_vector_expr_cmp_funcs) == sizeof(EVAL_VECTOR_EXPR_CMP_FUNCS),
              "unexpected size");
static_assert(sizeof(g_ser_nullsafe_rowcmp_funcs) == sizeof(NULLSAFE_ROW_CMP_FUNCS),
              "unexpected size");
static_assert(sizeof(g_ser_rowcmp_funcs) == sizeof(ROW_CMP_FUNCS),
              "unexpected size");
bool g_ser_eval_vector_expr_cmp_funcs_init = ObFuncSerialization::convert_NxN_array(
  g_ser_eval_vector_expr_cmp_funcs, reinterpret_cast<void **>(EVAL_VECTOR_EXPR_CMP_FUNCS),
  MAX_VEC_TC, 7, 0, 7);
bool g_ser_nullsafe_rowcmp_funcs_init = ObFuncSerialization::convert_NxN_array(
  g_ser_nullsafe_rowcmp_funcs, reinterpret_cast<void **>(NULLSAFE_ROW_CMP_FUNCS),
  MAX_VEC_TC, 2, 0, 2);
bool g_ser_rowcmp_funcs_init = ObFuncSerialization::convert_NxN_array(
  g_ser_rowcmp_funcs, reinterpret_cast<void **>(ROW_CMP_FUNCS),
  MAX_VEC_TC, 1, 0, 1);
REG_SER_FUNC_ARRAY(OB_SFA_CMP_EXPR_EVAL_VECTOR, g_ser_eval_vector_expr_cmp_funcs,
                   sizeof(g_ser_eval_vector_expr_cmp_funcs) / sizeof(void *));

REG_SER_FUNC_ARRAY(OB_SFA_VECTOR_NULLSAFE_CMP, g_ser_nullsafe_rowcmp_funcs,
                   sizeof(g_ser_nullsafe_rowcmp_funcs) / sizeof(void *));
REG_SER_FUNC_ARRAY(OB_SFA_VECTOR_CMP, g_ser_rowcmp_funcs,
                   sizeof(g_ser_rowcmp_funcs) / sizeof(void *));
} // end namespace sql
} // end namespace oceanabse
