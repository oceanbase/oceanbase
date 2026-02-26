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

#ifndef OCEANBASE_SHARE_VECTOR_EXPR_CMP_FUNC_H_
#define OCEANBASE_SHARE_VECTOR_EXPR_CMP_FUNC_H_

#include "common/object/ob_obj_type.h"
#include "sql/engine/expr/ob_expr.h"
#include "common/object/ob_obj_compare.h"

namespace oceanbase
{
namespace sql
{
  struct ObDatumMeta;
} // end namespace sql

namespace common
{

struct VectorCmpExprFuncsHelper
{
  static void get_cmp_set(const sql::ObDatumMeta &l_meta, const sql::ObDatumMeta &r_meta,
                          sql::NullSafeRowCmpFunc &null_first_cmp, sql::NullSafeRowCmpFunc &null_last_cmp);

  static sql::RowCmpFunc get_row_cmp_func(const sql::ObDatumMeta &l_meta,
                                          const sql::ObDatumMeta &r_meta);

  static sql::ObExpr::EvalVectorFunc get_eval_vector_expr_cmp_func(const sql::ObDatumMeta &l_meta,
                                                                   const sql::ObDatumMeta &r_meta,
                                                                   const common::ObCmpOp cmp_op);
};

} // end namespace common
} // end namespace oceanbase
 #endif // OCEANBASE_SHARE_VECTOR_EXPR_CMP_FUNC_H_