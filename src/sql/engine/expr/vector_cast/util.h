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

#ifndef OCEANBASE_SQL_ENG_EXPR_VECTOR_CAST_UTIL_H_
#define OCEANBASE_SQL_ENG_EXPR_VECTOR_CAST_UTIL_H_

#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "share/ob_errno.h"
#include "share/object/ob_obj_cast.h"

#define CAST_CHECKER_ARG_DECL const ObExpr &expr, ObEvalCtx &ctx, const EvalBound &bound,\
                                     const ObBitVector &skip, int &warning
#define CAST_CHECKER_ARG  expr, ctx, bound, skip, warning

namespace oceanbase
{
using namespace common;
namespace sql
{
template<VecValueTypeClass vec_tc, typename Vector>
struct BatchValueRangeChecker
{
  static const bool defined_ = false;
  static int check(CAST_CHECKER_ARG_DECL);
};

} // end sql
} // end ocenabase

#include "sql/engine/expr/vector_cast/util.ipp"

#undef DEF_BATCH_RANGE_CHECKER_DECL
#undef DEF_BATCH_RANGE_CHECKER_ARG
#endif // OCEANBASE_SQL_ENG_EXPR_VECTOR_CASE_UTIL_H_