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
namespace oceanbase
{
using namespace common;
namespace sql
{
template <VecValueTypeClass vec_tc, typename Vector>
struct ValueRangeChecker
{
  static const bool defined_ = false;
  static int check(const ObExpr &expr, ObEvalCtx &ctx, const uint64_t cast_mode, Vector *res_vec,
                   const int64_t batch_idx, int &warning)
  {
    int ret = OB_NOT_IMPLEMENT;
    SQL_LOG(WARN, "not implemented accuracy checker", K(ret), K(vec_tc));
    return ret;
  }
};

template<VecValueTypeClass vec_tc, typename Vector>
struct BatchValueRangeChecker
{
  static int check(const ObExpr &expr, ObEvalCtx &ctx, const EvalBound &bound,
                   const ObBitVector &skip, int &warning)
  {
    int ret = OB_SUCCESS;
    warning = ret;
    Vector *res_vec = static_cast<Vector *>(expr.get_vector(ctx));
    if (bound.get_all_rows_active() && !res_vec->has_null()) {
      for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
        ret = ValueRangeChecker<vec_tc, Vector>::check(
          expr, ctx, expr.extra_, res_vec, i, warning);
      }
    } else {
      for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
        if (skip.at(i) || res_vec->is_null(i)) { continue; }
        ret = ValueRangeChecker<vec_tc, Vector>::check(
          expr, ctx, expr.extra_, res_vec, i, warning);
      }
    }
    return ret;
  }
};

} // end sql
} // end ocenabase

#include "sql/engine/expr/vector_cast/util.ipp"

#endif // OCEANBASE_SQL_ENG_EXPR_VECTOR_CASE_UTIL_H_