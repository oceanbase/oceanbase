/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_EXE
#include "sql/engine/expr/ob_expr_local_dynamic_filter.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace sql
{

int ObExprLocalDynamicFilter::calc_result_typeN(ObExprResType &type, ObExprResType *types,
                                                int64_t param_num, ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  UNUSED(types);
  UNUSED(param_num);
  type.set_int32();
  type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
  type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  return ret;
}

// local dynamic filter should only be pushdown white filter, and it is not expected to call the eval function.
int ObExprLocalDynamicFilter::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                      ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_local_dynamic_filter;
  rt_expr.eval_batch_func_ = eval_local_dynamic_filter_batch;
  return ret;
}

int ObExprLocalDynamicFilter::eval_local_dynamic_filter(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return OB_NOT_IMPLEMENT;
}

int ObExprLocalDynamicFilter::eval_local_dynamic_filter_batch(const ObExpr &expr, ObEvalCtx &ctx,
  const ObBitVector &skip, const int64_t batch_size)
{
  return OB_NOT_IMPLEMENT;
}

int ObExprLocalDynamicFilter::prepare_storage_white_filter_data(const ObExpr &expr,
                                                                ObDynamicFilterExecutor &dynamic_filter,
                                                                ObEvalCtx &eval_ctx,
                                                                LocalDynamicFilterParams &params,
                                                                bool &is_data_prepared)
{
  return OB_NOT_IMPLEMENT;
}

int ObExprLocalDynamicFilter::update_storage_white_filter_data(const ObExpr &expr,
                                                               ObDynamicFilterExecutor &dynamic_filter,
                                                               ObEvalCtx &eval_ctx,
                                                               LocalDynamicFilterParams &params,
                                                               bool &is_update)
{
  return OB_NOT_IMPLEMENT;
}

// just use pk increment value as hash val to leverage current logic of dynamic in filter
int ObExprLocalDynamicFilter::pk_increment_hash_func(const common::ObDatum &datum, const uint64_t seed, uint64_t &res)
{
  int ret = OB_SUCCESS;
  UNUSED(seed);
  res = 0;
  if (datum.is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("datum is null", K(ret));
  } else {
    res = datum.get_uint64();
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
