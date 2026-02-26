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
#define USING_LOG_PREFIX SQL

#include "single_row_sum.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{
int init_single_row_sum_agg(VecValueTypeClass in_tc, VecValueTypeClass out_tc,
                            RuntimeContext &agg_ctx, int64_t col_id, ObIAllocator &allocator,
                            IAggregate *&agg)
{
  int ret = OB_SUCCESS;
  ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(col_id);
  switch (in_tc) {
  case VEC_TC_INTEGER: {
    switch (out_tc) {
    case VEC_TC_NUMBER: {
      INIT_SUM_TO_NMB_CASE(VEC_TC_INTEGER);
      break;
    }
    case VEC_TC_DEC_INT128: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_INTEGER, VEC_TC_DEC_INT128);
      break;
    }
    case VEC_TC_DEC_INT256: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_INTEGER, VEC_TC_DEC_INT256);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    }
    break;
  }
  case VEC_TC_UINTEGER: {
    switch (out_tc) {
    case VEC_TC_NUMBER: {
      INIT_SUM_TO_NMB_CASE(VEC_TC_UINTEGER);
      break;
    }
    case VEC_TC_DEC_INT128: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_UINTEGER, VEC_TC_DEC_INT128);
      break;
    }
    case VEC_TC_DEC_INT256: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_UINTEGER, VEC_TC_DEC_INT256);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    }
    break;
  }
  case VEC_TC_DEC_INT32: {
    switch(out_tc) {
    case VEC_TC_NUMBER: {
      INIT_SUM_TO_NMB_CASE(VEC_TC_DEC_INT32);
      break;
    }
    case VEC_TC_DEC_INT128: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_DEC_INT32, VEC_TC_DEC_INT128);
      break;
    }
    case VEC_TC_DEC_INT256: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_DEC_INT32, VEC_TC_DEC_INT256);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    }
    break;
  }
  case VEC_TC_DEC_INT64: {
    switch (out_tc) {
    case VEC_TC_NUMBER: {
      INIT_SUM_TO_NMB_CASE(VEC_TC_DEC_INT64);
      break;
    }
    case VEC_TC_DEC_INT128: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_DEC_INT64, VEC_TC_DEC_INT128);
      break;
    }
    case VEC_TC_DEC_INT256: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_DEC_INT64, VEC_TC_DEC_INT256);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    }
    break;
  }
  case VEC_TC_DEC_INT128: {
    switch (out_tc) {
    case VEC_TC_NUMBER: {
      INIT_SUM_TO_NMB_CASE(VEC_TC_DEC_INT128);
      break;
    }
    case VEC_TC_DEC_INT128: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_DEC_INT128, VEC_TC_DEC_INT128);
      break;
    }
    case VEC_TC_DEC_INT256: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_DEC_INT128, VEC_TC_DEC_INT256);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    }
    break;
  }
  case VEC_TC_DEC_INT256: {
    switch (out_tc) {
    case VEC_TC_NUMBER: {
      INIT_SUM_TO_NMB_CASE(VEC_TC_DEC_INT256);
      break;
    }
    case VEC_TC_DEC_INT256: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_DEC_INT256, VEC_TC_DEC_INT256);
      break;
    }
    case VEC_TC_DEC_INT512: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_DEC_INT256, VEC_TC_DEC_INT512);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    }
    break;
  }
  case VEC_TC_DEC_INT512: {
    switch(out_tc) {
    case VEC_TC_NUMBER: {
      INIT_SUM_TO_NMB_CASE(VEC_TC_DEC_INT512);
      break;
    }
    case VEC_TC_DEC_INT512: {
      INIT_SUM_TO_DEC_CASE(VEC_TC_DEC_INT512, VEC_TC_DEC_INT512);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    }
    break;
  }
  case VEC_TC_NUMBER: {
    if (out_tc != VEC_TC_NUMBER) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      INIT_SUM_TO_NMB_CASE(VEC_TC_NUMBER);
    }
    break;
  }
  case VEC_TC_FLOAT: {
    if (out_tc != VEC_TC_FLOAT) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret =
        init_agg_func<SingleRowAggregate<T_FUN_SUM, VEC_TC_FLOAT, VEC_TC_FLOAT>>(agg_ctx, col_id, allocator, agg);
    }
    break;
  }
  case VEC_TC_DOUBLE: {
    if (out_tc != VEC_TC_DOUBLE) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret =
        init_agg_func<SingleRowAggregate<T_FUN_SUM, VEC_TC_DOUBLE, VEC_TC_DOUBLE>>(agg_ctx, col_id, allocator, agg);
    }
    break;
  }
  case VEC_TC_FIXED_DOUBLE: {
    if (out_tc != VEC_TC_FIXED_DOUBLE) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = init_agg_func<SingleRowAggregate<T_FUN_SUM, VEC_TC_FIXED_DOUBLE, VEC_TC_FIXED_DOUBLE>>(
        agg_ctx, col_id, allocator, agg);
    }
    break;
  }
  case VEC_TC_COLLECTION: {
    if (out_tc != VEC_TC_COLLECTION) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = init_agg_func<SingleRowAggregate<T_FUN_SUM, VEC_TC_COLLECTION, VEC_TC_COLLECTION>>(
        agg_ctx, col_id, allocator, agg);
    }
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    break;
  }
  }
  if (OB_FAIL(ret)) {
    SQL_LOG(WARN, "init fast single aggregate failed", K(in_tc), K(out_tc));
  }
  return ret;
}

int init_single_row_sum_aggregate(RuntimeContext &agg_ctx, const int col_id, ObIAllocator &allocator,
                                  IAggregate *&agg)
{
  int ret = OB_SUCCESS;
  agg = nullptr;
  ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(col_id);
  VecValueTypeClass param_vec = aggr_info.param_exprs_.at(0)->get_vec_value_tc();
  if (aggr_info.get_expr_type() == T_FUN_SUM) {
    ret = init_single_row_sum_agg(param_vec, aggr_info.expr_->get_vec_value_tc(), agg_ctx, col_id, allocator, agg);
  }
  if (OB_FAIL(ret)) {
    SQL_LOG(WARN, "init count aggregate failed", K(ret));
  }
  return ret;
}
} // end helper
} // end aggregate
} // end share
} // end oceanbase