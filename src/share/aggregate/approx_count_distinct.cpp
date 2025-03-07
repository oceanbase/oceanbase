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
#define USING_LOG_PREFIX SQL_ENG

#include "approx_count_distinct.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{
extern int init_approx_count_distinct_mysql_aggregate(RuntimeContext &agg_ctx,
                                                      const int64_t agg_col_id,
                                                      ObIAllocator &allocator, IAggregate *&agg);
extern int init_approx_count_distinct_oracle_aggregate(RuntimeContext &agg_ctx,
                                                       const int64_t agg_col_id,
                                                       ObIAllocator &allocator, IAggregate *&agg);
int init_approx_count_distinct_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                                         ObIAllocator &allocator, IAggregate *&agg)
{
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    ret = init_approx_count_distinct_mysql_aggregate(agg_ctx, agg_col_id, allocator, agg);
  } else {
    ret = init_approx_count_distinct_oracle_aggregate(agg_ctx, agg_col_id, allocator, agg);
  }
  return ret;
}

int init_approx_count_distinct_synopsis_merge_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                                         ObIAllocator &allocator, IAggregate *&agg)
{
  int ret = OB_SUCCESS;
  ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
  if (T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE == aggr_info.get_expr_type()) {
    ObDatumMeta &param_meta = aggr_info.param_exprs_.at(0)->datum_meta_;
    VecValueTypeClass in_tc =
      get_vec_value_tc(param_meta.type_, param_meta.scale_, param_meta.precision_);
    if (in_tc != VEC_TC_STRING) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid input type", K(in_tc), K(param_meta));
    } else {
      ret = init_agg_func<ApproxCountDistinct<T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE,
                                              VEC_TC_STRING, VEC_TC_STRING>>(
        agg_ctx, agg_col_id, aggr_info.has_distinct_, allocator, agg);
    }
  }
  return ret;
}
}
} // end aggregate
} // end share
} // end oceanbase