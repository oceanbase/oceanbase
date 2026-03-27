/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "share/aggregate/group_concat.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{
int init_group_concat_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                                ObIAllocator &allocator, IAggregate *&agg)
{
  int ret = OB_SUCCESS;
  ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
  agg = nullptr;
  bool has_distinct = aggr_info.has_distinct_;

  if (!aggr_info.has_order_by_) {
    ret = init_agg_func<GroupConcatAggregate<VEC_TC_STRING>>(agg_ctx, agg_col_id, has_distinct, allocator,
                                                      agg);
  } else {
    ret = init_agg_func<GroupConcatAggregate<VEC_TC_STRING>>(agg_ctx, agg_col_id, has_distinct, allocator,
                                                      agg, true);
  }
  return ret;
}
} // end namespace helper

} // end namespace aggregate
} // end namespace share
} // end namespace oceanbase