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