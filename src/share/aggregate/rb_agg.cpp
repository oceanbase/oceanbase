/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "rb_agg.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{
int init_rb_build_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                         ObIAllocator &allocator, IAggregate *&agg)
{
  return init_rb_aggregate<T_FUN_SYS_RB_BUILD_AGG>(agg_ctx, agg_col_id, allocator, agg);
}

int init_rb_and_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                          ObIAllocator &allocator, IAggregate *&agg)
{
  return init_rb_aggregate<T_FUN_SYS_RB_AND_AGG>(agg_ctx, agg_col_id, allocator, agg);
}

int init_rb_or_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                         ObIAllocator &allocator, IAggregate *&agg)
{
  return init_rb_aggregate<T_FUN_SYS_RB_OR_AGG>(agg_ctx, agg_col_id, allocator, agg);
}

} // end namespace helper
} // end namespace aggregate
} // end namespace share
} // end namespace oceanbase