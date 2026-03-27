/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL
#include "arg_max.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{
int init_arg_max_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                       ObIAllocator &allocator, IAggregate *&agg)
{
  return init_min_max_agg<T_FUN_ARG_MAX>(agg_ctx, agg_col_id, allocator, agg);
}
} // end helper
} // end aggregate
} // end share
} // end oceanbase