/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL
#include "stddev_samp.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{
int init_stddev_samp_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                            ObIAllocator &allocator, IAggregate *&agg)
{
  return init_statistics_aggregate<STDDEV_SAMP>(agg_ctx, agg_col_id, allocator, agg);
}
} // end helper
} // end aggregate
} // end share
} // end oceanbase