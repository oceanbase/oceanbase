/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_AGGREGATE_MAX_H_
#define OCEANBASE_SHARE_AGGREGATE_MAX_H_

#include "min_max.ipp"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{
template int init_min_max_agg<T_FUN_MAX>(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                                         ObIAllocator &allocator, IAggregate *&agg);
} // end helper
} // end aggregate
} // end share
} // end oceanbase
#endif // OCEANBASE_SHARE_AGGREGATE_MAX_H_