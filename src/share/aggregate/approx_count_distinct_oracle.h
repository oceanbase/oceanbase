/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_SHARE_AGG_APX_CNT_DISTINCT_ORACLE_H_
#define OB_SHARE_AGG_APX_CNT_DISTINCT_ORACLE_H_

#include "approx_count_distinct.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{
int init_approx_count_distinct_oracle_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                                                ObIAllocator &allocator, IAggregate *&agg);
} // end helper
} // end aggregate
} // end share
} // end oceanbase
#endif // OB_SHARE_AGG_APX_CNT_DISTINCT_ORACLE_H_