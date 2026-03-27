/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_SHARE_AGG_SINGLE_ROW_CNT_SUM_H_
#define OB_SHARE_AGG_SINGLE_ROW_CNT_SUM_H_

#include "single_row.h"
#include "single_row_init.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{
int init_single_row_count_sum_aggregate(RuntimeContext &agg_ctx, const int col_id,
                                        ObIAllocator &allocator, IAggregate *&agg);
} // end helper
} // end aggregate
} // end share
} // end oceanbase

#endif // OB_SHARE_AGG_SINGLE_ROW_CNT_SUM_H_