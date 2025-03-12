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