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

#define USING_LOG_PREFIX SQL

#include "arg_min.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{

int init_arg_min_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                           ObIAllocator &allocator, IAggregate *&agg)
{
  return init_min_max_agg<T_FUN_ARG_MIN>(agg_ctx, agg_col_id, allocator, agg);
}
} // end helper
} // end aggregate
} // end share
} // end oceanbase