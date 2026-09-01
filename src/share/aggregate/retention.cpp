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

#include "share/aggregate/retention.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{

int init_retention_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                             ObIAllocator &allocator, IAggregate *&agg)
{
  int ret = OB_SUCCESS;
  ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
  if (OB_ISNULL(aggr_info.expr_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "invalid null expr", K(ret));
  } else if (OB_UNLIKELY(aggr_info.param_exprs_.count() < 1
                         || aggr_info.param_exprs_.count() > RetentionAggregate::MAX_RETENTION_EVENTS)) {
    // stage-2 (merge) has exactly 1 collection param; stage-1 has 1~32 int params.
    // Either way, count must fall in [1, MAX_RETENTION_EVENTS].
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "retention requires 1 to MAX_RETENTION_EVENTS parameters",
            K(ret), K(aggr_info.param_exprs_.count()));
  } else if (OB_FAIL(helper::init_agg_func<RetentionAggregate>(agg_ctx, agg_col_id,
                                                                false /*has_distinct*/,
                                                                allocator, agg))) {
    SQL_LOG(WARN, "init retention aggregate failed", K(ret));
  }
  return ret;
}

} // namespace helper
} // namespace aggregate
} // namespace share
} // namespace oceanbase
