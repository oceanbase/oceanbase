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

#include "hybrid_hist.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{

int init_hybrid_hist_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                               ObIAllocator &allocator, IAggregate *&agg)
{
  int ret = OB_SUCCESS;
  ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
  ObDatumMeta &param_meta = aggr_info.param_exprs_.at(0)->datum_meta_;
  bool has_distinct = aggr_info.has_distinct_;
  if (OB_FAIL(init_agg_func<HybridHist>(agg_ctx, 
                                        agg_col_id, 
                                        has_distinct, 
                                        allocator, 
                                        agg, 
                                        true))) {
    SQL_LOG(WARN, "failed init agg func", K(ret));
  }
  return ret;
}

}
}
}
}