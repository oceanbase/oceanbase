/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include "single_row.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{
#define INIT_APPROX_CNT_SYN(vec_tc)                                                                \
  case (vec_tc): {                                                                                 \
    ret = init_agg_func<SingleRowApproxCntSynopsis<vec_tc>>(agg_ctx, col_id, allocator, agg);      \
  } break

int init_single_row_approx_cnt_synopsis_aggregate(RuntimeContext &agg_ctx, const int col_id,
                                                  ObIAllocator &allocator, IAggregate *&agg)
{
  int ret = OB_SUCCESS;
  agg = nullptr;
  ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(col_id);
  VecValueTypeClass param_vec = aggr_info.param_exprs_.at(0)->get_vec_value_tc();
  switch (param_vec) {
    LST_DO_CODE(INIT_APPROX_CNT_SYN, AGG_VEC_TC_LIST);
    default: {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected vector type class", K(param_vec), K(*aggr_info.expr_));
    }
  }
  return ret;
}
} // end helper
} // end aggregate
} // end share
} // end oceanbase