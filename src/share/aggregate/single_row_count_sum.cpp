/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SQL

#include "single_row_count_sum.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{
int init_single_row_count_sum_aggregate(RuntimeContext &agg_ctx, const int col_id,
                                        ObIAllocator &allocator, IAggregate *&agg)
{
  int ret = OB_SUCCESS;
  agg = nullptr;
  ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(col_id);
  VecValueTypeClass param_vec = aggr_info.param_exprs_.at(0)->get_vec_value_tc();
  if (aggr_info.get_expr_type() == T_FUN_COUNT_SUM) {
    switch (param_vec) {
      LST_DO_CODE(INIT_GENERAL_COUNT_SUM_CASE, AGG_VEC_TC_LIST);
      default: {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected vector type class", K(param_vec), K(*aggr_info.expr_));
      }
    }
  }
  if (OB_FAIL(ret)) {
    SQL_LOG(WARN, "init count aggregate failed", K(ret));
  }
  return ret;
}
} // end helper
} // end aggregate
} // end share
} // end oceanbase