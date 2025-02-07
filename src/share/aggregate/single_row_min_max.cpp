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
int init_single_row_min_max_aggregate(RuntimeContext &agg_ctx, const int col_id, ObIAllocator &allocator,
                                      IAggregate *&agg)
{
  int ret = OB_SUCCESS;
  agg = nullptr;
  ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(col_id);
  VecValueTypeClass param_vec = aggr_info.param_exprs_.at(0)->get_vec_value_tc();
  if (aggr_info.get_expr_type() == T_FUN_MIN) {
    switch (param_vec) {
      LST_DO_CODE(INIT_GENERAL_MIN_CASE, AGG_VEC_TC_LIST);
      default: {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
    }
  } else if (aggr_info.get_expr_type() == T_FUN_MAX) {
    switch (param_vec) {
      LST_DO_CODE(INIT_GENERAL_MAX_CASE, AGG_VEC_TC_LIST);
      default: {
        ret = OB_ERR_UNEXPECTED;
        break;
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