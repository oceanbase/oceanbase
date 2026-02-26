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

#include "sum_opnsize.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{

int init_sum_opnsize_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                               ObIAllocator &allocator, IAggregate *&agg)
{
  int ret = OB_SUCCESS;
  ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
  bool has_distinct = aggr_info.has_distinct_;
  if (aggr_info.param_exprs_.count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("current param exprs count is not 1", K(ret));
  } else {
    VecValueTypeClass vec_tc =
      get_vec_value_tc(aggr_info.get_first_child_type(), aggr_info.get_first_child_datum_scale(),
                      aggr_info.get_first_child_datum_precision());
    bool has_lob_header = is_lob_storage(aggr_info.get_first_child_type()) &&
                          aggr_info.param_exprs_.at(0)->obj_meta_.has_lob_header();
    if (lib::is_oracle_mode() && !has_lob_header) {
      ret = init_agg_func<
              SumOpNSize<VEC_TC_NUMBER, false>>(agg_ctx, agg_col_id, has_distinct, allocator, agg);
    } else if (!lib::is_oracle_mode() && !has_lob_header) {
      ret = init_agg_func<
              SumOpNSize<VEC_TC_INTEGER, false>>(agg_ctx, agg_col_id, has_distinct, allocator, agg);
    } else if (lib::is_oracle_mode() && has_lob_header) {
      ret = init_agg_func<
              SumOpNSize<VEC_TC_NUMBER, true>>(agg_ctx, agg_col_id, has_distinct, allocator, agg);
    } else {
      ret = init_agg_func<
              SumOpNSize<VEC_TC_INTEGER, true>>(agg_ctx, agg_col_id, has_distinct, allocator, agg);
    }
  }
  return ret;
}

}
} // end aggregate
} // end share
} // end oceanbase