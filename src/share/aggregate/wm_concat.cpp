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

#include "wm_concat.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{

int init_wm_concat_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                             ObIAllocator &allocator, IAggregate *&agg)
{
  int ret = OB_SUCCESS;
  ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);

  bool has_distinct = aggr_info.has_distinct_;
  ObExprOperatorType expr_type = aggr_info.get_expr_type();

  VecValueTypeClass in_tc = VEC_TC_STRING;
  if (aggr_info.param_exprs_.count() > 0) {
    in_tc = aggr_info.param_exprs_.at(0)->get_vec_value_tc();
  }
  if (expr_type == T_FUN_KEEP_WM_CONCAT) {
    // For KEEP WM_CONCAT, always use the init_agg_func with the last parameter as true
    if (in_tc == VEC_TC_LOB) {
      ret = init_agg_func<WmConcatAggregate<VEC_TC_LOB, true>>(
        agg_ctx, agg_col_id, has_distinct, allocator, agg, true);
    } else {
      ret = init_agg_func<WmConcatAggregate<VEC_TC_STRING, false>>(
        agg_ctx, agg_col_id, has_distinct, allocator, agg, true);
    }
  } else {
    // suppose that clob has lob header
    switch (in_tc) {
      case VEC_TC_LOB: {
        if (!aggr_info.has_order_by_) {
          ret = init_agg_func<WmConcatAggregate<VEC_TC_LOB, true>>(
            agg_ctx, agg_col_id, has_distinct, allocator, agg);
        } else {
          ret = init_agg_func<WmConcatAggregate<VEC_TC_LOB, true>>(
            agg_ctx, agg_col_id, has_distinct, allocator, agg, true);
        }
        break;
      }
      default: {
        if (!aggr_info.has_order_by_) {
          ret = init_agg_func<WmConcatAggregate<VEC_TC_STRING, false>>(
            agg_ctx, agg_col_id, has_distinct, allocator, agg);
        } else {
          ret = init_agg_func<WmConcatAggregate<VEC_TC_STRING, false>>(
            agg_ctx, agg_col_id, has_distinct, allocator, agg, true);
        }
        break;
      }
    }
  }
  return ret;
}

} // end namespace helper
} // end namespace aggregate
} // end namespace share
} // end namespace oceanbase
