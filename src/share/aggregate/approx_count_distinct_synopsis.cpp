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

#include "approx_count_distinct_synopsis.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{
int init_approx_count_distinct_synopsis_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                                                  ObIAllocator &allocator, IAggregate *&agg)
{
#define INIT_APP_CNT_DIST_SYN_CASE(vec_tc)                                                         \
  case (vec_tc): {                                                                                 \
    ret = init_agg_func<                                                                           \
      ApproxCountDistinct<T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS, vec_tc, VEC_TC_STRING>>(           \
      agg_ctx, agg_col_id, has_distinct, allocator, agg);                                          \
  } break

  int ret = OB_SUCCESS;
  ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
  bool has_distinct = aggr_info.has_distinct_;
  if (T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS == aggr_info.get_expr_type()) {
    VecValueTypeClass vec_tc =
      get_vec_value_tc(aggr_info.get_first_child_type(), aggr_info.get_first_child_datum_scale(),
                       aggr_info.get_first_child_datum_precision());
    switch (vec_tc) {
      LST_DO_CODE(INIT_APP_CNT_DIST_SYN_CASE, AGG_VEC_TC_LIST);
      default: {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "invalid param format", K(ret), K(vec_tc));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid function type", K(ret), K(aggr_info.get_expr_type()), K(aggr_info));
  }
  return ret;
}
} // end helper
} // end aggregate
} // end share
} // end oceanbase