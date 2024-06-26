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

#include "approx_count_distinct.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{
int init_approx_count_distinct_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                                         ObIAllocator &allocator, IAggregate *&agg)
{
#define INIT_CASE(func_type, vec_tc)                                                               \
  case (vec_tc): {                                                                                 \
    if (func_type == T_FUN_APPROX_COUNT_DISTINCT) {                                                \
      if (lib::is_oracle_mode()) {                                                                 \
        ret = init_agg_func<ApproxCountDistinct<func_type, vec_tc, VEC_TC_NUMBER>>(                \
          agg_ctx, agg_col_id, has_distinct, allocator, agg);                                      \
      } else {                                                                                     \
        ret = init_agg_func<ApproxCountDistinct<func_type, vec_tc, VEC_TC_INTEGER>>(               \
          agg_ctx, agg_col_id, has_distinct, allocator, agg);                                      \
      }                                                                                            \
    } else {                                                                                       \
      ret = init_agg_func<ApproxCountDistinct<func_type, vec_tc, VEC_TC_STRING>>(                  \
        agg_ctx, agg_col_id, has_distinct, allocator, agg);                                        \
    }                                                                                              \
  } break

#define INIT_APP_CNT_DISTINCT_CASE(vec_tc) INIT_CASE(T_FUN_APPROX_COUNT_DISTINCT, vec_tc)
#define INIT_APP_CNT_DIST_SYN_CASE(vec_tc) INIT_CASE(T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS, vec_tc)
#define INIT_APP_CNT_DIST_MERGE_CASE(vec_tc) INIT_CASE(T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE, vec_tc)

  int ret = OB_SUCCESS;
  ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
  bool has_distinct = aggr_info.has_distinct_;
  if (T_FUN_APPROX_COUNT_DISTINCT == aggr_info.get_expr_type()) {
    ObDatumMeta &param_meta = aggr_info.param_exprs_.at(0)->datum_meta_;
    VecValueTypeClass in_tc =
      get_vec_value_tc(param_meta.type_, param_meta.scale_, param_meta.precision_);
    switch (in_tc) {
      LST_DO_CODE(INIT_APP_CNT_DISTINCT_CASE, AGG_VEC_TC_LIST);
      default: {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "invalid param format", K(ret), K(in_tc));
      }
    }
  } else if (T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE == aggr_info.get_expr_type()) {
    ObDatumMeta &param_meta = aggr_info.param_exprs_.at(0)->datum_meta_;
    VecValueTypeClass in_tc =
      get_vec_value_tc(param_meta.type_, param_meta.scale_, param_meta.precision_);
    switch (in_tc) {
      LST_DO_CODE(INIT_APP_CNT_DIST_MERGE_CASE, AGG_VEC_TC_LIST);
    default: {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid param format", K(ret), K(in_tc));
    }
    }
  } else if (T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS == aggr_info.get_expr_type()) {
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
  }
  return ret;
}
}
} // end aggregate
} // end share
} // end oceanbase