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
#include "min_max.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{
template <ObItemType func_type>
inline static int init_min_max_agg(RuntimeContext &agg_ctx,
                                   const int64_t agg_col_id, ObIAllocator &allocator,
                                   IAggregate *&agg)
{
#define INIT_AGGREGATE_CASE(vec_tc)                                                                \
  case (vec_tc): {                                                                                 \
    ret = init_agg_func<MinMaxAggregate<vec_tc, T_FUN_MIN == func_type>>(                          \
      agg_ctx, agg_col_id, aggr_info.has_distinct_, allocator, agg);                               \
    if (OB_FAIL(ret)) { SQL_LOG(WARN, "init aggregate failed", K(ret)); }                          \
  } break

  int ret = OB_SUCCESS;
  agg = nullptr;

  ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
  if (OB_ISNULL(aggr_info.expr_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "invalid null expr", K(ret));
  } else {
    VecValueTypeClass res_vec =
      get_vec_value_tc(aggr_info.expr_->datum_meta_.type_, aggr_info.expr_->datum_meta_.scale_,
                       aggr_info.expr_->datum_meta_.precision_);

    switch (res_vec) {
      LST_DO_CODE(INIT_AGGREGATE_CASE, AGG_VEC_TC_LIST);
    default: {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected result type of min/max aggregate", K(ret), K(res_vec));
    }
    }
  }
  return ret;

#undef INIT_AGGREGATE_CASE
}

int init_min_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                       ObIAllocator &allocator, IAggregate *&agg)
{
  return init_min_max_agg<T_FUN_MIN>(agg_ctx, agg_col_id, allocator, agg);
}

int init_max_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                       ObIAllocator &allocator, IAggregate *&agg)
{
  return init_min_max_agg<T_FUN_MAX>(agg_ctx, agg_col_id, allocator, agg);
}
} // end helper
} // end aggregate
} // end share
} // end oceanbase