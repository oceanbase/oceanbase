/**
 * Copyright (c) 2025 OceanBase
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

#include "str_prefix_max.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{

#define INIT_STR_PREFIX_AGG_CASE(vec_tc)                                                  \
  case (vec_tc): {                                                                        \
    ret = init_agg_func<StrPrefixMax<vec_tc>>(                                            \
      agg_ctx, agg_col_id, aggr_info.has_distinct_, allocator, agg);                      \
  } break

int init_string_prefix_max_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                                    ObIAllocator &allocator, IAggregate *&agg)
{
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
      LST_DO_CODE(INIT_STR_PREFIX_AGG_CASE, VEC_TC_STRING, VEC_TC_LOB);
    default: {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected result type of min/max aggregate", K(ret), K(res_vec));
    }
    }
  }
  return ret;
}

#undef INIT_STR_PREFIX_AGG_CASE

}
}
}
}