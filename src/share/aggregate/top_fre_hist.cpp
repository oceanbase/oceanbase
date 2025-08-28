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

#include "top_fre_hist.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{

#define INIT_TOP_FRE_HIST_CASE(vec_tc)                                                  \
  case (vec_tc): {                                                                      \
    ret = init_agg_func<TopFreHist<vec_tc, false>>(                                     \
      agg_ctx, agg_col_id, has_distinct, allocator, agg);                               \
  } break

int init_top_fre_hist_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                                ObIAllocator &allocator, IAggregate *&agg)
{
  int ret = OB_SUCCESS;
  ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
  ObDatumMeta &param_meta = aggr_info.param_exprs_.at(0)->datum_meta_;
  VecValueTypeClass in_tc =
      get_vec_value_tc(param_meta.type_, param_meta.scale_, param_meta.precision_);
  bool has_distinct = aggr_info.has_distinct_;
  if (aggr_info.is_need_deserialize_row_) {
    if (in_tc == VEC_TC_LOB) {
      ret = init_agg_func<TopFreHist<VEC_TC_LOB, true>>(
              agg_ctx, agg_col_id, has_distinct, allocator, agg);
    } else {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid aggr expr", K(ret), K(in_tc));
    }
  } else {
    switch (in_tc) {
      LST_DO_CODE(INIT_TOP_FRE_HIST_CASE, AGG_VEC_TC_LIST);
      default: {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "invalid param format", K(ret), K(in_tc));
      }
    }
  }
  
  return ret;
}

}
}
}
}