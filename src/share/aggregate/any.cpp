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
#include "any.h"


namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{
int init_any_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                       ObIAllocator &allocator, IAggregate *&agg)
{

#define INIT_AGGREGATE_CASE(vec_tc)                                                                \
  case (vec_tc): {                                                                                 \
    ret = init_agg_func<AnyAggregate<vec_tc>>(                                               \
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
    if (OB_UNLIKELY(aggr_info.param_exprs_.count() != 1)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected param count of any aggregate", K(ret), K(aggr_info.param_exprs_.count()));
    } else {
      const ObExpr *expr = aggr_info.param_exprs_.at(0);
      VecValueTypeClass res_vec =
                        get_vec_value_tc(expr->datum_meta_.type_, expr->datum_meta_.scale_,
                                         expr->datum_meta_.precision_);
      switch (res_vec) {
        LST_DO_CODE(INIT_AGGREGATE_CASE, AGG_VEC_TC_LIST);
      default: {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected result type of any aggregate", K(ret), K(res_vec));
      }
      }
    }
  }

  return ret;

#undef INIT_AGGREGATE_CASE
}




} // end helper
} // end aggregate
} // end share
} // end oceanbase
