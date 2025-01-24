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

namespace oceanbase
{
namespace share
{
namespace aggregate
{
  // TODO: add approx_distinct_count
  // TODO: add sysbit_ops
namespace helper
{

extern int init_single_row_sum_aggregate(RuntimeContext &agg_ctx, const int col_id, ObIAllocator &allocator,
                                          IAggregate *&agg);
extern int init_single_row_count_aggregate(RuntimeContext &agg_ctx, const int col_id, ObIAllocator &allocator,
                                            IAggregate *&agg);
extern int init_single_row_min_max_aggregate(RuntimeContext &agg_ctx, const int col_id, ObIAllocator &allocator,
                                              IAggregate *&agg);

int init_single_row_aggregates(RuntimeContext &agg_ctx, ObIAllocator &allocator,
                               ObIArray<IAggregate *> &aggregates)
{
  int ret = OB_SUCCESS;
  IAggregate *agg= nullptr;
  for (int i = 0; OB_SUCC(ret) && i < agg_ctx.aggr_infos_.count(); i++) {
    ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(i);
    agg = nullptr;
    if (OB_ISNULL(aggr_info.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid null aggregate expr", K(ret));
    } else if (aggr_info.get_expr_type() == T_FUN_COUNT && aggr_info.param_exprs_.empty()) {
      if (lib::is_mysql_mode()) {
        ret = init_agg_func<SingleRowAggregate<T_FUN_COUNT, VEC_TC_INTEGER, VEC_TC_INTEGER>>(
          agg_ctx, i, allocator, agg);
      } else {
        ret = init_agg_func<SingleRowAggregate<T_FUN_COUNT, VEC_TC_INTEGER, VEC_TC_NUMBER>>(
          agg_ctx, i, allocator, agg);
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("init single row aggregate failed", K(ret));
      }
    } else if (aggr_info.is_implicit_first_aggr()) {// do nothing
    } else {
      ObDatumMeta &res_meta = aggr_info.expr_->datum_meta_;
      ObDatumMeta &child_meta = aggr_info.param_exprs_.at(0)->datum_meta_;
      VecValueTypeClass res_vec =
        get_vec_value_tc(res_meta.type_, res_meta.scale_, res_meta.precision_);
      VecValueTypeClass param_vec =
        get_vec_value_tc(child_meta.type_, child_meta.scale_, child_meta.precision_);
      switch(aggr_info.get_expr_type()) {
      case T_FUN_COUNT: {
        ret = init_single_row_count_aggregate(agg_ctx, i, allocator, agg);
        break;
      }
      case T_FUN_SUM:
      case T_FUN_COUNT_SUM: {
        ret = init_single_row_sum_aggregate(agg_ctx, i, allocator, agg);
        break;
      }
      case T_FUN_MIN:
      case T_FUN_MAX: {
        ret = init_single_row_min_max_aggregate(agg_ctx, i, allocator, agg);
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        SQL_LOG(WARN, "not supported aggregate operation", K(ret), K(*aggr_info.expr_));
        break;
      }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(aggregates.push_back(agg))) {
      SQL_LOG(WARN, "push back element failed", K(ret));
    }
  } // end for
  return ret;
}
} // end helper
} // end aggregate
} // end share
} // end oceanbase