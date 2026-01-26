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

#include "statistics.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{

namespace helper
{

int init_var_samp_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                            ObIAllocator &allocator, IAggregate *&agg)
{
  int ret = OB_SUCCESS;
  ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
  OB_ASSERT(aggr_info.expr_ != NULL);
  OB_ASSERT(aggr_info.expr_->args_ != NULL && aggr_info.expr_->arg_cnt_ > 0);

  ObDatumMeta &in_meta = aggr_info.expr_->args_[0]->datum_meta_;
  ObDatumMeta &out_meta = aggr_info.expr_->datum_meta_;
  VecValueTypeClass in_tc = get_vec_value_tc(in_meta.type_, in_meta.scale_, in_meta.precision_);
  VecValueTypeClass out_tc = get_vec_value_tc(out_meta.type_, out_meta.scale_, out_meta.precision_);
#define INIT_VAR_SAMP_AGGREGATE(in_tc, out_tc) \
  if (out_tc == VEC_TC_DOUBLE) { \
    ret = init_agg_func<VarSampAggregate<in_tc, VEC_TC_DOUBLE>>( \
      agg_ctx, agg_col_id, aggr_info.has_distinct_, allocator, agg); \
  } else { \
    ret = init_agg_func<VarSampAggregate<in_tc, VEC_TC_FIXED_DOUBLE>>( \
      agg_ctx, agg_col_id, aggr_info.has_distinct_, allocator, agg); \
  } \
  break;

  switch (in_tc) {
    case VEC_TC_INTEGER: {
      INIT_VAR_SAMP_AGGREGATE(VEC_TC_INTEGER, VEC_TC_DOUBLE);
      break;
    }
    case VEC_TC_UINTEGER: {
      INIT_VAR_SAMP_AGGREGATE(VEC_TC_UINTEGER, VEC_TC_DOUBLE);
      break;
    }
    case VEC_TC_FLOAT: {
      INIT_VAR_SAMP_AGGREGATE(VEC_TC_FLOAT, VEC_TC_DOUBLE);
      break;
    }
    case VEC_TC_DOUBLE: {
      INIT_VAR_SAMP_AGGREGATE(VEC_TC_DOUBLE, VEC_TC_DOUBLE);
      break;
    }
    case VEC_TC_FIXED_DOUBLE: {
      INIT_VAR_SAMP_AGGREGATE(VEC_TC_FIXED_DOUBLE, VEC_TC_DOUBLE);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected in & out type class", K(in_tc), K(out_tc));
    }
  }

  if (OB_FAIL(ret)) {
    SQL_LOG(WARN, "init var_samp aggregate failed", K(ret), K(in_tc), K(out_tc));
  }
  return ret;
}

} // end helper

} // end aggregate
} // end share
} // end oceanbase
