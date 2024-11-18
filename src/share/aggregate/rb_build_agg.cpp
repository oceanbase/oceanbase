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
#include "rb_build_agg.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{

int init_rb_build_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id, ObIAllocator &allocator, IAggregate *&agg)
{
#define INIT_AGGREGATE_CASE(in_tc)                                                                      \
  case (in_tc): {                                                                                       \
    switch (out_tc) {                                                                                   \
    case VEC_TC_ROARINGBITMAP: {                                                                        \
      ret = init_agg_func<RbBuildAggregate<in_tc, VEC_TC_ROARINGBITMAP>>(agg_ctx, agg_col_id,           \
          aggr_info.has_distinct_, allocator, agg);                                                     \
      break;                                                                                            \
    }                                                                                                   \
    default: {                                                                                          \
      ret = OB_ERR_UNEXPECTED;                                                                          \
      SQL_LOG(WARN, "unexpected in & out type class", K(in_tc), K(out_tc));                             \
    }                                                                                                   \
    } break;                                                                                            \
  }

  int ret = OB_SUCCESS;
  ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
  OB_ASSERT(aggr_info.expr_ != NULL);
  OB_ASSERT(aggr_info.expr_->args_ != NULL && aggr_info.expr_->arg_cnt_ > 0);
  ObDatumMeta &in_meta = aggr_info.expr_->args_[0]->datum_meta_;
  ObDatumMeta &out_meta = aggr_info.expr_->datum_meta_;
  VecValueTypeClass in_tc = get_vec_value_tc(in_meta.type_, in_meta.scale_, in_meta.precision_);
  VecValueTypeClass out_tc = get_vec_value_tc(out_meta.type_, out_meta.scale_, out_meta.precision_);
  void *buf = nullptr;
  switch (in_tc) {
  INIT_AGGREGATE_CASE(VEC_TC_NULL)
  INIT_AGGREGATE_CASE(VEC_TC_INTEGER)
  INIT_AGGREGATE_CASE(VEC_TC_UINTEGER)
  default: {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    SQL_LOG(WARN, "invalid input type", K(in_tc), K(in_meta), K(out_tc));
  }
  }
  return ret;
#undef INIT_AGGREGATE_CASE
}

} // end namespace helper


} // end namespace aggregate
} // end namespace share
} // end namespace oceanbase