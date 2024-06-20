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

#include "sum.h"
#include "sum_nmb.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{

namespace helper
{

int init_sum_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                       ObIAllocator &allocator, IAggregate *&agg, int32_t *tmp_res_size)
{
#define INIT_SUM_AGGREGATE_TMP_STORE(in_tc, out_tc, tmp_tc)                                        \
  if (tmp_res_size != nullptr) {                                                                   \
    *tmp_res_size = sizeof(RTCType<tmp_tc>);                                                       \
  } else {                                                                                         \
    ret = init_agg_func<SumAggregateWithTempStore<in_tc, out_tc, RTCType<tmp_tc>>>(                \
      agg_ctx, agg_col_id, aggr_info.has_distinct_, allocator, agg);                               \
  }

#define INIT_SUM_AGGREGATE(in_tc, out_tc)                                                          \
  if (tmp_res_size != nullptr) {                                                                   \
    *tmp_res_size = 0;                                                                             \
  } else {                                                                                         \
    ret = init_agg_func<SumAggregate<in_tc, out_tc>>(agg_ctx, agg_col_id, aggr_info.has_distinct_, \
                                                     allocator, agg);                              \
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
  case (VEC_TC_INTEGER): { // integer
    switch (out_tc) {
    case VEC_TC_NUMBER: {
      INIT_SUM_AGGREGATE_TMP_STORE(VEC_TC_INTEGER, VEC_TC_NUMBER, VEC_TC_INTEGER);
      break;
    }
    case VEC_TC_DEC_INT64: {
      INIT_SUM_AGGREGATE(VEC_TC_INTEGER, VEC_TC_DEC_INT64);
      break;
    }
    case VEC_TC_DEC_INT128: {
      INIT_SUM_AGGREGATE_TMP_STORE(VEC_TC_INTEGER, VEC_TC_DEC_INT128, VEC_TC_INTEGER);
      break;
    }
    case VEC_TC_DEC_INT256: {
      INIT_SUM_AGGREGATE_TMP_STORE(VEC_TC_INTEGER, VEC_TC_DEC_INT256, VEC_TC_INTEGER);
      break;
    }
    case VEC_TC_DEC_INT512: {
      INIT_SUM_AGGREGATE_TMP_STORE(VEC_TC_INTEGER, VEC_TC_DEC_INT512, VEC_TC_INTEGER);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected in & out type class", K(in_tc), K(out_tc));
    }
    }
    break;
  }
  case (VEC_TC_UINTEGER): { // uinteger
     switch (out_tc) {
    case VEC_TC_NUMBER: {
      INIT_SUM_AGGREGATE_TMP_STORE(VEC_TC_UINTEGER, VEC_TC_NUMBER, VEC_TC_UINTEGER);
      break;
    }
    case VEC_TC_DEC_INT64: {
      INIT_SUM_AGGREGATE(VEC_TC_UINTEGER, VEC_TC_DEC_INT64);
      break;
    }
    case VEC_TC_DEC_INT128: {
      INIT_SUM_AGGREGATE_TMP_STORE(VEC_TC_UINTEGER, VEC_TC_DEC_INT128, VEC_TC_UINTEGER);
      break;
    }
    case VEC_TC_DEC_INT256: {
      INIT_SUM_AGGREGATE_TMP_STORE(VEC_TC_UINTEGER, VEC_TC_DEC_INT256, VEC_TC_UINTEGER);
      break;
    }
    case VEC_TC_DEC_INT512: {
      INIT_SUM_AGGREGATE_TMP_STORE(VEC_TC_UINTEGER, VEC_TC_DEC_INT512, VEC_TC_UINTEGER);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected in & out type class", K(in_tc), K(out_tc));
    }
    }
    break;
  }
  case (VEC_TC_FLOAT): { // float
    if (out_tc != VEC_TC_FLOAT) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected in & out type class", K(in_tc), K(out_tc));
    } else {
      INIT_SUM_AGGREGATE(VEC_TC_FLOAT, VEC_TC_FLOAT);
    }
    break;
  }
  case (VEC_TC_FIXED_DOUBLE): { // fixed double
    if (out_tc != VEC_TC_FIXED_DOUBLE) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected in & out type class", K(in_tc), K(out_tc));
    } else {
      INIT_SUM_AGGREGATE(VEC_TC_FIXED_DOUBLE, VEC_TC_FIXED_DOUBLE);
    }
    break;
  }
  case (VEC_TC_DOUBLE): { // double
    if (out_tc != VEC_TC_DOUBLE) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected in & out type class", K(in_tc), K(out_tc));
    } else {
      INIT_SUM_AGGREGATE(VEC_TC_DOUBLE, VEC_TC_DOUBLE);
    }
    break;
  }
  case (VEC_TC_NUMBER): { // number
    if (out_tc != VEC_TC_NUMBER) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected in & out type class", K(in_tc), K(out_tc));
    } else {
      if (tmp_res_size != nullptr) {
        *tmp_res_size = 0;
      } else {
        ret = init_agg_func<SumNumberAggregate>(agg_ctx, agg_col_id, aggr_info.has_distinct_,
                                                allocator, agg);
      }
    }
    break;
  }
  case VEC_TC_DEC_INT32: { // int32
    switch(out_tc) {
    case VEC_TC_NUMBER: {
      INIT_SUM_AGGREGATE_TMP_STORE(VEC_TC_DEC_INT32, VEC_TC_NUMBER, VEC_TC_DEC_INT64);
      break;
    }
    case VEC_TC_DEC_INT128: {
      INIT_SUM_AGGREGATE(VEC_TC_DEC_INT32, VEC_TC_DEC_INT128);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected in & out type class", K(in_tc), K(out_tc));
    }
    }
    break;
  }
  case VEC_TC_DEC_INT64: { // int64
    switch(out_tc) {
    case VEC_TC_NUMBER: {
      INIT_SUM_AGGREGATE_TMP_STORE(VEC_TC_DEC_INT64, VEC_TC_NUMBER, VEC_TC_DEC_INT64);
      break;
    }
    case VEC_TC_DEC_INT128: {
      INIT_SUM_AGGREGATE(VEC_TC_DEC_INT64, VEC_TC_DEC_INT128);
      break;
    }
    case VEC_TC_DEC_INT256: {
      INIT_SUM_AGGREGATE(VEC_TC_DEC_INT64, VEC_TC_DEC_INT256);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected in & out type class", K(in_tc), K(out_tc));
    }
    }
    break;
  }
  case VEC_TC_DEC_INT128: { // int128
    switch(out_tc) {
    case VEC_TC_NUMBER: {
      INIT_SUM_AGGREGATE_TMP_STORE(VEC_TC_DEC_INT128, VEC_TC_NUMBER, VEC_TC_DEC_INT128);
      break;
    }
    case VEC_TC_DEC_INT128: {
      INIT_SUM_AGGREGATE(VEC_TC_DEC_INT128, VEC_TC_DEC_INT128);
      break;
    }
    case VEC_TC_DEC_INT256: {
      INIT_SUM_AGGREGATE(VEC_TC_DEC_INT128, VEC_TC_DEC_INT256);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected in & out type class", K(in_tc), K(out_tc));
    }
    }
    break;
  }
  case VEC_TC_DEC_INT256: { // int256
    switch(out_tc) {
    case VEC_TC_NUMBER: {
      INIT_SUM_AGGREGATE_TMP_STORE(VEC_TC_DEC_INT256, VEC_TC_NUMBER, VEC_TC_DEC_INT256);
      break;
    }
    case VEC_TC_DEC_INT256: {
      INIT_SUM_AGGREGATE(VEC_TC_DEC_INT256, VEC_TC_DEC_INT256);
      break;
    }
    case VEC_TC_DEC_INT512: {
      INIT_SUM_AGGREGATE(VEC_TC_DEC_INT256, VEC_TC_DEC_INT512);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected in & out type class", K(in_tc), K(out_tc));
    }
    }
    break;
  }
  case VEC_TC_DEC_INT512: { // int512
    switch(out_tc) {
    case VEC_TC_NUMBER: {
      INIT_SUM_AGGREGATE_TMP_STORE(VEC_TC_DEC_INT512 , VEC_TC_NUMBER, VEC_TC_DEC_INT512);
      break;
    }
    case VEC_TC_DEC_INT512: {
      INIT_SUM_AGGREGATE(VEC_TC_DEC_INT512, VEC_TC_DEC_INT512);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected in & out type class", K(in_tc), K(out_tc));
    }
    }
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "unexpected in & out type class", K(in_tc), K(out_tc));
  }
  }
  return ret;
#undef INIT_SUM_AGGREGATE
}

int init_count_sum_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                             ObIAllocator &allocator, IAggregate *&agg)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    return init_agg_func<SumNumberAggregate>(
      agg_ctx, agg_col_id, agg_ctx.aggr_infos_.at(agg_col_id).has_distinct_, allocator, agg);
  } else {
    return init_agg_func<SumAggregate<VEC_TC_INTEGER, VEC_TC_INTEGER>>(
      agg_ctx, agg_col_id, agg_ctx.aggr_infos_.at(agg_col_id).has_distinct_, allocator, agg);
  }
}
} // end helper
} // end aggregate
} // end share
} // end oceanbase