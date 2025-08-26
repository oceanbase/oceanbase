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

#ifndef  OCEANBASE_SHARE_RB_AGGREGATE_H_
#define  OCEANBASE_SHARE_RB_AGGREGATE_H_

#include "share/aggregate/iaggregate.h"
#include "lib/roaringbitmap/ob_rb_utils.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{

template<ObItemType func_type, VecValueTypeClass in_tc, VecValueTypeClass out_tc>
class RbAggregate final: public BatchAggregateWrapper<RbAggregate<func_type, in_tc, out_tc>>
{

public:
  static const constexpr VecValueTypeClass IN_TC = in_tc;
  static const constexpr VecValueTypeClass OUT_TC = out_tc;

public:
  RbAggregate() {}

  inline int get_rb(RuntimeContext &agg_ctx, int32_t agg_col_id, char *agg_cell, bool create_if_not_exsit, ObRbAggCell *&ret_ptr)
  {
    int ret = OB_SUCCESS;
    ObRbAggAllocator *rb_allocator = nullptr;
    ObRbAggCell *rb = reinterpret_cast<ObRbAggCell*>(EXTRACT_MEM_ADDR(agg_cell));
    if (OB_ISNULL(rb) && create_if_not_exsit) {
      if (OB_ISNULL(rb_allocator = agg_ctx.get_rb_allocator())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "get rb allocator fail", K(ret));
      } else if (OB_ISNULL(rb = rb_allocator->alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "allocate memory failed", K(ret));
      } else {
        STORE_MEM_ADDR(rb, (agg_cell));
      }
    }
    if (OB_SUCC(ret)) {
      ret_ptr = rb;
    }
    return ret;
  }

  template <typename ColumnFormat>
  int collect_group_result(RuntimeContext &agg_ctx, const ObExpr &agg_expr,
                           const int32_t agg_col_id, const char *agg_cell, const int32_t agg_cell_len)
  {
    int ret = OB_SUCCESS;
    int64_t output_idx = agg_ctx.eval_ctx_.get_batch_idx();
    ColumnFormat *res_vec = static_cast<ColumnFormat *>(agg_expr.get_vector(agg_ctx.eval_ctx_));
    ObString rb_bin;
    ObRbAggCell *rb = nullptr;
    ObRbAggAllocator *rb_allocator = nullptr;
    if (OB_LIKELY(! agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell).at(agg_col_id))) {
      res_vec->set_null(output_idx);
    } else if (OB_FAIL(get_rb(agg_ctx, agg_col_id, const_cast<char *>(agg_cell), false/*create_if_not_exist*/, rb) )) {
      SQL_LOG(WARN, "failed to get roaringbitmap", K(ret), K(agg_col_id), KP(agg_cell));
    } else if (OB_ISNULL(rb_allocator = agg_ctx.get_rb_allocator())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "get rb allocator fail", K(ret));
    } else if (OB_FAIL(rb_allocator->rb_serialize(rb_bin, rb))) {
      SQL_LOG(WARN, "failed to serialize roaringbitmap", K(ret));
    } else {
      ObString blob_locator;
      ObExprStrResAlloc expr_res_alloc(agg_expr, agg_ctx.eval_ctx_);
      ObTextStringResult blob_res(ObRoaringBitmapType, true, &expr_res_alloc);
      int64_t total_length = rb_bin.length();
      if (OB_FAIL(blob_res.init(total_length))) {
        SQL_LOG(WARN, "failed to init blob res", K(ret), K(rb_bin), K(total_length));
      } else if (OB_FAIL(blob_res.append(rb_bin))) {
        SQL_LOG(WARN, "failed to append roaringbitmap binary data", K(ret), K(rb_bin));
      } else {
        blob_res.get_result_buffer(blob_locator);
        res_vec->set_payload_shallow(output_idx, blob_locator.ptr(), blob_locator.length());
      }
    }
    // destory because threse are 3rd libary memory need to free
    if (OB_NOT_NULL(rb)) {
      rb_allocator->free(rb);
      STORE_MEM_ADDR(NULL, (const_cast<char*>(agg_cell)));
    }
    return ret;
  }

  inline int get_value(const char* data, int32_t data_len, uint64_t &val)
  {
    int ret = OB_SUCCESS;
    ObDatum datum;
    datum.ptr_ = data;
    datum.len_ = data_len;
    if (VEC_TC_UINTEGER == in_tc) {
      val = datum.get_uint64();
    } else if (VEC_TC_INTEGER == in_tc)  {
      int64_t val_64 = datum.get_int();
      if (val_64 < INT32_MIN) {
        ret = OB_SIZE_OVERFLOW;
        SQL_LOG(WARN, "negative integer not in the range of int32", K(ret), K(val_64));
      } else if (val_64 < 0) {
        // convert negative integer to uint32
        uint32_t val_u32 = static_cast<uint32_t>(val_64);
        val = static_cast<uint64_t>(val_u32);
      } else {
        val = static_cast<uint64_t>(val_64);
      }
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
      SQL_LOG(WARN, "invalid data type for roaringbitmap build agg", K(ret), K(in_tc));
    }
    return ret;
  }

  inline int add_value(RuntimeContext &agg_ctx, int32_t agg_col_id, char *agg_cell, const char* data, int32_t data_len)
  {
    int ret = OB_SUCCESS;
    ObRbAggCell *rb = nullptr;
    ObArenaAllocator tmp_alloc;
    if (OB_FAIL(get_rb(agg_ctx, agg_col_id, agg_cell, true/*create_if_not_exist*/, rb))) {
      SQL_LOG(WARN, "failed to get roaringbitmap", K(ret), K(agg_col_id), KP(agg_cell));
    } else if (func_type == T_FUN_SYS_RB_BUILD_AGG) {
      uint64_t val = 0;
      if (OB_FAIL(get_value(data, data_len, val))) {
        SQL_LOG(WARN, "get value fail", K(ret), K(data_len), KP(data));
      } else if (OB_FAIL(rb->value_add(val))) {
        SQL_LOG(WARN, "failed to add value to roaringbitmap", K(ret), K(val));
      }
    } else {
      // T_FUN_SYS_RB_OR_AGG or T_FUN_SYS_RB_AND_AGG
      ObString value_rb_bin(data_len, data);
      if (in_tc == VEC_TC_ROARINGBITMAP && OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_alloc,
                                                                  ObRoaringBitmapType,
                                                                  CS_TYPE_BINARY,
                                                                  true,
                                                                  value_rb_bin))) {
        SQL_LOG(WARN, "fail to get real data.", K(ret), K(value_rb_bin));
      } else if (OB_FAIL(rb->value_calc(value_rb_bin, func_type, in_tc != VEC_TC_ROARINGBITMAP))) {
        SQL_LOG(WARN, "failed to add value to roaringbitmap", K(ret));
      }
    }
    return ret;
  }

  inline int add_one_row(RuntimeContext &agg_ctx, int64_t row_num, int64_t batch_size,
                         const bool is_null, const char *data, const int32_t data_len,
                         int32_t agg_col_idx, char *agg_cell) override
  {
    int ret = OB_SUCCESS;
    if (! is_null) {
      if (OB_FAIL(add_value(agg_ctx, agg_col_idx, agg_cell, data, data_len))) {
        SQL_LOG(WARN, "add value fail", K(ret), K(agg_col_idx), KP(agg_cell), KP(data), K(data_len));
      } else {
        agg_ctx.locate_notnulls_bitmap(agg_col_idx, agg_cell).set(agg_col_idx);
      }
    }
    return ret;
  }

  template <typename ColumnFmt>
  OB_INLINE int add_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                     const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    // add_row ensure data not null, so here don't need check column is null
    // and caller will set not null, so here don't need set again.
    const char *payload = nullptr;
    int32_t len = 0;
    columns.get_payload(row_num, payload, len);
    if (OB_FAIL(add_value(agg_ctx, agg_col_id, agg_cell, payload, len))) {
      SQL_LOG(WARN, "add value fail", K(ret), K(agg_col_id), KP(agg_cell), KP(payload), K(len));
    }
    return ret;
  }

  template <typename ColumnFmt>
  OB_INLINE int add_nullable_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                              const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    if (columns.is_null(row_num)) {
      // ignore null
    } else {
      const char *payload = nullptr;
      int32_t len = 0;
      columns.get_payload(row_num, payload, len);
      if (OB_FAIL(add_value(agg_ctx, agg_col_id, agg_cell, payload, len))) {
        SQL_LOG(WARN, "add value fail", K(ret), K(agg_col_id), KP(agg_cell), KP(payload), K(len));
      } else {
        agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell).set(agg_col_id);
      }
    }
    return ret;
  }

  virtual int rollup_aggregation(RuntimeContext &agg_ctx, const int32_t agg_col_idx,
                                 AggrRowPtr group_row, AggrRowPtr rollup_row,
                                 int64_t cur_rollup_group_idx,
                                 int64_t max_group_cnt = INT64_MIN) override
  {
    UNUSEDx(cur_rollup_group_idx, max_group_cnt);
    int ret = OB_SUCCESS;
    char *curr_agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_idx, group_row);
    char *rollup_agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_idx, rollup_row);
    ObRbAggCell *curr_rb = nullptr;
    ObRbAggCell *rollup_rb = nullptr;
    const NotNullBitVector &curr_not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, curr_agg_cell);
    if (OB_UNLIKELY(! curr_not_nulls.at(agg_col_idx))) {
      // do noting, keep null
    } else if (OB_FAIL(get_rb(agg_ctx, agg_col_idx, const_cast<char *>(curr_agg_cell), false/*create_if_not_exist*/, curr_rb))) {
      SQL_LOG(WARN, "cur rb is null", K(ret), KP(curr_rb), KP(rollup_rb));
    } else if (OB_FAIL(get_rb(agg_ctx, agg_col_idx, const_cast<char *>(rollup_agg_cell), true/*create_if_not_exist*/, rollup_rb))) {
      SQL_LOG(WARN, "rollup rb is null", K(ret), KP(curr_rb), KP(rollup_rb));
    } else if (OB_FAIL(rollup_rb->rollup(curr_rb, func_type))) {
      SQL_LOG(WARN, "rb rollup fail", K(ret), KP(curr_rb), KP(rollup_rb));
    } else {
      agg_ctx.locate_notnulls_bitmap(agg_col_idx, rollup_agg_cell).set(agg_col_idx);
    }
    return ret;
  }

  int eval_group_extra_result(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                              const int32_t cur_group_id) override
  {
    int ret = OB_SUCCESS;
    ObRbAggAllocator *rb_allocator = nullptr;
    char *agg_cell = nullptr;
    int32_t agg_cell_len = 0;
    agg_ctx.get_agg_payload(agg_col_id, cur_group_id, agg_cell, agg_cell_len);
    ObExpr *agg_expr = agg_ctx.aggr_infos_.at(agg_col_id).expr_;
    ObRbAggCell *curr_rb = nullptr;
    if (OB_ISNULL(agg_expr)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "agg expr is null", K(ret), K(agg_col_id), K(cur_group_id));
    } else if (OB_LIKELY(! agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell).at(agg_col_id))) {
      // skip null
    } else if (OB_FAIL(get_rb(agg_ctx, agg_col_id, const_cast<char *>(agg_cell), false/*create_if_not_exist*/, curr_rb))) {
      SQL_LOG(WARN, "cur rb is null", K(ret), KP(curr_rb));
    } else if (OB_ISNULL(rb_allocator = agg_ctx.get_rb_allocator())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "get rb allocator fail", K(ret));
    } else if (OB_FAIL(rb_allocator->rb_serialize(curr_rb))) {
      SQL_LOG(WARN, "failed to serialize roaringbitmap", K(ret));
    }
    return ret;
  }

  TO_STRING_KV("aggregate", "rb_agg");

};

template <ObItemType func_type>
inline int init_rb_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                             ObIAllocator &allocator, IAggregate *&agg)
{
#define INIT_AGGREGATE_CASE(func_type, in_tc)                                                     \
  case (in_tc): {                                                                                 \
    ret = helper::init_agg_func<RbAggregate<func_type, in_tc, VEC_TC_ROARINGBITMAP>>(agg_ctx,     \
      agg_col_id, aggr_info.has_distinct_, allocator, agg);                                       \
    if (OB_FAIL(ret)) {                                                                           \
      SQL_LOG(WARN, "init rb aggregate failed", K(ret));                                          \
    }                                                                                             \
    break;                                                                                        \
  }

  int ret = OB_SUCCESS;
  ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
  ObDatumMeta &in_meta = aggr_info.expr_->args_[0]->datum_meta_;
  ObDatumMeta &out_meta = aggr_info.expr_->datum_meta_;
  VecValueTypeClass in_tc = get_vec_value_tc(in_meta.type_, in_meta.scale_, in_meta.precision_);
  VecValueTypeClass out_tc = get_vec_value_tc(out_meta.type_, out_meta.scale_, out_meta.precision_);
  if (out_tc != VEC_TC_ROARINGBITMAP) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    SQL_LOG(WARN, "invalid output type", K(in_tc), K(in_meta), K(out_tc));
  } else if (func_type == T_FUN_SYS_RB_BUILD_AGG) {
    switch (in_tc) {
      INIT_AGGREGATE_CASE(T_FUN_SYS_RB_BUILD_AGG, VEC_TC_NULL)
      INIT_AGGREGATE_CASE(T_FUN_SYS_RB_BUILD_AGG, VEC_TC_INTEGER)
      INIT_AGGREGATE_CASE(T_FUN_SYS_RB_BUILD_AGG, VEC_TC_UINTEGER)
      default: {
        ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
        SQL_LOG(WARN, "invalid input type", K(in_tc), K(in_meta));
      }
    }
  } else if (func_type == T_FUN_SYS_RB_AND_AGG) {
    switch (in_tc) {
      INIT_AGGREGATE_CASE(T_FUN_SYS_RB_AND_AGG, VEC_TC_NULL)
      INIT_AGGREGATE_CASE(T_FUN_SYS_RB_AND_AGG, VEC_TC_STRING)
      INIT_AGGREGATE_CASE(T_FUN_SYS_RB_AND_AGG, VEC_TC_ROARINGBITMAP)
      default: {
        ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
        SQL_LOG(WARN, "invalid input type", K(in_tc), K(in_meta));
      }
    }
  } else if (func_type == T_FUN_SYS_RB_OR_AGG) {
    switch (in_tc) {
      INIT_AGGREGATE_CASE(T_FUN_SYS_RB_OR_AGG, VEC_TC_NULL)
      INIT_AGGREGATE_CASE(T_FUN_SYS_RB_OR_AGG, VEC_TC_STRING)
      INIT_AGGREGATE_CASE(T_FUN_SYS_RB_OR_AGG, VEC_TC_ROARINGBITMAP)
      default: {
        ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
        SQL_LOG(WARN, "invalid input type", K(in_tc), K(in_meta));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "invalid func type", K(ret), K(func_type));
  }

  return ret;

#undef INIT_AGGREGATE_CASE
}

} // end namespace aggregate
} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_RB_AGGREGATE_H_
