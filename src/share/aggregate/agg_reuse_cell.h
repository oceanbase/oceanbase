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

#ifndef OCEANBASE_AGG_REUSE_CELL_H_
#define OCEANBASE_AGG_REUSE_CELL_H_

#include "share/aggregate/agg_ctx.h"
#include "share/aggregate/util.h"
#include "share/aggregate/approx_count_distinct.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
template<ObExprOperatorType agg_func>
struct ReuseAggCell
{
  static int save(const RuntimeContext &agg_ctx, const int64_t agg_col_id, const char *agg_row, void *store_v)
  {
    return OB_SUCCESS;
  }
  static int restore(const RuntimeContext &agg_ctx, const int64_t agg_col_id, char *agg_row, void *store_v)
  {
    return OB_SUCCESS;
  }
  static int64_t stored_size() { return 0; }
};

template<>
struct ReuseAggCell<T_FUN_MAX>
{
  static int save(const RuntimeContext &agg_ctx, const int64_t agg_col_id, const char *agg_row, void *store_v)
  {
    int ret = OB_SUCCESS;
    VecValueTypeClass vec_tc = agg_ctx.aggr_infos_.at(agg_col_id).expr_->get_vec_value_tc();
    if (helper::is_var_len_agg_cell(vec_tc)) {
      StoredValue *store = reinterpret_cast<StoredValue *>(store_v);
      const char *agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_row);
      store->buf_ = *reinterpret_cast<const char * const *>(agg_cell + sizeof(int32_t) + sizeof(char *));
      store->buf_len_ = *reinterpret_cast<const int32_t *>(agg_cell + sizeof(int32_t) + sizeof(char *) * 2);
    }
    return ret;
  }

  static int restore(const RuntimeContext &agg_ctx, const int64_t agg_col_id, char *agg_row, void *store_v)
  {
    int ret =  OB_SUCCESS;
    VecValueTypeClass vec_tc = agg_ctx.aggr_infos_.at(agg_col_id).expr_->get_vec_value_tc();
    if (helper::is_var_len_agg_cell(vec_tc)) {
      const StoredValue *store = reinterpret_cast<const StoredValue *>(store_v);
      char *agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_row);
      *reinterpret_cast<const char **>(agg_cell + sizeof(int32_t) + sizeof(char *)) = store->buf_;
      *reinterpret_cast<int32_t *>(agg_cell + sizeof(int32_t) + sizeof(char *) * 2) = store->buf_len_;
    }
    return ret;
  }

  static int64_t stored_size()
  {
    return sizeof(StoredValue);
  }
private:
  struct StoredValue
  {
    StoredValue(): buf_(nullptr), buf_len_(0) {}
    const char* buf_;
    int32_t buf_len_;
  };
};

template<>
struct ReuseAggCell<T_FUN_APPROX_COUNT_DISTINCT>
{
  static int save(const RuntimeContext &agg_ctx, const int64_t agg_col_id, const char *agg_row, void *store_v)
  {
    int ret = OB_SUCCESS;
    StoredValue *store = reinterpret_cast<StoredValue *>(store_v);
    const char *agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_row);
    int cell_len = agg_ctx.row_meta().get_cell_len(agg_col_id, agg_row);
    store->bucket_buf_ = *reinterpret_cast<const char * const *>(agg_cell + cell_len);
    return ret;
  }
  static int restore(const RuntimeContext &agg_ctx, const int64_t agg_col_id, char *agg_row, void *store_v)
  {
    int ret = OB_SUCCESS;
    constexpr int64_t llc_num_buckets =
      ApproxCountDistinct<T_FUN_APPROX_COUNT_DISTINCT, VEC_TC_INTEGER,
                          VEC_TC_INTEGER>::LLC_NUM_BUCKETS;
    StoredValue *store = reinterpret_cast<StoredValue *>(store_v);
    char *agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_row);
    int cell_len = agg_ctx.row_meta().get_cell_len(agg_col_id, agg_row);
    *reinterpret_cast<const char **>(agg_cell + cell_len) = store->bucket_buf_;
    NotNullBitVector &not_nulls = agg_ctx.row_meta().locate_notnulls_bitmap(agg_row);
    if (store->bucket_buf_ != nullptr) {
      MEMSET(const_cast<char *>(store->bucket_buf_), 0, llc_num_buckets);
    }
    return ret;
  }
  static int64_t stored_size() { return sizeof(StoredValue); }
private:
  struct StoredValue
  {
    StoredValue(): bucket_buf_(nullptr) {}
    const char *bucket_buf_;
  };
};

template<>
struct ReuseAggCell<T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS>
{
  static int save(const RuntimeContext &agg_ctx, const int64_t agg_col_id, const char *agg_row, void *store_v)
  {
    int ret = OB_SUCCESS;
    const char *agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_row);
    StoredValue *store = reinterpret_cast<StoredValue *>(store_v);
    store->res_buf_ = *reinterpret_cast<const char * const *>(agg_cell);
    return ret;
  }
  static int restore(const RuntimeContext &agg_ctx, const int64_t agg_col_id, char *agg_row, void *store_v)
  {
    constexpr int64_t llc_num_buckets =
      ApproxCountDistinct<T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS, VEC_TC_INTEGER,
                          VEC_TC_STRING>::LLC_NUM_BUCKETS;
    int ret = OB_SUCCESS;
    char *agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_row);
    StoredValue *store = reinterpret_cast<StoredValue *>(store_v);
    *reinterpret_cast<const char **>(agg_cell) = store->res_buf_;
    NotNullBitVector &not_nulls = agg_ctx.row_meta().locate_notnulls_bitmap(agg_row);
    if (store->res_buf_ != nullptr) {
      MEMSET(const_cast<char *>(store->res_buf_), 0, llc_num_buckets);
    }
    return ret;
  }

  static int64_t stored_size() { return sizeof(StoredValue); }
private:
  struct StoredValue
  {
    StoredValue(): res_buf_(nullptr) {}
    const char *res_buf_;
  };
};

template<>
struct ReuseAggCell<T_FUN_GROUP_CONCAT>
{
  static int save(const RuntimeContext &agg_ctx, const int64_t agg_col_id, const char *agg_row, void *store_v)
  {
    int ret = OB_SUCCESS;
    StoredValue *store = reinterpret_cast<StoredValue *>(store_v);
    const char *agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_row);
    int32_t str_buf_size = *reinterpret_cast<const int32_t *>(agg_cell + sizeof(char **));
    store->str_buf_ = *reinterpret_cast<const char * const*>(agg_cell);
    store->str_buf_size_ = str_buf_size;
    return ret;
  }
  static int restore(const RuntimeContext &agg_ctx, const int64_t agg_col_id, char *agg_row, void *store_v)
  {
    int ret = OB_SUCCESS;
    StoredValue *store = reinterpret_cast<StoredValue *>(store_v);
    char *agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_row);
    *reinterpret_cast<int32_t *>(agg_cell + sizeof(char **)) = store->str_buf_size_;
    *reinterpret_cast<const char **>(agg_cell) = store->str_buf_;
    return ret;
  }
  static int64_t stored_size() { return sizeof(StoredValue); }
private:
  struct StoredValue
  {
    StoredValue(): str_buf_(nullptr), str_buf_size_(0) {}
    const char *str_buf_;
    int32_t str_buf_size_;
  };
};

struct ReuseAggCellMgr
{
  ReuseAggCellMgr(ObIAllocator &allocator, int32_t agg_cnt) :
    tmp_store_vals_(allocator, agg_cnt), allocator_(allocator), extra_store_idx_(-1)
  {}
  int init(RuntimeContext &agg_ctx);
  int save(RuntimeContext &agg_ctx, const char *agg_row);
  int restore(RuntimeContext &agg_ctx, char *agg_row);

private:
  int save_extra_stores(RuntimeContext &agg_ctx, const char *agg_row);
  int restore_extra_stores(RuntimeContext &agg_ctx, char *agg_row);

private:
  ObFixedArray<void *, ObIAllocator> tmp_store_vals_;
  ObIAllocator &allocator_;
  int32_t extra_store_idx_;
};
}
}
}
#endif // OCEANBASE_AGG_REUSE_CELL_H_