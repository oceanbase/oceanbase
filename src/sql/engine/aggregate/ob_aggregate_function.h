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

#ifndef _OB_AGGREGATE_FUNCTION_H
#define _OB_AGGREGATE_FUNCTION_H
#include "lib/string/ob_sql_string.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_vector.h"
#include "lib/list/ob_dlist.h"
#include "lib/hash/ob_hashset.h"
#include "common/row/ob_row_store.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "lib/container/ob_fixed_array.h"
#include "sql/engine/sort/ob_sort_impl.h"

namespace oceanbase {
namespace sql {
typedef common::ObDList<ObSqlExpression> ObAggrExprList;

class ObAggregateDistinctItem {
public:
  ObAggregateDistinctItem()
      : group_id_(0), col_idx_(0), cs_type_(common::CS_TYPE_INVALID), cells_(NULL), cs_type_list_(NULL)
  {}

  uint64_t hash() const;
  bool operator==(const ObAggregateDistinctItem& other) const;
  inline bool operator!=(const ObAggregateDistinctItem& other) const
  {
    return !operator==(other);
  }
  TO_STRING_KV(K_(group_id), K_(col_idx), KP_(cells), K_(cs_type_list));
  int64_t group_id_;
  int64_t col_idx_;
  common::ObCollationType cs_type_;
  // size of %cells_ is $cs_type_list_->count()
  common::ObObj* cells_;
  const common::ObIArray<common::ObCollationType>* cs_type_list_;
};

// Context structure for one aggregation function of one group, only some functions need this:
//  with distinct: need this for distinct calculate
//  group concat:  need context to hold the input cells.
//  rank: need context to hold the input cells.
//  percent_rank: need context to hold the input cells.
//  dense_rank: need context to hold the input cells.
//  cume_dist: need context to hold the input cells.
//  max keep(): need context to hold the input cells.
//  min keep(): need context to hold the input cells.
//  sum keep(): need context to hold the input cells.
//  count keep(): need context to hold the input cells.
//
//
// We only implement distinct logic here, no common interface abstracted here,
// use the derived class directly.
class ObAggCellCtx {
public:
  // %alloc is used to initialize the structures, can not be used to hold the data
  explicit ObAggCellCtx(common::ObIAllocator& alloc);
  virtual ~ObAggCellCtx();

  virtual void reuse();

public:
  // for distinct calculate may be replace by hash based distinct in the future.
  ObUniqueSort* distinct_set_;
  common::ObArrayHelper<ObSortColumn> sort_columns_;

  int init_distinct_set(const uint64_t tenant_id, const common::ObIArray<common::ObCollationType>& cs_types,
      const int64_t sort_col_cnt, const bool need_rewind);

protected:
  common::ObIAllocator& alloc_;
};

class ObGroupConcatRowStore {
public:
  ObGroupConcatRowStore();
  ~ObGroupConcatRowStore();

  int init(const uint64_t tenant_id, const common::ObIArray<ObSortColumn>& sort_columns,
      const ObSortImpl::SortExtraInfos* extra_infos, const bool rewind);
  void reuse();

  int add_row(const common::ObNewRow& row)
  {
    rows_++;
    return need_sort_ ? sort_.add_row(row) : rs_.add_row(row);
  }

  int get_next_row(const common::ObNewRow*& row)
  {
    int ret = need_sort_ ? sort_.get_next_row(row) : rs_it_.get_next_row(*const_cast<common::ObNewRow**>(&row));
    if (OB_SUCCESS == ret) {
      iter_idx_++;
    }
    return ret;
  }

  int finish_add_row();

  // iterate twice with rollup process, rewind is needed.
  int rewind();
  bool is_last_row() const
  {
    return iter_idx_ == rows_;
  }
  bool empty() const
  {
    return 0 == rows_;
  }
  bool is_iterated() const
  {
    return iter_idx_ > 0;
  }
  int get_row_count() const
  {
    return rows_;
  }
  int get_sort_columns(common::ObIArray<ObSortColumn>& sort_columns)
  {
    return sort_.get_sort_columns(sort_columns);
  }
  bool is_allocate_sort() const
  {
    return need_sort_;
  }

private:
  bool need_sort_;
  int64_t rows_;
  int64_t iter_idx_;

  ObChunkRowStore rs_;
  ObChunkRowStore::Iterator rs_it_;
  ObSortImpl sort_;
};

class ObGroupConcatCtx : public ObAggCellCtx {
public:
  ObGroupConcatCtx(common::ObIAllocator& alloc) : ObAggCellCtx(alloc), gc_rs_(NULL)
  {}

  virtual ~ObGroupConcatCtx();

  virtual void reuse() override;

public:
  ObGroupConcatRowStore* gc_rs_;
};

enum {
  // processing row from lower op uses memory allocated from calc_buf of this level (Group by),
  // we need to put a limit to it, otherwise too much memory can be wasted. .
  AGGR_CALC_BUF_LIMIT = 2 * common::OB_MALLOC_NORMAL_BLOCK_SIZE
};

class ObAggregateFunction {
public:
  struct GroupRow {
    GroupRow() : row_(NULL), ctx_(NULL)
    {}
    common::ObRowStore::StoredRow* row_;
    ObAggCellCtx** ctx_;

    TO_STRING_KV(K(row_), KP(ctx_));
  };

  struct ExprCtxIdx {
    ExprCtxIdx() : expr_(NULL), ctx_idx_(-1)
    {}
    const ObAggregateExpression* expr_;
    int64_t ctx_idx_;

    TO_STRING_KV(K(expr_), K(ctx_idx_));
  };

public:
  ObAggregateFunction();
  ~ObAggregateFunction();
  void set_int_div_as_double(bool did);
  bool get_int_div_as_double() const;
  void set_sort_based_gby()
  {
    is_sort_based_gby_ = true;
  }
  void set_in_window_func()
  {
    set_sort_based_gby();
    in_window_func_ = true;
  }

  int init(const int64_t input_column_count, const ObAggrExprList* aggr_columns, common::ObExprCtx& expr_ctx,
      int32_t prepare_row_num, int64_t distinct_set_bucket_num);
  int init_agg_udf(const ObIArray<ObAggUdfMeta>& udf_metas);
  void destroy();
  void reuse();
  int get_cur_row(const common::ObRowStore::StoredRow*& stored_row, const int64_t group_id = 0) const;
  inline int64_t get_output_column_cnt() const
  {
    return output_column_count_;
  }
  int prepare(
      const common::ObNewRow& row, const int64_t group_id = 0, common::ObRowStore::StoredRow** output_row = NULL);
  int process(const common::ObNewRow& row, const common::ObTimeZoneInfo* tz_info, const int64_t group_id = 0);
  int get_result(common::ObNewRow& row, const common::ObTimeZoneInfo* tz_info, const int64_t group_id = 0);
  // used by ScalarAggregate operator when there's no input rows
  int get_result_for_empty_set(common::ObNewRow& row);
  int64_t get_used_mem_size() const;
  int64_t get_hold_mem_size() const;
  // added by wangguoping to support rollup
  // these functions are only called by merge group by
  int rollup_init(const int64_t num_group_col);
  int rollup_process(const common::ObTimeZoneInfo* tz_info, const int64_t group_id1, const int64_t group_id2,
      const int64_t diff_col_idx, bool set_grouping = false);
  int reuse_group(const int64_t group_id);
  int init_first_rollup_cols(
      common::ObIAllocator* alloc, const ObIArray<ObColumnInfo>& group_idxs, const ObIArray<ObColumnInfo>& rollup_idxs);
  void set_tenant_id(const uint64_t tenant_id)
  {
    stored_row_buf_.set_tenant_id(tenant_id);
    agg_cell_ctx_alloc_.set_tenant_id(tenant_id);
  }

  ObArenaAllocator& get_stored_row_buf()
  {
    return stored_row_buf_;
  }

  bool has_distinct() const
  {
    return has_distinct_;
  }
  bool has_sort() const
  {
    return has_sort_;
  }
  int check_rows_equal(const ObAggregateExpression* cexpr, common::ObIArray<ObSortColumn>& sort_columns,
      const ObNewRow* prev_row, const ObNewRow* curr_row, bool& is_equal);
  int deep_copy_cur_row(ObNewRow*& prev_row, const ObNewRow cur_row);
  int compare_calc(ObObj& obj1, ObObj& obj2, const ObOpSchemaObj& extra_info, int32_t& compare_result, bool& is_asc);

private:
  inline ObAggCellCtx* get_agg_cell_ctx(const int64_t group_id, const int64_t cell_idx)
  {
    return cell_idx >= 0 ? row_array_.at(group_id).ctx_[cell_idx] : NULL;
  }
  // function members
  int init_aggr_cell(const ObItemType aggr_fun, const common::ObNewRow& oprands, common::ObObj& res1,
      common::ObObj* res2, ObAggCellCtx* agg_cell_ctx, common::ObCollationType cs_type, int64_t agg_udf_id,
      ObAggUdfMeta* agg_udf_meta);
  int calc_aggr_cell(const ObItemType aggr_fun, const common::ObNewRow& oprands, common::ObObj& res1,
      common::ObObj* res2, const common::ObTimeZoneInfo* tz_info, common::ObCollationType cs_type,
      ObAggCellCtx* cell_ctx, int64_t offset = 0);
  int calc_distinct_item(const ObItemType aggr_fun, const common::ObNewRow& input_row,
      const common::ObTimeZoneInfo* tz_info, const ObAggregateExpression* cexpr, ObAggregateDistinctItem& distinct_item,
      common::ObObj& res1, common::ObObj* res2, ObAggCellCtx* cell_ctx, const bool should_fill_item = true,
      const bool should_calc = true);
  int aggr_distinct_cell(const ObItemType aggr_fun, ObAggCellCtx* cell_ctx, common::ObObj& res1, common::ObObj* res2,
      const common::ObTimeZoneInfo* tz_info, common::ObCollationType cs_type, int64_t agg_udf_id,
      ObAggUdfMeta* agg_udf_meta);
  int clone_cell(const common::ObObj& src_cell, common::ObObj& target_cell);
  int clone_number_cell(const common::ObObj& src_cell, common::ObObj& target_cell);
  int fill_distinct_item_cell_list(
      const ObAggregateExpression* cexpr, const common::ObNewRow& input_row, ObAggregateDistinctItem& distinct_item);
  int max_calc(common::ObObj& base, const common::ObObj& other, common::ObCollationType cs_type);
  int min_calc(common::ObObj& base, const common::ObObj& other, common::ObCollationType cs_type);
  int add_calc(
      common::ObObj& res, const common::ObObj& left, const common::ObObj& right, const common::ObTimeZoneInfo* tz_info);
  int linear_inter_calc(common::ObObj& res, const common::ObObj& left, const common::ObObj& right,
      const common::number::ObNumber& factor, const int16_t scale);
  int get_percentile_param(const ObItemType aggr_fun, const common::ObObj& param_obj, const int64_t not_null_start_loc,
      const int64_t total_row_count, int64_t& dest_loc, bool& need_linear_inter, common::number::ObNumber& factor,
      common::ObDataBuffer& allocator);
  // added by wangguoping to support rollup
  int get_stored_row(const int64_t group_id, common::ObRowStore::StoredRow*& stored_row);
  int rollup_aggregation(const ObItemType aggr_fun, const bool is_first_rollup, const common::ObTimeZoneInfo* tz_info,
      common::ObCollationType cs_type, common::ObObj& res1, common::ObObj& res2, common::ObObj& aux_res1,
      common::ObObj& aux_res2, ObGroupConcatRowStore* group_concat_row_store1,
      ObGroupConcatRowStore* group_concat_row_store2, bool set_grouping);
  int init_one_group(const int64_t group_id);

  // HyperLogLogCount-related functions
  int llc_init(common::ObObj* llc_bitmap);
  static uint64_t llc_calc_hash_value(
      const common::ObNewRow& oprands, common::ObCollationType cs_type, bool& has_null_cell);
  static int llc_add_value(const uint64_t value, common::ObObj* llc_bitmap);
  static int llc_add(common::ObObj& res, const common::ObObj& left, const common::ObObj& right);
  int get_wm_concat_result(
      const ObAggregateExpression*& cexpr, ObGroupConcatCtx*& cell_ctx, bool is_keep_group_concat, ObObj& concat_obj);

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObAggregateFunction);
  typedef common::hash::ObHashSet<ObAggregateDistinctItem, common::hash::NoPthreadDefendMode> AggrDistinctBucket;
  typedef common::ObSEArray<GroupRow, 1> RowArray;
  const static int64_t CONCAT_STR_BUF_LEN = common::OB_MAX_VARCHAR_LENGTH;
  const static int64_t STORED_ROW_MAGIC_NUM = 0xaaaabbbbccccdddd;

private:
  // data members
  bool has_distinct_;
  bool has_sort_;
  bool is_sort_based_gby_;
  bool in_window_func_;
  bool aggr_fun_need_cell_ctx_;
  ObSEArray<ExprCtxIdx, 4> agg_columns_;
  int64_t agg_cell_ctx_cnt_;
  int64_t output_column_count_;
  int64_t full_column_count_;
  bool did_int_div_as_double_;
  common::ObArenaAllocator stored_row_buf_;
  common::ObExprCtx expr_ctx_;
  int64_t child_column_count_;
  AggrDistinctBucket aggr_distinct_set_;
  RowArray row_array_;
  char concat_str_buf_[CONCAT_STR_BUF_LEN];
  // allocator for aggregation cell context
  common::ObArenaAllocator agg_cell_ctx_alloc_;
  uint64_t group_concat_max_len_;
  // common allocator, can not be reset in reuse() or reset()
  common::ObArenaAllocator alloc_;
  common::ObObj* input_cells_;
  int64_t gconcat_cur_row_num_;  // for log warning, the rows which are ignored is exclude.

  // HyperLogLogCount-related data members
  static const int8_t LLC_BUCKET_BITS = 10;
  static const int64_t LLC_NUM_BUCKETS = (1 << LLC_BUCKET_BITS);

  common::ObFixedArray<bool, common::ObIAllocator> first_rollup_cols_;

  common::ObArenaAllocator agg_udf_buf_;
  common::ObSEArray<ObAggUdfMeta, 16> agg_udf_metas_;
  common::hash::ObHashMap<int64_t, ObAggUdfExeUnit, common::hash::NoPthreadDefendMode> agg_udf_;
};

inline void ObAggregateFunction::set_int_div_as_double(bool did)
{
  did_int_div_as_double_ = did;
}

inline bool ObAggregateFunction::get_int_div_as_double() const
{
  return did_int_div_as_double_;
}

inline int ObAggregateFunction::get_cur_row(
    const common::ObRowStore::StoredRow*& stored_row, const int64_t group_id) const
{
  int ret = common::OB_SUCCESS;
  GroupRow tmp;
  ret = row_array_.at(group_id, tmp);
  stored_row = tmp.row_;
  return ret;
}

inline int64_t ObAggregateFunction::get_hold_mem_size() const
{
  return stored_row_buf_.total() + agg_cell_ctx_alloc_.total() + alloc_.total() + agg_udf_buf_.total();
}

inline int64_t ObAggregateFunction::get_used_mem_size() const
{
  return stored_row_buf_.used() + agg_cell_ctx_alloc_.used() + alloc_.used() + agg_udf_buf_.used();
}
}  // end namespace sql
}  // end namespace oceanbase

#endif /* _OB_AGGREGATE_FUNCTION_H */
