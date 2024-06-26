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

#ifndef OCEANBASE_SHARE_AGGREGATE_CTX_H_
#define OCEANBASE_SHARE_AGGREGATE_CTX_H_

#include "sql/engine/aggregate/ob_aggregate_processor.h"
#include "share/aggregate/util.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
using namespace sql;
class AggBitVector;
using AggrRowPtr = char *;
using AggregateExtras = ObAggregateProcessor::ExtraResult **;
// examples of aggregate row:
// with extra idx:
//
//   count(a),        sum(b),              count(distinct(a)),      concat(...),     implicit_first_row                  null bits
//  ---------------------------------------------------------------------------------------------------------------------------------
//  | int64  | <ob_number, int64_tmp_res>|      int64          |  <char *, in32_t> |   <char *, int32>   | int32 (idx) |   bits     |
//  ---------------------------------------------------------------------------------------------------------------------------------
//                                                                                                              |
//                                                                                                              |
//                                               |               ...                   |                        |
//                                               |-------------------------------------|                        |
//                   extra_info array            | <distinct_extra *, concat_extra*>   |<------------------------
//                                               |-------------------------------------|
//                                               |               ...                   |
//
//  no extra idx:
//   count(a),       sum(b),      implicit_first_row     null bits
// ---------------------------------------------------------------
// | int64      |   int128    |   <char *, int32>      |   bits  |
// ---------------------------------------------------------------

struct AggrRowMeta
{
  AggrRowMeta() :
    row_size_(0), col_cnt_(0), extra_cnt_(0), nullbits_offset_(0), extra_idx_offset_(0),
    col_offsets_(nullptr), tmp_res_sizes_(nullptr), extra_idxes_(nullptr), use_var_len_(nullptr)
  {}

  void reset()
  {
    row_size_ = 0;
    col_cnt_ = 0;
    extra_cnt_ = 0;
    nullbits_offset_ = 0;
    col_offsets_ = nullptr;
    tmp_res_sizes_ = nullptr;
    extra_idx_offset_ = 0;
    extra_idxes_ = nullptr;
    use_var_len_ = nullptr;
  }

  inline char *locate_cell_payload(const int32_t col_id, char *row) const
  {
    OB_ASSERT(col_id < col_cnt_);
    OB_ASSERT(col_offsets_ != nullptr);
    int32_t col_offset = col_offsets_[col_id];
    OB_ASSERT(col_offset < row_size_);
    return row + col_offset;
  }
  inline const char *locate_cell_payload(const int32_t col_id, const char *agg_row) const
  {
    return static_cast<const char *>(locate_cell_payload(col_id, const_cast<char *>(agg_row)));
  }
  inline int32_t get_cell_len(const int32_t col_id, const char *row) const
  {
    OB_ASSERT(col_id < col_cnt_);
    OB_ASSERT(col_offsets_ != nullptr);
    OB_ASSERT(tmp_res_sizes_ != nullptr);
    if (use_var_len_ != nullptr && use_var_len_->at(col_id)) {
      return *reinterpret_cast<const int32_t *>(row + col_offsets_[col_id] + sizeof(char *));
    } else {
      return col_offsets_[col_id + 1] - col_offsets_[col_id] - tmp_res_sizes_[col_id];
    }
  }

  inline void locate_cell_payload(const int32_t col_id, char *agg_row, char *&cell,
                                  int32_t &cell_len) const
  {
    OB_ASSERT(col_id < col_cnt_);
    OB_ASSERT(col_offsets_ != nullptr);
    OB_ASSERT(tmp_res_sizes_ != nullptr);
    cell = agg_row + col_offsets_[col_id];
    if (use_var_len_ != nullptr && use_var_len_->at(col_id)) {
      cell_len = *reinterpret_cast<int32_t *>(cell + sizeof(char *));
    } else {
      cell_len = col_offsets_[col_id + 1] - col_offsets_[col_id] - tmp_res_sizes_[col_id];
    }
  }

  inline NotNullBitVector &locate_notnulls_bitmap(char *agg_row) const
  {
    return *reinterpret_cast<NotNullBitVector *>(agg_row + nullbits_offset_);
  }

  inline bool is_var_len(const int32_t col_id) const
  {
    return (use_var_len_ != nullptr && use_var_len_->at(col_id));
  }

  TO_STRING_KV(K_(row_size), K_(col_cnt), K_(extra_cnt), K_(nullbits_offset), K_(extra_idx_offset));
  int32_t row_size_;
  int32_t col_cnt_;
  int32_t extra_cnt_;
  // null bits offset
  int32_t nullbits_offset_;
  int32_t extra_idx_offset_;
  // agg_col memory offset
  int32_t *col_offsets_;
  int32_t *tmp_res_sizes_;
  // idx for extra infos
  int32_t *extra_idxes_;
  // if agg_col memory is described as <char *, int32>, it's marked as var_var_len
  AggBitVector *use_var_len_;
};

struct RowSelector
{
  RowSelector() : selector_array_(nullptr), size_(0)
  {}
  RowSelector(const uint16_t *selector_array, const int32_t size) :
    selector_array_(selector_array), size_(size)
  {}

  bool is_empty() const
  {
    return selector_array_ == nullptr || size_ == 0;
  }
  int32_t index(const int64_t i) const
  {
    OB_ASSERT(i < size_);
    OB_ASSERT(selector_array_ != NULL);
    return selector_array_[i];
  }

  const uint16_t *selector() const
  {
    return selector_array_;
  }

  int32_t size() const
  {
    return size_;
  }

  const uint16_t *selector_array_;
  const int32_t size_;
};

struct RemovalInfo
{
  RemovalInfo() :
    enable_removal_opt_(false), is_max_min_idx_changed_(false), max_min_index_(-1),
    is_inverse_agg_(false), null_cnt_(0)
  {}
  bool enable_removal_opt_;
  bool is_max_min_idx_changed_;
  int64_t max_min_index_; // used to record index of min/max value
  bool is_inverse_agg_; // used to determine which interface is called: add_batch_rows/remove_batch_rows
  int32_t null_cnt_;
  TO_STRING_KV(K_(enable_removal_opt), K_(is_max_min_idx_changed), K_(max_min_index),
               K_(is_inverse_agg), K_(null_cnt));

  void reset()
  {
    enable_removal_opt_ = false;
    max_min_index_ = -1;
    is_inverse_agg_ = false;
    null_cnt_ = 0;
    is_max_min_idx_changed_ = false;
  }

  void reset_for_new_frame()
  {
    max_min_index_ = -1;
    is_inverse_agg_ = false;
    null_cnt_ = 0;
    is_max_min_idx_changed_ = false;
  }
};

struct RuntimeContext
{
  RuntimeContext(sql::ObEvalCtx &eval_ctx, uint64_t tenant_id, ObIArray<ObAggrInfo> &aggr_infos,
                 const lib::ObLabel &label) :
    eval_ctx_(eval_ctx),
    aggr_infos_(aggr_infos),
    allocator_(label, OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id, ObCtxIds::WORK_AREA),
    op_monitor_info_(nullptr), io_event_observer_(nullptr), agg_row_meta_(),
    agg_rows_(ModulePageAllocator(label, tenant_id, ObCtxIds::WORK_AREA)),
    agg_extras_(ModulePageAllocator(label, tenant_id, ObCtxIds::WORK_AREA)),
    removal_info_(), win_func_agg_(false)
  {}

  inline const AggrRowMeta &row_meta() const
  {
    return agg_row_meta_;
  }
  inline ObAggregateProcessor::ExtraResult *&get_extra(const int64_t agg_col_id, const char *agg_cell)
  {
    OB_ASSERT(agg_col_id < agg_row_meta_.col_cnt_);
    OB_ASSERT(agg_cell != nullptr);
    OB_ASSERT(row_meta().extra_idx_offset_ > 0);
    OB_ASSERT(row_meta().col_offsets_ != nullptr);
    OB_ASSERT(row_meta().extra_idxes_ != nullptr);
    const char *row = agg_cell - row_meta().col_offsets_[agg_col_id];
    int32_t extra_idx = *reinterpret_cast<const int32_t *>(row + row_meta().extra_idx_offset_);
    OB_ASSERT(row_meta().extra_idxes_[agg_col_id] >= 0);
    int32_t agg_extra_id = row_meta().extra_idxes_[agg_col_id];
    OB_ASSERT(agg_extras_.at(extra_idx) != nullptr
              && row_meta().extra_cnt_> agg_extra_id);
    return agg_extras_.at(extra_idx)[agg_extra_id];
  }
  ObAggrInfo &locate_aggr_info(const int64_t agg_col_idx)
  {
    OB_ASSERT(agg_col_idx < aggr_infos_.count());
    return aggr_infos_.at(agg_col_idx);
  }

  ObScale get_first_param_scale(int64_t agg_col_id) const
  {
    OB_ASSERT(!aggr_infos_.at(agg_col_id).param_exprs_.empty());
    OB_ASSERT(aggr_infos_.at(agg_col_id).param_exprs_.at(0) != NULL);
    if (ob_is_integer_type(aggr_infos_.at(agg_col_id).param_exprs_.at(0)->datum_meta_.type_)) {
      return DEFAULT_SCALE_FOR_INTEGER;
    } else {
      return aggr_infos_.at(agg_col_id).param_exprs_.at(0)->datum_meta_.scale_;
    }
  }

  void get_agg_payload(const int32_t agg_col_id, const int32_t group_id, char *&payload,
                       int32_t &len)
  {
    OB_ASSERT(group_id < agg_rows_.count());
    AggrRowPtr row = agg_rows_.at(group_id);
    row_meta().locate_cell_payload(agg_col_id, row, payload, len);
    return;
  }

  void get_agg_payload(const int32_t agg_col_id, const int32_t group_id, const char *&payload,
                       int32_t &len)
  {
    char *tmp_payload = nullptr;
    get_agg_payload(agg_col_id, group_id, tmp_payload, len);
    payload = tmp_payload;
  }

  NotNullBitVector &locate_notnulls_bitmap(const int32_t agg_col_id, char *agg_cell) const
  {
    OB_ASSERT(row_meta().col_cnt_ > agg_col_id);
    OB_ASSERT(row_meta().col_offsets_ != nullptr);
    OB_ASSERT(row_meta().nullbits_offset_ >= 0);
    char *row = agg_cell - row_meta().col_offsets_[agg_col_id];
    return row_meta().locate_notnulls_bitmap(row);
  }

  const NotNullBitVector &locate_notnulls_bitmap(const int32_t agg_col_id,
                                                 const char *agg_cell) const
  {
    OB_ASSERT(row_meta().col_cnt_ > agg_col_id);
    OB_ASSERT(row_meta().col_offsets_ != nullptr);
    OB_ASSERT(row_meta().nullbits_offset_ >= 0);
    const char *row = agg_cell - row_meta().col_offsets_[agg_col_id];
    return row_meta().locate_notnulls_bitmap(const_cast<char *>(row));
  }

  int32_t get_cell_len(const int32_t agg_col_id, const char *agg_cell) const
  {
    OB_ASSERT(agg_cell != nullptr);
    OB_ASSERT(row_meta().col_offsets_ != nullptr);
    OB_ASSERT(row_meta().col_cnt_ > agg_col_id);
    const char *row = agg_cell - row_meta().col_offsets_[agg_col_id];
    return row_meta().get_cell_len(agg_col_id, row);
  }

  void set_agg_cell(const char *src, const int32_t data_len, const int32_t col_id,
                    char *agg_cell) const
  {
    if (agg_row_meta_.use_var_len_ != nullptr && agg_row_meta_.use_var_len_->at(col_id)) {
      *reinterpret_cast<int64_t *>(agg_cell) = reinterpret_cast<int64_t>(src);
      *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *)) = data_len;
    } else {
      MEMCPY(agg_cell, src, data_len);
    }
  }
  void reuse()
  {
    agg_rows_.reuse();
    for (int i = 0; i < agg_extras_.count(); i++) {
      if (OB_NOT_NULL(agg_extras_.at(i))) {
        for (int j = 0; j < row_meta().extra_cnt_; j++) {
          if (OB_NOT_NULL(agg_extras_.at(i)[j])) {
            agg_extras_.at(i)[j]->~ExtraResult();
          }
        } // end for
      }
    } // end for
    agg_extras_.reuse();
    allocator_.reset_remain_one_page();
    removal_info_.reset();
  }
  void destroy()
  {
    for (int i = 0; i < agg_extras_.count(); i++) {
      if (OB_NOT_NULL(agg_extras_.at(i))) {
        for (int j = 0; j < row_meta().extra_cnt_; j++) {
        if (OB_NOT_NULL(agg_extras_.at(i)[j])) {
            agg_extras_.at(i)[j]->~ExtraResult();
          }
        } // end for
      }
    } // end for
    agg_rows_.reset();
    agg_extras_.reset();
    allocator_.reset();
    op_monitor_info_ = nullptr;
    io_event_observer_ = nullptr;
    agg_row_meta_.reset();
    removal_info_.reset();
    win_func_agg_ = false;
  }

  inline void enable_removal_opt()
  {
    removal_info_.enable_removal_opt_ = true;
  }
  inline void set_inverse_agg(bool is_inverse)
  {
    removal_info_.is_inverse_agg_ = is_inverse;
  }

  inline void disable_inverse_agg()
  {
    removal_info_.reset();
  }

  int init_row_meta(ObIArray<ObAggrInfo> &aggr_infos, ObIAllocator &alloc);
  sql::ObEvalCtx &eval_ctx_;
  ObIArray<ObAggrInfo> &aggr_infos_;
  // used to allocate runtime data memory, such as rows for distinct extra.
  ObArenaAllocator allocator_;
  sql::ObMonitorNode *op_monitor_info_;
  sql::ObIOEventObserver *io_event_observer_;
  AggrRowMeta agg_row_meta_;
  ObSegmentArray<AggrRowPtr, OB_MALLOC_MIDDLE_BLOCK_SIZE, common::ModulePageAllocator>
    agg_rows_;
  ObSegmentArray<AggregateExtras, OB_MALLOC_MIDDLE_BLOCK_SIZE, common::ModulePageAllocator>
    agg_extras_;
  RemovalInfo removal_info_;
  bool win_func_agg_;
};

/*
 IAggregate is a abstract interface for aggregate functions, to implement a new aggregate function,
 do as following code template:
  ```C++
   class AggFunc final: public BatchAggregateWrapper<AggFunc>
   {
   public:
    // need define in & out vector type class
    static const VecValueTypeClass IN_TC = {in_tc};
    static const VecValueTypeClass OUT_TC = {out_tc};
   public:
     template<typename ColumnFmt>
     int add_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                 const int32_t agg_col_id, char *aggr_cell)
     {
        // do add row
        // `ColumnFmt` is type of input vector
        // ColumnFmt is specified format for input param
     }
     template<typename ColumnFmt>
     int add_nullable_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                          const int32_t agg_col_id, char *aggr_cell)
     {
        // do add nullable row
        // if (columns.is_null(row_num)) {  do add null  }
     }
     template<typename ResultFmt>
     int collect_group_result(RuntimeContext &agg_ctx,
                              const sql::ObExpr &agg_expr, const int32_t agg_col_id,
                              const char *aggr_cell, const int32_t aggr_cell_len)
     {
        // do collect group result
        // `ResultFmt` is type of output vector
        // ResultFmt is specified result column format.
     }
     // optinal member function
     template<typename ColumnFmt>
     int add_param_batch(RuntimeContext &agg_ctx, ObBitVector &skip, const EvalBound &bound,
                         const RowSelector &row_sel, const int32_t agg_col_id,
                         const int32_t param_id, ColumnFmt &param_vec, char *aggr_cell)
      {
        // if aggregate function has multiple param exprs, e.g. `count`,
        // this member function should be defined to add batch for each param.
      }
     TO_STRING_KV("aggregate", {aggregate_name});
   };
 ```
*/
class IAggregate
{
public:
  inline virtual int collect_batch_group_results(RuntimeContext &agg_ctx,
                                                 const int32_t agg_col_id,
                                                 const int32_t cur_group_id,
                                                 const int32_t output_start_idx,
                                                 const int32_t expect_batch_size,
                                                 int32_t &output_size,
                                                 const ObBitVector *skip = nullptr) = 0;

  inline virtual int collect_batch_group_results(RuntimeContext &agg_ctx,
                                                 const int32_t agg_col_id,
                                                 const int32_t output_start_idx,
                                                 const int32_t batch_size,
                                                 const ObCompactRow **rows,
                                                 const RowMeta &row_meta) = 0;
  inline virtual int add_batch_rows(RuntimeContext &agg_ctx,
                                    int32_t agg_col_idx,
                                    const sql::ObBitVector &skip, const sql::EvalBound &bound,
                                    char *agg_cell,
                                    const RowSelector row_sel = RowSelector{}) = 0;

  inline virtual int add_batch_for_multi_groups(RuntimeContext &agg_ctx, AggrRowPtr *agg_rows,
                                                RowSelector &row_sel, const int64_t batch_size,
                                                const int32_t agg_col_id) = 0;
  inline virtual int add_one_row(RuntimeContext &agg_ctx, const int64_t batch_idx,
                                 const int64_t batch_size, const bool is_null, const char *data,
                                 const int32_t data_len, int32_t agg_col_idx, char *agg_cell) = 0;
  // temp result for one batch,
  // used for sum
  inline virtual void *get_tmp_res(RuntimeContext &agg_ctx, int32_t agg_col_idx,
                                   char *agg_cell) = 0;

  // calculation info such as obj_meta for one batch
  inline virtual int64_t get_batch_calc_info(RuntimeContext &agg_ctx, int32_t agg_col_idx,
                                             char *agg_cell) = 0;

  // TODO: add rollup interface
  // for wrapper aggregates, like `ObDistinctWrapper`
  inline virtual void set_inner_aggregate(IAggregate *agg) = 0;
  // initializer
  virtual int init(RuntimeContext &agg_ctx, const int64_t agg_col_id, ObIAllocator &allocator) = 0;

  virtual void reuse() = 0;

  virtual void destroy() = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

} // end aggregate
} // end share
} // end oceanbase
#endif // OCEANBASE_SHARE_AGGREGATE_CTX_H_