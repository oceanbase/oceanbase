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

#ifndef OCEANBASE_SHARE_AGGREGATE_AGGR_EXTRA_H_
#define OCEANBASE_SHARE_AGGREGATE_AGGR_EXTRA_H_

#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "sql/engine/basic/ob_vector_result_holder.h"
#include "sql/engine/basic/ob_hp_infras_vec_mgr.h"
#include "sql/engine/sort/ob_sort_vec_op_provider.h"
#include "share/stat/ob_topk_hist_estimator.h"
#include "share/stat/ob_hybrid_hist_estimator.h"
#include "share/aggregate/util.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
class VecExtraResult
{
public:
  // %alloc is used to initialize the structures, can not be used to hold the data
  explicit VecExtraResult(common::ObIAllocator &alloc, ObMonitorNode &op_monitor_info) :
    flags_(0), alloc_(alloc), op_monitor_info_(op_monitor_info)
  {}
  virtual ~VecExtraResult()
  {}
  bool is_inited() const
  {
    return is_inited_;
  }
  bool is_evaluated() const
  {
    return is_evaluated_;
  }
  void set_is_evaluated()
  {
    is_evaluated_ = true;
  }
  virtual void reuse()
  {}
  DECLARE_VIRTUAL_TO_STRING;

protected:
  union
  {
    uint8_t flags_;
    struct
    {
      int32_t is_inited_ : 1;
      int32_t is_evaluated_ : 1;
    };
  };
  common::ObIAllocator &alloc_;
  ObMonitorNode &op_monitor_info_;
};

class HashBasedDistinctVecExtraResult : public VecExtraResult
{
public:
  explicit HashBasedDistinctVecExtraResult(common::ObIAllocator &alloc,
                                           ObMonitorNode &op_monitor_info) :
    VecExtraResult(alloc, op_monitor_info),
    hash_values_for_batch_(nullptr), my_skip_(nullptr), aggr_info_(nullptr),
    hp_infras_mgr_(nullptr), hp_infras_(nullptr), need_rewind_(false), max_batch_size_(0),
    try_check_tick_(0), status_flags_(0), brs_holder_(&alloc)
  {}
  virtual ~HashBasedDistinctVecExtraResult();
  virtual void reuse();
  int rewind();
  int init_distinct_set(const ObAggrInfo &aggr_info, const bool need_rewind,
                        ObHashPartInfrasVecMgr &hp_infras_mgr, ObEvalCtx &eval_ctx);
  int insert_row_for_batch(const common::ObIArray<ObExpr *> &exprs, const int64_t batch_size,
                           const ObBitVector *skip = nullptr, const int64_t start_idx = 0);
  int get_next_unique_hash_table_batch(const common::ObIArray<ObExpr *> &exprs,
                                       const int64_t max_row_cnt, int64_t &read_rows);
  int init_vector_default(ObEvalCtx &ctx, const int64_t size);
  DECLARE_VIRTUAL_TO_STRING;

private:
  int init_hp_infras();
  int init_my_skip(const int64_t batch_size);
  int build_distinct_data_for_batch(const common::ObIArray<ObExpr *> &exprs,
                                    const int64_t batch_size);
  inline int try_check_status()
  {
    return ((++try_check_tick_) % 1024 == 0)
      ? THIS_WORKER.check_status()
      : common::OB_SUCCESS;
  }

protected:
  uint64_t *hash_values_for_batch_;
  ObBitVector *my_skip_;
  const ObAggrInfo *aggr_info_;
  ObHashPartInfrasVecMgr *hp_infras_mgr_;
  HashPartInfrasVec *hp_infras_;
  bool need_rewind_;
  int64_t max_batch_size_;
  int64_t try_check_tick_;
  union
  {
    uint8_t status_flags_;
    struct
    {
      uint32_t inited_hp_infras_ : 1;
      uint32_t got_row_ : 1;
    };
  };

public:
  ObVectorsResultHolder brs_holder_;
};
class DataStoreVecExtraResult : public VecExtraResult
{
public:
  explicit DataStoreVecExtraResult(common::ObIAllocator &alloc, ObMonitorNode &op_monitor_info,
                                   bool need_sort) :
    VecExtraResult(alloc, op_monitor_info),
    sort_(NULL), pvt_skip(nullptr), need_sort_(need_sort), data_store_inited_(false),
    data_store_brs_holder_(&alloc), need_count_(false), sort_count_(0)
  {}

  virtual ~DataStoreVecExtraResult();

  int add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &eval_ctx,
                const sql::EvalBound &bound, const sql::ObBitVector &skip,
                const uint16_t selector[], const int64_t size, ObIAllocator &allocator);

  int add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &eval_ctx,
                const sql::EvalBound &bound, const sql::ObBitVector &skip, ObIAllocator &allocator);

  int get_next_batch(ObEvalCtx &ctx, const common::ObIArray<ObExpr *> &exprs, int64_t &read_rows);

  int prepare_for_eval();

  void reuse();

  bool data_store_is_inited() const
  {
    return data_store_inited_;
  }

  int init_data_set(ObAggrInfo &aggr_info, ObEvalCtx &eval_ctx, ObMonitorNode *op_monitor_info,
                    ObIOEventObserver *io_event_observer_, ObIAllocator &allocator,
                    bool need_rewind);

  int rewind();

  int64_t get_sort_count() { return sort_count_; }

  void set_need_count(bool b) { need_count_ = b; }

protected:
  union
  {
    ObSortVecOpProvider *sort_;
    ObTempRowStore *store_;
  };

  ObTempRowStore::Iterator *vec_result_iter_;

  ObBitVector *pvt_skip;

  bool need_sort_;
  bool data_store_inited_;

public:
  ObVectorsResultHolder data_store_brs_holder_;
  bool need_count_;
  int64_t sort_count_;
};

class TopFreHistVecExtraResult : public VecExtraResult
{
public:
  explicit TopFreHistVecExtraResult(common::ObIAllocator &alloc,
                                    ObMonitorNode &op_monitor_info)
    : VecExtraResult(alloc, op_monitor_info),
      lob_prefix_allocator_("CalcTopkHist", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(),
                            ObCtxIds::WORK_AREA),
      tmp_batch_cap_(0),
      tmp_batch_idx_(0),
      tmp_batch_payloads_(NULL),
      tmp_batch_payload_lens_(NULL),
      tmp_batch_hash_vals_(NULL),
      window_size_(0),
      item_size_(0),
      max_disuse_cnt_(0),
      is_topk_hist_need_des_row_(false)
  {}
  virtual ~TopFreHistVecExtraResult();

  int init_topk_fre_histogram_item(ObIAllocator &allocator,
                                   ObAggrInfo &aggr_info,
                                   ObEvalCtx &eval_ctx);

  void reuse();

  int rewind();

  ObTopKFrequencyHistograms &get_topk_hist()
  {
    return topk_fre_hist_;
  }

  OB_INLINE int add_one_batch_item(const char* payload,
                                   int payload_len,
                                   uint64_t hash_val)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(tmp_batch_idx_ >= tmp_batch_cap_)) {
      if (OB_FAIL(flush_batch_rows())) {
        SQL_LOG(WARN, "failed to flush batch rows", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      *(tmp_batch_payloads_ + tmp_batch_idx_) = payload;
      tmp_batch_payload_lens_[tmp_batch_idx_] = payload_len;
      tmp_batch_hash_vals_[tmp_batch_idx_] = hash_val;
      ++tmp_batch_idx_;
    }
    return ret;
  }

  OB_INLINE int flush_batch_rows()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(topk_fre_hist_.add_batch_items(tmp_batch_payloads_,
                                               tmp_batch_payload_lens_,
                                               tmp_batch_hash_vals_,
                                               tmp_batch_idx_))) {
      SQL_LOG(WARN, "failed to add batch items", K(ret));
    } else {
      tmp_batch_idx_ = 0;
    }
    return ret;
  }

  OB_INLINE void inc_disuse_cnt()
  {
    topk_fre_hist_.inc_disuse_cnt();
  }

  OB_INLINE ObIAllocator& get_lob_prefix_allocator() { return lob_prefix_allocator_; }

protected:
  void set_topk_fre_histogram_item();
  ObTopKFrequencyHistograms topk_fre_hist_;
  ObArenaAllocator lob_prefix_allocator_;
  int64_t tmp_batch_cap_;
  int64_t tmp_batch_idx_;
  const char **tmp_batch_payloads_;
  int32_t *tmp_batch_payload_lens_;
  uint64_t *tmp_batch_hash_vals_;
  int64_t window_size_;
  int64_t item_size_;
  int64_t max_disuse_cnt_;
  bool is_topk_hist_need_des_row_;
};

class HybridHistVecAvailableMemChecker
{
public:
  explicit HybridHistVecAvailableMemChecker(int64_t cur_cnt) : cur_cnt_(cur_cnt)
  {}
  OB_INLINE bool operator()(int64_t max_row_count)
  {
    return cur_cnt_ > max_row_count;
  }

private:
  int64_t cur_cnt_;
};

class WindowFunnelVecExtraResult : public VecExtraResult
{
public:
  static const int64_t MAX_EVENT_LIST_LENGTH = 2 * 1024 * 1024 /* 2MB */ / 16;

  struct EventList
  {
    int64_t time_;
    int64_t event_idx_;
    bool operator < (const EventList &other) const
    {
      if (OB_UNLIKELY(time_ == other.time_)) {
        return event_idx_ < other.event_idx_;
      } else {
        return time_ < other.time_;
      }
    }
  };
  explicit WindowFunnelVecExtraResult(common::ObIAllocator &alloc,
                                    ObMonitorNode &op_monitor_info)
    : VecExtraResult(alloc, op_monitor_info),
      event_list_(nullptr),
      event_list_cap_(0),
      event_list_size_(0),
      sort_(nullptr),
      pvt_skip_(nullptr),
      pseudo_exprs_(nullptr),
      is_sorted_(true),
      event_list_inited_(false),
      sort_inited_(false)
    {}

  virtual ~WindowFunnelVecExtraResult()
  {
    reuse();
    is_sorted_ = true;
    event_list_ = nullptr;
    event_list_cap_ = 0;
    event_list_size_ = 0;
    event_list_inited_ = false;
    sort_inited_ = false;
  }

  OB_INLINE int init_event_list(ObIAllocator &allocator, int64_t cap)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(event_list_ = (EventList *) allocator.alloc(sizeof(EventList) * cap))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      event_list_cap_ = cap;
      event_list_size_ = 0;
      event_list_inited_ = true;
    }
    return ret;
  }

  int add_event(int64_t event_idx, int64_t time, ObIAllocator &allocator,
                ObAggrInfo &aggr_info, ObEvalCtx &eval_ctx, ObMonitorNode *op_monitor_info,
                ObIOEventObserver *io_event_observer, bool need_rewind)
  {
    int ret = OB_SUCCESS;
    if (!event_list_inited_ && OB_FAIL(init_event_list(allocator, 2))) {
      SQL_LOG(WARN, "init event list failed", K(ret));
    } else if (event_list_size_ >= event_list_cap_) {
      void *new_event_list = nullptr;
      int64_t new_cap = event_list_cap_ * 2;
      if (OB_ISNULL(new_event_list = allocator.alloc(sizeof(EventList) * new_cap))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        MEMCPY(new_event_list, event_list_, sizeof(EventList) * event_list_size_);
        event_list_ = (EventList *) new_event_list;
        event_list_cap_ = new_cap;
      }
    }
    if (OB_SUCC(ret)) {
      event_list_[event_list_size_].time_ = time;
      event_list_[event_list_size_].event_idx_ = event_idx;
      event_list_size_++;
      is_sorted_ = event_list_size_ > 1 ?
        (is_sorted_ && (event_list_[event_list_size_ - 2].time_ <= event_list_[event_list_size_ - 1].time_)) : true;
    }

    if (event_list_size_ >= MAX_EVENT_LIST_LENGTH) {
      // Add the data from event_list_ to the sort provider store.
      if (OB_FAIL(add_batch(aggr_info, eval_ctx, op_monitor_info, io_event_observer, allocator, need_rewind))) {
        SQL_LOG(WARN, "failed to add batch", K(ret));
      } else {
        event_list_size_ = 0;
      }
    }
    return ret;
  }

  int init_sort_provider(ObAggrInfo &aggr_info, ObEvalCtx &eval_ctx,
                         ObMonitorNode *op_monitor_info, ObIOEventObserver *io_event_observer,
                         ObIAllocator &allocator, bool need_rewind)
  {
    int ret = OB_SUCCESS;
    ObSortVecOpContext context;
    context.tenant_id_ = eval_ctx.exec_ctx_.get_my_session()->get_effective_tenant_id();

    OB_ASSERT(aggr_info.sort_collations_.count() > 0);

    ObSortCollations &sort_collations_ = aggr_info.sort_collations_;

    void *sort_key_buf = nullptr;
    ExprFixedArray *sort_key = nullptr;
    const int64_t PSEUDO_EXPR_COUNT = WINDOW_FUNNEL_PSEUDO_EVENT_IDX_EXPR_IDX - WINDOW_FUNNEL_PSEUDO_TIME_EXPR_IDX + 1;

    if (OB_ISNULL(sort_key_buf = allocator.alloc(sizeof(ExprFixedArray)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "allocate memory failed", K(ret));
    } else if (FALSE_IT(sort_key = new (sort_key_buf) ExprFixedArray(allocator))) {
    } else if (OB_FAIL(sort_key->init(PSEUDO_EXPR_COUNT))) {
      SQL_LOG(WARN, "failed to init", K(ret));
    } else {
      for (int i = WINDOW_FUNNEL_PSEUDO_TIME_EXPR_IDX;
            OB_SUCC(ret) && i <= WINDOW_FUNNEL_PSEUDO_EVENT_IDX_EXPR_IDX; i++) {
        if (OB_FAIL(sort_key->push_back(aggr_info.param_exprs_.at(i)))) {
          SQL_LOG(WARN, "failed to push back", K(ret));
        }
      }
    }

    context.sk_exprs_ = sort_key;
    context.has_addon_ = false;
    context.sk_collations_ = &sort_collations_;
    context.eval_ctx_ = &eval_ctx;
    context.exec_ctx_ = &eval_ctx.exec_ctx_;
    context.need_rewind_ = need_rewind;
    // one sortkey and not null
    context.enable_single_col_compare_ = true;

    void *sort_buf = nullptr;

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sort_buf = allocator.alloc(sizeof(ObSortVecOpProvider)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "allocate memory failed", K(ret));
      } else if (FALSE_IT(sort_ = new (sort_buf) ObSortVecOpProvider(*op_monitor_info))) {
      } else if (OB_FAIL(sort_->init(context))) {
        LOG_WARN("failed to init sort", K(ret));
      } else {
        sort_->set_operator_type(op_monitor_info->get_operator_type());
        sort_->set_operator_id(op_monitor_info->get_op_id());
        sort_->set_io_event_observer(io_event_observer);
      }
    }

    if (OB_SUCC(ret) && pvt_skip_ == nullptr) {
      char *skip_buf = nullptr;
      int skip_size = ObBitVector::memory_size(eval_ctx.max_batch_size_);
      if (OB_ISNULL(skip_buf = (char *)allocator.alloc(skip_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "allocate memory failed", K(ret));
      } else {
        pvt_skip_ = to_bit_vector(skip_buf);
        pvt_skip_->reset(eval_ctx.max_batch_size_);
      }
    }
    if (OB_SUCC(ret)) {
      sort_inited_ = true;
      pseudo_exprs_ = sort_key;
    }
    return ret;
  }

  template <typename ColumnFmt>
  int fill_vector_batch(const ObExpr &expr, int64_t col_offset, ObEvalCtx &eval_ctx, int64_t start_idx, int64_t end_idx)
  {
    int ret = OB_SUCCESS;
    ColumnFmt *columns = static_cast<ColumnFmt *>(expr.get_vector(eval_ctx));
    for (int64_t i = 0; i < end_idx - start_idx; i++) {
      columns->set_payload(i, (const char *)(event_list_) + sizeof(EventList) * (i + start_idx) + col_offset, sizeof(int64_t));
    }
    return ret;
  }

  int fill_vector(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &eval_ctx, int64_t start_idx, int64_t end_idx)
  {
    int ret = OB_SUCCESS;
    int64_t col_offset = 0;
    for (int i = 0; i < exprs.count() && OB_SUCC(ret); i++) {
      ObExpr *cur_expr = exprs.at(i);
      VectorFormat fmt = cur_expr->get_format(eval_ctx);
      switch (fmt) {
      case VEC_FIXED: {
        fill_vector_batch<ObFixedLengthFormat<int64_t>>(*cur_expr, col_offset, eval_ctx, start_idx, end_idx);
        break;
      }
      case VEC_UNIFORM: {
        fill_vector_batch<ObUniformFormat<false>>(*cur_expr, col_offset, eval_ctx, start_idx, end_idx);
        break;
      }
      case VEC_UNIFORM_CONST: {
        fill_vector_batch<ObUniformFormat<true>>(*cur_expr, col_offset, eval_ctx, start_idx, end_idx);
        break;
      }
      case VEC_CONTINUOUS: {
        fill_vector_batch<ObContinuousFormat>(*cur_expr, col_offset, eval_ctx, start_idx, end_idx);
        break;
      }
      case VEC_DISCRETE: {
        fill_vector_batch<ObDiscreteFormat>(*cur_expr, col_offset, eval_ctx, start_idx, end_idx);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
      }
      }
      col_offset += sizeof(int64_t);
      cur_expr->get_eval_info(eval_ctx).set_projected(true);
    }
    return ret;
  }

  int init_vector_default(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx, const int64_t size)
  {
    int ret = OB_SUCCESS;
    for (int i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      const ObExpr *expr = exprs.at(i);
      if (OB_FAIL(expr->init_vector_for_write(ctx, expr->get_default_res_format(), size))) {
        LOG_WARN("failed to init vector default", K(ret));
      }
    }
    return ret;
  }

  int add_batch(ObAggrInfo &aggr_info, ObEvalCtx &eval_ctx,
                ObMonitorNode *op_monitor_info, ObIOEventObserver *io_event_observer,
                ObIAllocator &allocator, bool need_rewind)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!event_list_inited_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "event list not initialized", K(ret));
    } else if (!sort_inited_ && OB_FAIL(init_sort_provider(aggr_info,
                                                           eval_ctx,
                                                           op_monitor_info,
                                                           io_event_observer,
                                                           allocator,
                                                           need_rewind))) {
      SQL_LOG(WARN, "init sort provider failed", K(ret));
    } else if (OB_FAIL(init_vector_default(*pseudo_exprs_,
                                           eval_ctx,
                                           eval_ctx.max_batch_size_))) {
      SQL_LOG(WARN, "failed to init vector default", K(ret));
    } else {
      int64_t batch_size = eval_ctx.max_batch_size_;
      int64_t start_idx = 0;
      int64_t end_idx = min(batch_size, event_list_size_);
      pvt_skip_->reset(eval_ctx.max_batch_size_);
      while (OB_SUCC(ret) && start_idx < end_idx && start_idx < event_list_size_) {
        ObBatchRows brs = ObBatchRows(const_cast<ObBitVector &>(*pvt_skip_), end_idx - start_idx, true);
        bool need_dump = true;
        if (OB_FAIL(fill_vector(*pseudo_exprs_, eval_ctx,
                                start_idx, end_idx))) {
          SQL_LOG(WARN, "failed to fill vector", K(ret));
        } else if (OB_FAIL(sort_->add_batch(brs, need_dump))) {
          SQL_LOG(WARN, "failed to add batch", K(ret));
        } else {
          start_idx += batch_size;
          end_idx += batch_size;
          end_idx = min(end_idx, event_list_size_);
        }
      }
    }
    return ret;
  }

  int prepare_for_eval(ObAggrInfo &aggr_info, ObEvalCtx &eval_ctx, ObMonitorNode *op_monitor_info,
                       ObIOEventObserver *io_event_observer, ObIAllocator &allocator, bool need_rewind)
  {
    int ret = OB_SUCCESS;
    if (sort_inited_) {
      // add the remaining data from event_list_ to the sort provider store.
      if (event_list_size_ > 0 && OB_FAIL(add_batch(aggr_info, eval_ctx, op_monitor_info,
                                                    io_event_observer, allocator, need_rewind))) {
        SQL_LOG(WARN, "failed to add batch", K(ret));
      } else if (OB_FAIL(sort_->sort())) {
        SQL_LOG(WARN, "failed to sort", K(ret));
      } else {
        event_list_size_ = 0;
      }
    } else if (!is_sorted_) {
      lib::ob_sort(event_list_, event_list_ + event_list_size_);
    }
    return ret;
  }
  void reuse()
  {
    if (sort_inited_) {
      if (sort_ != nullptr) {
        sort_->destroy();
      }
    }
    is_sorted_ = true;
    sort_inited_ = false;
    sort_ = nullptr;
    event_list_size_ = 0;
  }

  void reuse_event_list()
  {
    event_list_size_ = 0;
  }

  int rewind()
  {
    // do nothing
    return OB_SUCCESS;
  }

  int get_next_batch(ObEvalCtx &ctx, int64_t &read_rows)
  {
    int ret = OB_SUCCESS;
    ret = sort_->get_next_batch(ctx.max_batch_size_, read_rows);
    return ret;
  }

public:
  EventList *event_list_;
  int64_t event_list_cap_;
  int64_t event_list_size_;
  ObSortVecOpProvider *sort_;
  ObBitVector *pvt_skip_;
  ObIArray<ObExpr *> *pseudo_exprs_;
  bool is_sorted_;
  bool event_list_inited_;
  bool sort_inited_;
};

class HybridHistVecExtraResult : public VecExtraResult
{
  const static int MAX_BATCH_SIZE = 256;
public:
  explicit HybridHistVecExtraResult(common::ObIAllocator &alloc,
                                    ObMonitorNode &op_monitor_info)
    : VecExtraResult(alloc, op_monitor_info),
      store_(), mem_context_(nullptr),
      profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
      sql_mem_processor_(profile_, op_monitor_info),
      batch_vector_(NULL),
      lob_prefix_allocator_("LobHybridHist", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(),
                            ObCtxIds::WORK_AREA),
      bucket_num_(0), num_distinct_(0),
      null_count_(0), total_count_(0),
      pop_count_(0), pop_freq_(0),
      repeat_count_(0), prev_row_(NULL),
      prev_payload_(NULL),
      prev_len_(0), batch_idx_(0), max_batch_size_(MAX_BATCH_SIZE),
      batch_bucket_desc_(NULL),
      data_store_inited_(false)
    {}

  virtual ~HybridHistVecExtraResult()
  {
    sql_mem_processor_.unregister_profile();
    store_.reset();
    store_.~ObTempRowStore();
    if (nullptr != mem_context_) {
      DESTROY_CONTEXT(mem_context_);
      mem_context_ = nullptr;
    }
  }

  int init_data_set(ObIAllocator &allocator,
                    ObAggrInfo &aggr_info,
                    ObEvalCtx &eval_ctx,
                    ObIOEventObserver *io_event_observer);

  bool data_store_is_inited() const
  {
    return data_store_inited_;
  }

  void reuse()
  {
    store_.reset();
    prev_row_ = NULL;
    num_distinct_ = 0;
    null_count_ = 0;
    total_count_ = 0;
    pop_count_ = 0;
    pop_freq_ = 0;
    repeat_count_ = 0;
    pop_threshold_ = 0;
    prev_payload_ = NULL;
    prev_len_ = 0;
    batch_idx_ = 0;
  }

  int rewind()
  {
    int ret = OB_NOT_SUPPORTED;
    SQL_LOG(WARN, "unsupported hybrid hist in window function", K(ret));
    return ret;
  }

  int prepare_for_eval();

  int compute_hybrid_hist_result(int64_t max_batch_size,
                                 const ObObjMeta &obj_meta,
                                 ObIAllocator &allocator,
                                 ObHybridHistograms &histgram);
  OB_INLINE int64_t get_num_distinct() { return num_distinct_; }
  OB_INLINE void inc_null_count() { ++null_count_; }
  OB_INLINE const char* get_prev_payload() { return prev_payload_; }
  OB_INLINE int get_prev_payload_len() { return prev_len_; }
  OB_INLINE void calc_total_count(int64_t sort_count)
  {
    total_count_ = sort_count - null_count_;
    pop_threshold_ = total_count_ / bucket_num_;
  }
  OB_INLINE void inc_repeat_count() { ++repeat_count_; }
  OB_INLINE bool need_dump() const
  { return sql_mem_processor_.get_data_size() > sql_mem_processor_.get_mem_bound(); }

  int process_dump();
  int flush_batch_rows(bool need_dump);

  OB_INLINE ObIAllocator& get_lob_prefix_allocator() { return lob_prefix_allocator_; }

  OB_INLINE void fill_row_desc()
  {
    BucketDesc &desc = batch_bucket_desc_[batch_idx_];
    desc.ep_count_ = repeat_count_;
    desc.is_pop_ = repeat_count_ > pop_threshold_;
    if (desc.is_pop_) {
      pop_freq_ += repeat_count_;
      ++pop_count_;
    }
  }

  OB_INLINE void add_payload(const char* payload, int payload_len)
  {
    batch_vector_->get_ptrs()[batch_idx_] = const_cast<char*>(payload);
    batch_vector_->get_lens()[batch_idx_] = payload_len;
    ++batch_idx_;
    repeat_count_ = 1;
    ++num_distinct_;
    prev_payload_ = payload;
    prev_len_ = payload_len;
  }

  OB_INLINE int add_one_batch_item(const char* payload, int payload_len)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(batch_idx_ >= max_batch_size_)) {
      if (OB_FAIL(flush_batch_rows(false))) {
        SQL_LOG(WARN, "failed to flush batch rows", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      fill_row_desc();
      add_payload(payload, payload_len);
    }
    return ret;
  }

protected:
  int init_batch_vector(ObIAllocator &allocator, int max_batch_size);

protected:
  ObTempRowStore store_;
  lib::MemoryContext mem_context_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
  ObDiscreteBase *batch_vector_;
  ObArenaAllocator lob_prefix_allocator_;
  int64_t bucket_num_;
  int64_t num_distinct_;
  int64_t null_count_;
  int64_t total_count_;
  int64_t pop_count_;
  int64_t pop_freq_;
  int64_t repeat_count_;
  ObCompactRow *prev_row_;
  int pop_threshold_;
  const char* prev_payload_;
  int prev_len_;
  int batch_idx_;
  int max_batch_size_;
  BucketDesc *batch_bucket_desc_;
  bool data_store_inited_;
};

struct ExtraStores {

  ExtraStores() :
    distinct_extra_store(nullptr),
    data_store(nullptr),
    window_funnel_store_(nullptr),
    top_fre_hist_store_(nullptr),
    flags_(0)
  {}

  ~ExtraStores()
  {
    if (distinct_extra_store != nullptr) {
      distinct_extra_store->~HashBasedDistinctVecExtraResult();
      distinct_extra_store = nullptr;
    }
    if (data_store != nullptr) {
      data_store->~DataStoreVecExtraResult();
      data_store = nullptr;
    }
    if (is_top_fre_hist_ && top_fre_hist_store_ != nullptr) {
      top_fre_hist_store_->~TopFreHistVecExtraResult();
      top_fre_hist_store_ = nullptr;
    }
    if (is_hybrid_hist_ && hybrid_hist_store_ != nullptr) {
      hybrid_hist_store_->~HybridHistVecExtraResult();
      hybrid_hist_store_ = nullptr;
    }
    if (window_funnel_store_ != nullptr) {
      window_funnel_store_->~WindowFunnelVecExtraResult();
      window_funnel_store_ = nullptr;
    }
  }

  void reuse()
  {
    if (distinct_extra_store != nullptr) {
      distinct_extra_store->reuse();
    }
    if (data_store != nullptr) {
      data_store->reuse();
    }
    if (window_funnel_store_ != nullptr) {
      window_funnel_store_->reuse();
    }
  }

  void set_is_evaluated()
  {
    if (distinct_extra_store != nullptr) {
      distinct_extra_store->is_evaluated();
    }
    if (data_store != nullptr) {
      data_store->is_evaluated();
    }
  }

  bool is_evaluated()
  {
    bool evaluated = false;
    if (distinct_extra_store != nullptr) {
      evaluated |= distinct_extra_store->is_evaluated();
    }
    if (data_store != nullptr) {
      evaluated |= data_store->is_evaluated();
    }
    if (window_funnel_store_ != nullptr) {
      evaluated |= window_funnel_store_->is_evaluated();
    }
    return evaluated;
  }

  int rewind()
  {
    int ret = OB_SUCCESS;
    if (distinct_extra_store != nullptr) {
      ret = distinct_extra_store->rewind();
    }
    if (data_store != nullptr && OB_SUCC(ret)) {
      ret = data_store->rewind();
    }
    if (window_funnel_store_ != nullptr && OB_SUCC(ret)) {
      ret = window_funnel_store_->rewind();
    }
    return ret;
  }

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(K(distinct_extra_store));
    J_KV(K(data_store));
    J_OBJ_END();
    return pos;
  }

public:
  class HashBasedDistinctVecExtraResult *distinct_extra_store;
  class DataStoreVecExtraResult *data_store;
  class WindowFunnelVecExtraResult *window_funnel_store_;
  union {
    class TopFreHistVecExtraResult *top_fre_hist_store_;
    class HybridHistVecExtraResult *hybrid_hist_store_;
  };
  union {
    int64_t flags_;
    struct {
      bool is_top_fre_hist_ :  1;
      bool is_hybrid_hist_  :  1;
      int64_t reserved_     : 62;
    };
  };
};

} // namespace aggregate
} // end share
} // end oceanbase
#endif // OCEANBASE_SHARE_AGGREGATE_AGGR_EXTRA_H_