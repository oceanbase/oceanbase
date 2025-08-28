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
  }

  void reuse()
  {
    if (distinct_extra_store != nullptr) {
      distinct_extra_store->reuse();
    }
    if (data_store != nullptr) {
      data_store->reuse();
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