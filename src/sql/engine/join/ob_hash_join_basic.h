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

#ifndef SRC_SQL_ENGINE_JOIN_OB_HASH_JOIN_BASIC_H_
#define SRC_SQL_ENGINE_JOIN_OB_HASH_JOIN_BASIC_H_

#include "sql/engine/ob_operator.h"
#include "share/datum/ob_datum_funcs.h"
#include "lib/list/ob_dlist.h"
#include "lib/utility/ob_unify_serialize.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"

namespace oceanbase
{
namespace sql
{

class ObHashJoinBufMgr : public ObSqlMemoryCallback
{
public:
  ObHashJoinBufMgr() :
    reserve_memory_size_(0), data_ratio_(1.0), pre_total_alloc_size_(0), total_alloc_size_(0),
    page_size_(0), dumped_size_(0)
  {}
  virtual ~ObHashJoinBufMgr() {}

  inline void set_page_size(int64_t page_size) { page_size_ = page_size; }
  inline int64_t get_page_size() { return page_size_; }

  void set_reserve_memory_size(int64_t reserve_memory_size, double ratio)
  {
    reserve_memory_size_ = reserve_memory_size;
    data_ratio_ = ratio;
  }
  int64_t get_reserve_memory_size()
  {
    return reserve_memory_size_;
  }
  int64_t get_pre_total_alloc_size()
  {
    return pre_total_alloc_size_;
  }
  void set_pre_total_alloc_size(int64_t size)
  {
    pre_total_alloc_size_ = size;
  }
  int64_t get_total_alloc_size()
  {
    return total_alloc_size_;
  }
  int64_t get_dumped_size()
  {
    return dumped_size_;
  }

  double get_data_ratio()
  {
    return data_ratio_;
  }

  void reuse()
  {
    dumped_size_ = 0;
  }
  OB_INLINE virtual void alloc(int64_t mem)
  {
    total_alloc_size_ += mem;
  }
  OB_INLINE virtual void free(int64_t mem)
  {
    total_alloc_size_ -= mem;
  }
  OB_INLINE virtual void dumped(int64_t size)
  {
    dumped_size_ += size;
  }
  OB_INLINE bool is_full()
  {
    return reserve_memory_size_ < page_size_ + total_alloc_size_;
  }

  OB_INLINE bool need_dump()
  {
    return reserve_memory_size_ * RATIO / 100 < total_alloc_size_;
  }
private:
  const static int64_t RATIO = 80;
  int64_t reserve_memory_size_;
  double data_ratio_;
  int64_t pre_total_alloc_size_;
  int64_t total_alloc_size_;
  int64_t page_size_;
  int64_t dumped_size_;
};

struct ObHashJoinStoredJoinRow : public sql::ObChunkDatumStore::StoredRow
{
  const static int64_t HASH_VAL_BIT = 63;
  const static int64_t STORED_ROW_PTR_BIT = 63;
  const static int64_t HASH_VAL_MASK = UINT64_MAX >> (64 - HASH_VAL_BIT);
  struct ExtraInfo
  {
    union {
      // %hash_value_ is set when row is add to chunk datum store.
      struct {
        uint64_t hash_val_:HASH_VAL_BIT;
        uint64_t is_match_:1;
      };
      // %next_row_ is set when stored row added to hash table.
      struct {
        uint64_t next_row_:STORED_ROW_PTR_BIT;
      };
      uint64_t v_;
    };

    ObHashJoinStoredJoinRow *get_next() const
    {
      return reinterpret_cast<ObHashJoinStoredJoinRow *>(next_row_);
    }

    void set_next(ObHashJoinStoredJoinRow *ptr)
    {
      next_row_ = reinterpret_cast<uint64_t>(ptr);
    }
  };
  STATIC_ASSERT(sizeof(uint64_t) == sizeof(ExtraInfo), "unexpected size");

  ExtraInfo &get_extra_info()
  {
    static_assert(sizeof(ObHashJoinStoredJoinRow) == sizeof(sql::ObChunkDatumStore::StoredRow),
        "sizeof StoredJoinRow must be the save with StoredRow");
    return *reinterpret_cast<ExtraInfo *>(get_extra_payload());
  }
  const ExtraInfo &get_extra_info() const
  { return *reinterpret_cast<const ExtraInfo *>(get_extra_payload()); }

  uint64_t get_hash_value() const { return get_extra_info().hash_val_; }
  void set_hash_value(const uint64_t hash_val)
  { get_extra_info().hash_val_ = hash_val & HASH_VAL_MASK; }
  bool is_match() const { return get_extra_info().is_match_; }
  void set_is_match(bool is_match) { get_extra_info().is_match_ = is_match; }

  ObHashJoinStoredJoinRow *get_next() const { return get_extra_info().get_next(); }
  void set_next(ObHashJoinStoredJoinRow *ptr) { get_extra_info().set_next(ptr); }
};

class ObHashJoinBatch {
public:
  ObHashJoinBatch(
      common::ObIAllocator &alloc,
      ObHashJoinBufMgr *buf_mgr,
      uint64_t tenant_id,
      int32_t part_level,
      int64_t part_shift,
      int64_t batchno)
  : chunk_row_store_(common::ObModIds::OB_ARENA_HASH_JOIN, &alloc),
    inner_callback_(nullptr),
    part_level_(part_level),
    part_shift_(part_shift),
    batchno_(batchno),
    buf_mgr_(buf_mgr),
    tenant_id_(tenant_id),
    n_get_rows_(0),
    n_add_rows_(0),
    pre_total_size_(0),
    pre_bucket_number_(0),
    pre_part_count_(0)
  {}

  virtual ~ObHashJoinBatch();
  int get_next_batch(const common::ObIArray<ObExpr*> &exprs,
                     ObEvalCtx &ctx,
                     const int64_t max_rows,
                     int64_t &read_rows,
                     const ObHashJoinStoredJoinRow **stored_row);
  int get_next_batch(const ObHashJoinStoredJoinRow **stored_row,
                     const int64_t max_rows,
                     int64_t &read_rows);
  int get_next_row(
    const common::ObIArray<ObExpr*> &exprs,
    ObEvalCtx &eval_ctx,
    const ObHashJoinStoredJoinRow *&stored_row);
  int get_next_row(const ObHashJoinStoredJoinRow *&stored_row);
  int convert_row(
    const ObHashJoinStoredJoinRow *stored_row,
    const common::ObIArray<ObExpr*> &exprs,
    ObEvalCtx &eval_ctx);
  int add_row(
    const common::ObIArray<ObExpr*> &exprs,
    ObEvalCtx *eval_ctx,
    ObHashJoinStoredJoinRow *&stored_row);
  int add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
                const ObBitVector &skip, const int64_t batch_size,
                const uint16_t selector[], const int64_t size,
                ObHashJoinStoredJoinRow **stored_rows = nullptr);
  int add_row(const ObHashJoinStoredJoinRow *src_stored_row, ObHashJoinStoredJoinRow *&stored_row);
  int init();
  int open();
  int close();

  int finish_dump(bool memory_need_dump);
  int dump(bool all_dump, int64_t dumped_size);

  bool has_next() { return store_iter_.has_next(); }
  int set_iterator();
  int init_progressive_iterator();

  void set_part_level(int32_t part_level) { part_level_ = part_level; }
  void set_batchno(int64_t batchno) { batchno_ = batchno; }

  int32_t get_part_level() const { return part_level_; }
  int64_t get_batchno() const { return batchno_; }
  int64_t get_part_shift() const { return part_shift_; }

  int64_t get_cur_row_count() { return n_get_rows_; }

  void set_callback(ObSqlMemoryCallback *callback) { inner_callback_ = callback; }

  int load_next_chunk();
  int rescan();

  int64_t get_row_count_in_memory() { return chunk_row_store_.get_row_cnt_in_memory(); }
  int64_t get_row_count_on_disk() { return chunk_row_store_.get_row_cnt_on_disk(); }

  int64_t get_last_buffer_mem_size() { return chunk_row_store_.get_last_buffer_mem_size(); }
  int64_t get_size_in_memory() { return chunk_row_store_.get_mem_used(); }
  int64_t get_size_on_disk() { return chunk_row_store_.get_file_size(); }
  int64_t get_cur_chunk_row_cnt() { return store_iter_.get_cur_chunk_row_cnt(); }
  void set_iteration_age(sql::ObChunkDatumStore::IterationAge &age) { store_iter_.set_iteration_age(&age); }

  bool has_switch_block() { return chunk_row_store_.get_block_list_cnt() > 1; }

  void set_pre_total_size(int64_t pre_total_size) { pre_total_size_ = pre_total_size; }
  void set_pre_bucket_number(int64_t pre_bucket_number) { pre_bucket_number_ = pre_bucket_number; }
  void set_pre_part_count(int64_t pre_part_count) { pre_part_count_ = pre_part_count; }
  int64_t get_pre_total_size() { return pre_total_size_; }
  int64_t get_pre_bucket_number() { return pre_bucket_number_; }
  int64_t get_pre_part_count() { return pre_part_count_; }

  void set_memory_limit(int64_t limit) { chunk_row_store_.set_mem_limit(limit); }
  bool is_dumped() { return 0 < chunk_row_store_.get_file_size(); }
  int64_t get_dump_size() { return chunk_row_store_.get_file_size(); }
  sql::ObChunkDatumStore &get_chunk_row_store()
  {
    return chunk_row_store_;
  }
private:
  sql::ObChunkDatumStore chunk_row_store_;
  sql::ObChunkDatumStore::Iterator store_iter_;
  sql::ObSqlMemoryCallback *inner_callback_;
  int32_t part_level_;
  int64_t part_shift_;
  int64_t batchno_; // high: batch_round low: part_id
  ObHashJoinBufMgr *buf_mgr_;
  uint64_t tenant_id_;
  int64_t n_get_rows_;
  int64_t n_add_rows_;

  int64_t pre_total_size_;
  int64_t pre_bucket_number_;
  int64_t pre_part_count_;
};

struct ObHashJoinBatchPair
{
  ObHashJoinBatch *left_;
  ObHashJoinBatch *right_;

  ObHashJoinBatchPair() : left_(NULL), right_(NULL) {}
};

class ObHashJoinBatchMgr {
public:
  ObHashJoinBatchMgr(common::ObIAllocator &alloc, ObHashJoinBufMgr *buf_mgr, uint64_t tenant_id) :
    total_dump_count_(0),
    total_dump_size_(0),
    batch_count_(0),
    tenant_id_(tenant_id),
    alloc_(alloc),
    batch_list_(alloc),
    buf_mgr_(buf_mgr)
  {}

  virtual ~ObHashJoinBatchMgr();
  void reset();

  typedef common::ObList<ObHashJoinBatchPair, common::ObIAllocator> hj_batch_pair_list_type;

  int next_batch(ObHashJoinBatchPair &batch_pair);

  int64_t get_batch_list_size() { return batch_list_.size(); }
  int remove_undumped_batch(int64_t cur_dumped_partition,
                            int32_t batch_round);
  int get_or_create_batch(int32_t level,
                          int64_t part_shift,
                          int64_t batchno,
                          bool is_left,
                          ObHashJoinBatch *&batch,
                          bool only_get = false);

  void free(ObHashJoinBatch *batch) {
    if (NULL != batch) {
      batch->~ObHashJoinBatch();
      alloc_.free(batch);
      batch = NULL;
      batch_count_ --;
    }
  }

public:
  int64_t total_dump_count_;
  int64_t total_dump_size_;
  int64_t batch_count_;

private:
  static const int64_t PARTITION_IDX_MASK = 0x00000000FFFFFFFF;
  uint64_t tenant_id_;
  common::ObIAllocator &alloc_;
  hj_batch_pair_list_type batch_list_;
  ObHashJoinBufMgr *buf_mgr_;
};

class ObHashJoinPartition
{
public:
  ObHashJoinPartition() :
    buf_mgr_(nullptr),
    batch_mgr_(nullptr),
    batch_(nullptr),
    part_level_(-1),
    part_id_(-1)
    {}

  ObHashJoinPartition(ObHashJoinBufMgr *buf_mgr, ObHashJoinBatchMgr *batch_mgr) :
    buf_mgr_(buf_mgr),
    batch_mgr_(batch_mgr),
    batch_(nullptr),
    part_level_(-1),
    part_id_(-1)
    {}

  virtual ~ObHashJoinPartition()
  {
    reset();
  }

  void set_hj_buf_mgr(ObHashJoinBufMgr *buf_mgr) {
    buf_mgr_ = buf_mgr;
  }

  void set_hj_batch_mgr(ObHashJoinBatchMgr *batch_mgr) {
    batch_mgr_ = batch_mgr;
  }

  int init(
    int32_t part_level,
    int64_t part_shift,
    int32_t part_id,
    int32_t batch_round,
    bool is_left,
    ObHashJoinBufMgr *buf_mgr,
    ObHashJoinBatchMgr *batch_mgr,
    ObHashJoinBatch* pre_batch,
    ObOperator *child_op,
    ObSqlMemoryCallback *callback,
    int64_t dir_id,
    ObIOEventObserver *io_event_observer);

  int get_next_row(const ObHashJoinStoredJoinRow *&stored_row);
  inline int get_next_batch(const ObHashJoinStoredJoinRow **stored_row,
                            const int64_t max_rows,
                            int64_t &read_rows);

  // payload of ObRowStore::StoredRow will be set after added.
  int add_row(
    const ObIArray<ObExpr*> &exprs, ObEvalCtx *eval_ctx, ObHashJoinStoredJoinRow *&stored_row);
  int add_row(const ObHashJoinStoredJoinRow *src_stored_row, ObHashJoinStoredJoinRow *&stored_row);
  int add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
                const ObBitVector &skip, const int64_t batch_size,
                const uint16_t selector[], const int64_t size,
                ObHashJoinStoredJoinRow **stored_rows = nullptr);
  int finish_dump(bool memory_need_dump);
  int dump(bool all_dump, int64_t dumped_size);

  int init_iterator();
  int init_progressive_iterator();

  void set_part_level(int32_t part_level) { part_level_ = part_level; }
  void set_part_id(int32_t part_id) { part_id_ = part_id; }

  int32_t get_part_level() { return part_level_; }
  int32_t get_part_id() { return part_id_; }
  bool is_dumped() { return batch_->is_dumped(); } //需要实现

  int check();
  void reset();

  ObHashJoinBatch *get_batch() { return batch_; }

  bool has_switch_block() { return batch_->has_switch_block(); }

  int64_t get_row_count_in_memory() { return batch_->get_row_count_in_memory(); }
  int64_t get_row_count_on_disk() { return batch_->get_row_count_on_disk(); }

  int64_t get_size_in_memory() { return batch_->get_size_in_memory(); }
  int64_t get_size_on_disk() { return batch_->get_size_on_disk(); }

  int64_t get_last_buffer_mem_size() { return batch_->get_last_buffer_mem_size(); }

  int record_pre_batch_info(int64_t pre_part_count, int64_t pre_bucket_number, int64_t total_size);
private:
  ObHashJoinBufMgr *buf_mgr_;
  ObHashJoinBatchMgr *batch_mgr_;
  ObHashJoinBatch *batch_;
  int32_t part_level_;
  int32_t part_id_;
};

int ObHashJoinPartition::get_next_batch(const ObHashJoinStoredJoinRow **stored_row,
                                        const int64_t max_rows,
                                        int64_t &read_rows)
{
  return batch_->get_next_batch(stored_row, max_rows, read_rows);
}

} // end namespace sql
} // end namespace oceanbase

#endif /* SRC_SQL_ENGINE_JOIN_OB_HASH_JOIN_BASIC_H_ */


