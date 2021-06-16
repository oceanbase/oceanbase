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

namespace oceanbase {
namespace sql {

class ObHashJoinBufMgr : public ObSqlMemoryCallback {
public:
  ObHashJoinBufMgr()
      : reserve_memory_size_(0),
        pre_total_alloc_size_(0),
        total_alloc_size_(0),
        total_left_alloc_size_(0),
        page_size_(-1),
        dumped_size_(0)
  {}

  inline void set_page_size(int64_t page_size)
  {
    page_size_ = page_size;
  }
  inline int64_t get_page_size()
  {
    return page_size_;
  }

  void set_reserve_memory_size(int64_t reserve_memory_size)
  {
    reserve_memory_size_ = reserve_memory_size;
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

  // OB_INLINE void finish_left_dump() { total_left_alloc_size_ = total_alloc_size_; }

  OB_INLINE bool need_dump()
  {
    return reserve_memory_size_ * RATIO / 100 < total_alloc_size_;
  }

private:
  const static int64_t RATIO = 80;
  int64_t reserve_memory_size_;
  int64_t pre_total_alloc_size_;
  int64_t total_alloc_size_;
  int64_t total_left_alloc_size_;
  int64_t page_size_;
  int64_t dumped_size_;
};

struct ObHashJoinStoredJoinRow : public sql::ObChunkDatumStore::StoredRow {
  const static int64_t HASH_VAL_BIT = 63;
  const static int64_t HASH_VAL_MASK = UINT64_MAX >> (64 - HASH_VAL_BIT);
  struct ExtraInfo {
    uint64_t hash_val_ : HASH_VAL_BIT;
    uint64_t is_match_ : 1;
  };

  ExtraInfo& get_extra_info()
  {
    static_assert(sizeof(ObHashJoinStoredJoinRow) == sizeof(sql::ObChunkDatumStore::StoredRow),
        "sizeof StoredJoinRow must be the save with StoredRow");
    return *reinterpret_cast<ExtraInfo*>(get_extra_payload());
  }
  const ExtraInfo& get_extra_info() const
  {
    return *reinterpret_cast<const ExtraInfo*>(get_extra_payload());
  }

  uint64_t get_hash_value() const
  {
    return get_extra_info().hash_val_;
  }
  void set_hash_value(const uint64_t hash_val)
  {
    get_extra_info().hash_val_ = hash_val & HASH_VAL_MASK;
  }
  bool is_match() const
  {
    return get_extra_info().is_match_;
  }
  void set_is_match(bool is_match)
  {
    get_extra_info().is_match_ = is_match;
  }
};

class ObHashJoinBatch {
public:
  ObHashJoinBatch(
      common::ObIAllocator& alloc, ObHashJoinBufMgr* buf_mgr, uint64_t tenant_id, int32_t part_level, int32_t batchno)
      : alloc_(alloc),
        chunk_row_store_(&alloc),
        inner_callback_(nullptr),
        part_level_(part_level),
        batchno_(batchno),
        is_chunk_iter_(false),
        buf_mgr_(buf_mgr),
        tenant_id_(tenant_id),
        timeout_ms_(1000000),
        n_get_rows_(0),
        n_add_rows_(0),
        pre_total_size_(0),
        pre_bucket_number_(0),
        pre_part_count_(0)
  {}

  virtual ~ObHashJoinBatch();
  int get_next_row(
      const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx, const ObHashJoinStoredJoinRow*& stored_row);
  int get_next_row(const ObHashJoinStoredJoinRow*& stored_row);
  int get_next_block_row(const ObHashJoinStoredJoinRow*& stored_row);
  int convert_row(
      const ObHashJoinStoredJoinRow* stored_row, const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx);
  int add_row(const common::ObIArray<ObExpr*>& exprs, ObEvalCtx* eval_ctx, ObHashJoinStoredJoinRow*& stored_row);
  int add_row(const ObHashJoinStoredJoinRow* src_stored_row, ObHashJoinStoredJoinRow*& stored_row);
  int init();
  int open();
  int close();

  int finish_dump(bool memory_need_dump);
  int dump(bool all_dump);

  bool has_next()
  {
    return chunk_iter_.has_next_chunk();
  }
  int set_iterator(bool is_chunk_iter);
  int init_progressive_iterator();

  void set_part_level(int32_t part_level)
  {
    part_level_ = part_level;
  }
  void set_batchno(int32_t batchno)
  {
    batchno_ = batchno;
  }

  int32_t get_part_level() const
  {
    return part_level_;
  }
  int32_t get_batchno() const
  {
    return batchno_;
  }

  int64_t get_cur_row_count()
  {
    return n_get_rows_;
  }

  void set_callback(ObSqlMemoryCallback* callback)
  {
    inner_callback_ = callback;
  }

  int load_next_chunk();
  int rescan();

  int64_t get_row_count_in_memory()
  {
    return chunk_row_store_.get_row_cnt_in_memory();
  }
  int64_t get_row_count_on_disk()
  {
    return chunk_row_store_.get_row_cnt_on_disk();
  }

  int64_t get_size_in_memory()
  {
    return chunk_row_store_.get_mem_used();
  }
  int64_t get_size_on_disk()
  {
    return chunk_row_store_.get_file_size();
  }
  int64_t get_cur_chunk_row_cnt()
  {
    return chunk_iter_.get_cur_chunk_row_cnt();
  }
  int64_t get_cur_chunk_size()
  {
    return chunk_iter_.get_chunk_read_size();
  }

  bool has_switch_block()
  {
    return chunk_row_store_.get_block_list_cnt() > 1;
  }

  void set_pre_total_size(int64_t pre_total_size)
  {
    pre_total_size_ = pre_total_size;
  }
  void set_pre_bucket_number(int64_t pre_bucket_number)
  {
    pre_bucket_number_ = pre_bucket_number;
  }
  void set_pre_part_count(int64_t pre_part_count)
  {
    pre_part_count_ = pre_part_count;
  }
  int64_t get_pre_total_size()
  {
    return pre_total_size_;
  }
  int64_t get_pre_bucket_number()
  {
    return pre_bucket_number_;
  }
  int64_t get_pre_part_count()
  {
    return pre_part_count_;
  }

  void set_memory_limit(int64_t limit)
  {
    chunk_row_store_.set_mem_limit(limit);
  }
  bool is_dumped()
  {
    return 0 < chunk_row_store_.get_file_size();
  }
  int64_t get_dump_size()
  {
    return chunk_row_store_.get_file_size();
  }
  sql::ObChunkDatumStore& get_chunk_row_store()
  {
    return chunk_row_store_;
  }

private:
  const int64_t ROW_CNT_PER = 0x3FF;
  common::ObIAllocator& alloc_;
  sql::ObChunkDatumStore chunk_row_store_;
  sql::ObChunkDatumStore::RowIterator row_store_iter_;
  sql::ObChunkDatumStore::ChunkIterator chunk_iter_;
  sql::ObSqlMemoryCallback* inner_callback_;
  int32_t part_level_;
  int32_t batchno_;
  bool is_chunk_iter_;
  ObHashJoinBufMgr* buf_mgr_;
  uint64_t tenant_id_;
  int64_t timeout_ms_;
  int64_t n_get_rows_;
  int64_t n_add_rows_;

  int64_t pre_total_size_;
  int64_t pre_bucket_number_;
  int64_t pre_part_count_;
};

struct ObHashJoinBatchPair {
  ObHashJoinBatch* left_;
  ObHashJoinBatch* right_;

  ObHashJoinBatchPair() : left_(NULL), right_(NULL)
  {}
};

class ObHashJoinBatchMgr {
public:
  ObHashJoinBatchMgr(common::ObIAllocator& alloc, ObHashJoinBufMgr* buf_mgr, uint64_t tenant_id)
      : total_dump_count_(0),
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

  int next_batch(ObHashJoinBatchPair& batch_pair);

  int64_t get_batch_list_size()
  {
    return batch_list_.size();
  }
  int remove_undumped_batch();
  int get_or_create_batch(int32_t level, int32_t batchno, bool is_left, ObHashJoinBatch*& batch, bool only_get = false);

  void free(ObHashJoinBatch* batch)
  {
    if (NULL != batch) {
      batch->~ObHashJoinBatch();
      alloc_.free(batch);
      batch = NULL;
      batch_count_--;
    }
  }

public:
  int64_t total_dump_count_;
  int64_t total_dump_size_;
  int64_t batch_count_;

private:
  uint64_t tenant_id_;
  common::ObIAllocator& alloc_;
  hj_batch_pair_list_type batch_list_;
  ObHashJoinBufMgr* buf_mgr_;
};

class ObHashJoinPartition {
public:
  ObHashJoinPartition() : buf_mgr_(nullptr), batch_mgr_(nullptr), batch_(nullptr), part_level_(-1), part_id_(-1)
  {}

  ObHashJoinPartition(ObHashJoinBufMgr* buf_mgr, ObHashJoinBatchMgr* batch_mgr)
      : buf_mgr_(buf_mgr), batch_mgr_(batch_mgr), batch_(nullptr), part_level_(-1), part_id_(-1)
  {}

  virtual ~ObHashJoinPartition()
  {
    reset();
  }

  void set_hj_buf_mgr(ObHashJoinBufMgr* buf_mgr)
  {
    buf_mgr_ = buf_mgr;
  }

  void set_hj_batch_mgr(ObHashJoinBatchMgr* batch_mgr)
  {
    batch_mgr_ = batch_mgr;
  }

  int init(int32_t part_level, int32_t part_id, bool is_left, ObHashJoinBufMgr* buf_mgr, ObHashJoinBatchMgr* batch_mgr,
      ObHashJoinBatch* pre_batch, ObOperator* child_op, ObSqlMemoryCallback* callback, int64_t dir_id);

  int get_next_row(const ObHashJoinStoredJoinRow*& stored_row);
  int get_next_block_row(const ObHashJoinStoredJoinRow*& stored_row);

  // payload of ObRowStore::StoredRow will be set after added.
  int add_row(const ObIArray<ObExpr*>& exprs, ObEvalCtx* eval_ctx, ObHashJoinStoredJoinRow*& stored_row);
  int add_row(const ObHashJoinStoredJoinRow* src_stored_row, ObHashJoinStoredJoinRow*& stored_row);
  int finish_dump(bool memory_need_dump);
  int dump(bool all_dump);

  int init_iterator(bool is_chunk_iter);
  int init_progressive_iterator();

  void set_part_level(int32_t part_level)
  {
    part_level_ = part_level;
  }
  void set_part_id(int32_t part_id)
  {
    part_id_ = part_id;
  }

  int32_t get_part_level()
  {
    return part_level_;
  }
  int32_t get_part_id()
  {
    return part_id_;
  }
  bool is_dumped()
  {
    return batch_->is_dumped();
  }

  int check();
  void reset();

  ObHashJoinBatch* get_batch()
  {
    return batch_;
  }

  bool has_switch_block()
  {
    return batch_->has_switch_block();
  }

  int64_t get_row_count_in_memory()
  {
    return batch_->get_row_count_in_memory();
  }
  int64_t get_row_count_on_disk()
  {
    return batch_->get_row_count_on_disk();
  }

  int64_t get_size_in_memory()
  {
    return batch_->get_size_in_memory();
  }
  int64_t get_size_on_disk()
  {
    return batch_->get_size_on_disk();
  }

  int record_pre_batch_info(int64_t pre_part_count, int64_t pre_bucket_number, int64_t total_size);

private:
  ObHashJoinBufMgr* buf_mgr_;
  ObHashJoinBatchMgr* batch_mgr_;
  ObHashJoinBatch* batch_;
  int32_t part_level_;
  int32_t part_id_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* SRC_SQL_ENGINE_JOIN_OB_HASH_JOIN_BASIC_H_ */
