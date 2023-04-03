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

#ifndef _OB_HJ_BATCH_H
#define _OB_HJ_BATCH_H 1

#include "common/row/ob_row_store.h"
#include "sql/engine/join/ob_hj_buf_mgr.h"
#include "sql/engine/basic/ob_chunk_row_store.h"

namespace oceanbase
{
namespace sql
{
namespace join
{

struct ObStoredJoinRow;
class ObHJBatch {
public:
  ObHJBatch(common::ObIAllocator &alloc, ObHJBufMgr *buf_mgr, uint64_t tenant_id, int32_t part_level, int64_t part_shift, int32_t batchno) :
    chunk_row_store_(&alloc),
    inner_callback_(nullptr),
    part_level_(part_level),
    part_shift_(part_shift),
    batchno_(batchno),
    is_chunk_iter_(false),
    buf_mgr_(buf_mgr),
    tenant_id_(tenant_id),
    n_get_rows_(0),
    n_add_rows_(0),
    pre_total_size_(0),
    pre_bucket_number_(0),
    pre_part_count_(0)
  {}

  virtual ~ObHJBatch();
  int get_next_row(const common::ObNewRow *&row, const ObStoredJoinRow *&stored_row);
  int get_next_row(const ObStoredJoinRow *&stored_row);
  int convert_row(const common::ObNewRow *&row, const ObStoredJoinRow *&stored_row);
  int add_row(const common::ObNewRow &row, ObStoredJoinRow *&stored_row);
  int init();
  int open();
  int close();

  int finish_dump(bool memory_need_dump);
  int dump(bool all_dump);

  bool has_next() { return chunk_iter_.has_next_chunk(); }
  int set_iterator(bool is_chunk_iter);

  void set_part_level(int32_t part_level) { part_level_ = part_level; }
  void set_batchno(int32_t batchno) { batchno_ = batchno; }

  int32_t get_part_level() const { return part_level_; }
  int32_t get_batchno() const { return batchno_; }
  int64_t get_part_shift() const { return part_shift_; }

  int64_t get_cur_row_count() { return n_get_rows_; }

  void set_callback(ObSqlMemoryCallback *callback) { inner_callback_ = callback; }

  int load_next_chunk();
  int rescan();

  int64_t get_row_count_in_memory() { return chunk_row_store_.get_row_cnt_in_memory(); }
  int64_t get_row_count_on_disk() { return chunk_row_store_.get_row_cnt_on_disk(); }

  int64_t get_size_in_memory() { return chunk_row_store_.get_mem_used(); }
  int64_t get_size_on_disk() { return chunk_row_store_.get_file_size(); }
  int64_t get_cur_chunk_row_cnt() { return chunk_iter_.get_cur_chunk_row_cnt(); }
  int64_t get_cur_chunk_size() { return chunk_iter_.get_chunk_read_size();}

  void set_pre_total_size(int64_t pre_total_size) { pre_total_size_ = pre_total_size; }
  void set_pre_bucket_number(int64_t pre_bucket_number) { pre_bucket_number_ = pre_bucket_number; }
  void set_pre_part_count(int64_t pre_part_count) { pre_part_count_ = pre_part_count; }
  int64_t get_pre_total_size() { return pre_total_size_; }
  int64_t get_pre_bucket_number() { return pre_bucket_number_; }
  int64_t get_pre_part_count() { return pre_part_count_; }

  void set_memory_limit(int64_t limit) { chunk_row_store_.set_mem_limit(limit); }
  bool is_dumped() { return 0 < chunk_row_store_.get_file_size(); }
  int64_t get_dump_size() { return chunk_row_store_.get_file_size(); }
  sql::ObChunkRowStore &get_chunk_row_store()
  {
    return chunk_row_store_;
  }
private:
  sql::ObChunkRowStore chunk_row_store_;
  sql::ObChunkRowStore::RowIterator row_store_iter_;
  sql::ObChunkRowStore::ChunkIterator chunk_iter_;
  sql::ObSqlMemoryCallback *inner_callback_;
  int32_t part_level_;
  int64_t part_shift_;
  int32_t batchno_;
  bool is_chunk_iter_;
  ObHJBufMgr *buf_mgr_;
  uint64_t tenant_id_;
  int64_t n_get_rows_;
  int64_t n_add_rows_;

  int64_t pre_total_size_;
  int64_t pre_bucket_number_;
  int64_t pre_part_count_;
};


}
}
}

#endif /* _OB_HJ_BATCH_H */


