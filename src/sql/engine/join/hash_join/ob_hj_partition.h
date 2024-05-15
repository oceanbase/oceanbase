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

#ifndef SRC_SQL_ENGINE_JOIN_HASH_JOIN_OB_HJ_PARTITION_H_
#define SRC_SQL_ENGINE_JOIN_HASH_JOIN_OB_HJ_PARTITION_H_

#include "sql/engine/basic/ob_temp_row_store.h"
namespace oceanbase
{
namespace sql
{
class ObHJStoredRow;
class ObEvalCtx;
class ObExpr;

class ObHJPartition {
public:
  ObHJPartition(
      common::ObIAllocator &alloc,
      uint64_t tenant_id,
      int32_t part_level,
      int64_t part_shift,
      int64_t partno)
  : row_store_(&alloc),
    store_iter_(),
    part_level_(part_level),
    part_shift_(part_shift),
    partno_(partno),
    tenant_id_(tenant_id),
    n_get_rows_(0),
    n_add_rows_(0),
    pre_total_size_(0),
    pre_bucket_number_(0),
    pre_part_count_(0)
  {}

  virtual ~ObHJPartition();
  int get_next_batch(const common::ObIArray<ObExpr*> &exprs,
                     ObEvalCtx &ctx,
                     const int64_t max_rows,
                     int64_t &read_rows,
                     const ObHJStoredRow **stored_row);
  int get_next_batch(const ObHJStoredRow **stored_row,
                     const int64_t max_rows,
                     int64_t &read_rows);

  int add_batch(const common::IVectorPtrs &vectors,
                const uint16_t selector[],
                const int64_t size,
                ObHJStoredRow **stored_rows = nullptr);

  int add_row(const common::ObIArray<ObExpr*> &exprs,
              ObEvalCtx &eval_ctx,
              ObCompactRow *&stored_row) {
    return row_store_.add_row(exprs, eval_ctx, stored_row);
  }

  int add_row(const ObHJStoredRow *src, ObCompactRow *&stored_row) {
    return row_store_.add_row(reinterpret_cast<const ObCompactRow *>(src), stored_row);
  }

  int init(const ObExprPtrIArray &exprs, const int64_t max_batch_size,
           const common::ObCompressorType compressor_type);
  int open();
  void close();

  int finish_dump(bool memory_need_dump);
  int dump(bool all_dump, int64_t dumped_size);

  bool has_next() { return store_iter_.has_next(); }
  int begin_iterator();
  int init_progressive_iterator();

  void set_part_level(int32_t part_level) { part_level_ = part_level; }
  void set_batchno(int64_t partno) { partno_ = partno; }

  int32_t get_part_level() const { return part_level_; }
  int64_t get_partno() const { return partno_; }
  int64_t get_part_shift() const { return part_shift_; }

  int64_t get_cur_row_count() { return n_get_rows_; }

  int rescan();

  int64_t get_row_count_in_memory() { return row_store_.get_row_cnt_in_memory(); }
  int64_t get_row_count_on_disk() { return row_store_.get_row_cnt_on_disk(); }

  int64_t get_last_buffer_mem_size() { return row_store_.get_last_buffer_mem_size(); }
  int64_t get_size_in_memory() { return row_store_.get_mem_used(); }
  int64_t get_size_on_disk() { return row_store_.get_file_size(); }

  void set_iteration_age(sql::ObTempRowStore::IterationAge &age) {
    store_iter_.set_iteration_age(&age);
  }

  bool has_switch_block() { return row_store_.get_block_list_cnt() > 1; }

  void set_pre_total_size(int64_t pre_total_size) { pre_total_size_ = pre_total_size; }
  void set_pre_bucket_number(int64_t pre_bucket_number) { pre_bucket_number_ = pre_bucket_number; }
  void set_pre_part_count(int64_t pre_part_count) { pre_part_count_ = pre_part_count; }
  int64_t get_pre_total_size() { return pre_total_size_; }
  int64_t get_pre_bucket_number() { return pre_bucket_number_; }
  int64_t get_pre_part_count() { return pre_part_count_; }

  void set_memory_limit(int64_t limit) { row_store_.set_mem_limit(limit); }
  bool is_dumped() { return 0 < row_store_.get_file_size(); }
  int64_t get_dump_size() { return row_store_.get_file_size(); }
  ObTempRowStore &get_row_store()
  {
    return row_store_;
  }
  void record_pre_batch_info(int64_t pre_part_count, int64_t pre_bucket_number, int64_t total_size)
  {
    set_pre_part_count(pre_part_count);
    set_pre_bucket_number(pre_bucket_number);
    set_pre_total_size(total_size);
  }

private:
  ObTempRowStore row_store_;
  ObTempRowStore::Iterator store_iter_;
  int32_t part_level_;
  int64_t part_shift_;
  int64_t partno_; // high: batch_round low: part_id
  uint64_t tenant_id_;
  int64_t n_get_rows_;
  int64_t n_add_rows_;

  int64_t pre_total_size_;
  int64_t pre_bucket_number_;
  int64_t pre_part_count_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* SRC_SQL_ENGINE_JOIN_HASH_JOIN_OB_HJ_PARTITION_H_*/
