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
#include "sql/engine/join/ob_partition_store.h"

namespace oceanbase
{
namespace sql
{
class ObHJStoredRow;
class ObEvalCtx;
class ObExpr;
class ObPartitionStore;

class ObHJPartition {
public:
  ObHJPartition(
      common::ObIAllocator &alloc,
      uint64_t tenant_id,
      int32_t part_level,
      int64_t part_shift,
      int64_t partno)
  : part_level_(part_level),
    part_shift_(part_shift),
    partno_(partno),
    tenant_id_(tenant_id),
    pre_total_size_(0),
    pre_bucket_number_(0),
    pre_part_count_(0)
  {}

  virtual ~ObHJPartition() {
    close();
  }
  inline int get_next_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
                            const int64_t max_rows, int64_t &read_rows,
                            const ObHJStoredRow **stored_rows)
  {
    return partition_store_->get_next_batch(exprs, ctx, max_rows, read_rows,
                                            reinterpret_cast<const ObCompactRow **>(stored_rows));
  }

  inline int get_next_batch(const ObHJStoredRow **stored_rows, const int64_t max_rows,
                            int64_t &read_rows)
  {
    return partition_store_->get_next_batch(reinterpret_cast<const ObCompactRow **>(stored_rows),
                                            max_rows, read_rows);
  }

  inline int add_batch(const common::IVectorPtrs &vectors, const uint16_t selector[],
                       const int64_t size, ObHJStoredRow **stored_rows = nullptr)
  {
    return partition_store_->add_batch(vectors, selector, size,
                                       reinterpret_cast<ObCompactRow **>(stored_rows));
  }

  int add_row(const common::ObIArray<ObExpr*> &exprs,
              ObEvalCtx &eval_ctx,
              ObCompactRow *&stored_row) {
    return partition_store_->add_row(exprs, eval_ctx, stored_row);
  }

  int add_row(const ObHJStoredRow *src, ObCompactRow *&stored_row)
  {
    return partition_store_->add_row(reinterpret_cast<const ObCompactRow *>(src), stored_row);
  }

  inline int init(const ObExprPtrIArray &exprs, const int64_t max_batch_size,
                  const common::ObCompressorType compressor_type, uint32_t extra_size)
  {
    return partition_store_->init(exprs, max_batch_size, compressor_type, extra_size);
  }

  inline int open() {
    return partition_store_->open();
  }

  inline void close() {
    if (OB_NOT_NULL(partition_store_)) {
      partition_store_->close();
    }
  }

  inline int rescan() {
    return partition_store_->rescan();
  }

  inline int finish_dump(bool memory_need_dump) {
    return partition_store_->finish_dump(memory_need_dump);
  }

  inline int dump(bool all_dump, int64_t dumped_size) {
    return partition_store_->dump(all_dump, dumped_size);
  }

  inline bool has_next() { return partition_store_->has_next(); }

  inline int begin_iterator() {
    return partition_store_->begin_iterator();
  }

  void set_part_level(int32_t part_level) { part_level_ = part_level; }
  void set_partno(int64_t partno) { partno_ = partno; }

  int32_t get_part_level() const { return part_level_; }
  int64_t get_partno() const { return partno_; }
  int64_t get_part_shift() const { return part_shift_; }

  int64_t get_cur_row_count() { return partition_store_->get_cur_row_count(); }

  int64_t get_row_count_in_memory() { return partition_store_->get_row_count_in_memory(); }
  int64_t get_row_count_on_disk() { return partition_store_->get_row_count_on_disk(); }

  int64_t get_last_buffer_mem_size() { return partition_store_->get_last_buffer_mem_size(); }
  int64_t get_size_in_memory() { return partition_store_->get_size_in_memory(); }
  int64_t get_size_on_disk() { return partition_store_->get_size_on_disk(); }

  inline void set_iteration_age(sql::ObTempRowStore::IterationAge &age) {
    partition_store_->set_iteration_age(age);
  }

  bool has_switch_block() { return partition_store_->has_switch_block(); }

  void set_pre_total_size(int64_t pre_total_size) { pre_total_size_ = pre_total_size; }
  void set_pre_bucket_number(int64_t pre_bucket_number) { pre_bucket_number_ = pre_bucket_number; }
  void set_pre_part_count(int64_t pre_part_count) { pre_part_count_ = pre_part_count; }
  int64_t get_pre_total_size() { return pre_total_size_; }
  int64_t get_pre_bucket_number() { return pre_bucket_number_; }
  int64_t get_pre_part_count() { return pre_part_count_; }

  inline void set_memory_limit(int64_t limit) { partition_store_->set_memory_limit(limit); }
  inline bool is_dumped() { return 0 < partition_store_->is_dumped(); }
  inline int64_t get_dump_size() { return partition_store_->get_dump_size(); }
  ObTempRowStore &get_row_store() { return partition_store_->get_row_store(); }
  void record_pre_batch_info(int64_t pre_part_count, int64_t pre_bucket_number, int64_t total_size)
  {
    set_pre_part_count(pre_part_count);
    set_pre_bucket_number(pre_bucket_number);
    set_pre_total_size(total_size);
  }

  void set_partition_store(ObPartitionStore *partition_store) {
    partition_store_ = partition_store;
  }

  ObPartitionStore *get_partition_store() {
    return partition_store_;
  }

private:
  ObPartitionStore *partition_store_{nullptr};
  int32_t part_level_;
  int64_t part_shift_;
  int64_t partno_; // high: batch_round low: part_id
  uint64_t tenant_id_;

  int64_t pre_total_size_;
  int64_t pre_bucket_number_;
  int64_t pre_part_count_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* SRC_SQL_ENGINE_JOIN_HASH_JOIN_OB_HJ_PARTITION_H_*/
