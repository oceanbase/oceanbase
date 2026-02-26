/** * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "lib/rc/context.h"
#include "lib/utility/ob_hyperloglog.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/join/ob_partition_store.h"
#include "sql/engine/join/ob_join_filter_store_row.h"
#include "src/sql/engine/ob_sql_mem_mgr_processor.h"


namespace oceanbase
{
namespace sql
{

class ObSqlMemMgrProcessor;
class ObIOEventObserver;
struct ObBatchRows;
class ObJoinFilterOp;
class ObJoinFilterMaterialGroupController;

class MarkHashValueOP
{
public:
  MarkHashValueOP(uint64_t *batch_hash_values) : batch_hash_values_(batch_hash_values)
  {}
  OB_INLINE int operator()(const int64_t i)
  {
    batch_hash_values_[i] = batch_hash_values_[i] & ObHJStoredRow::HASH_VAL_MASK;
    return OB_SUCCESS;
  }

private:
  uint64_t *batch_hash_values_;
};

class MarkHashValueAndSetHyperLogLogOP
{
public:
  MarkHashValueAndSetHyperLogLogOP(uint64_t *batch_hash_values, ObHyperLogLogCalculator* hllc) : batch_hash_values_(batch_hash_values), hllc_(hllc)
  {}
  OB_INLINE int operator()(const int64_t i)
  {
    hllc_->set(batch_hash_values_[i]);
    batch_hash_values_[i] = batch_hash_values_[i] & ObHJStoredRow::HASH_VAL_MASK;
    return OB_SUCCESS;
  }

private:
  uint64_t *batch_hash_values_;
  ObHyperLogLogCalculator* hllc_{nullptr};
};

class ObJoinFilterMaxAvailableMemChecker
{
public:
  explicit ObJoinFilterMaxAvailableMemChecker(int64_t cur_cnt) : cur_cnt_(cur_cnt)
  {}
  bool operator()(int64_t max_row_count)
  {
    return cur_cnt_ > max_row_count;
  }

private:
  int64_t cur_cnt_;
};

class ObJoinFilterExtendMaxMemChecker
{
public:
  explicit ObJoinFilterExtendMaxMemChecker(int64_t cur_memory_size)
      : cur_memory_size_(cur_memory_size)
  {}
  bool operator()(int64_t max_memory_size)
  {
    return cur_memory_size_ > max_memory_size;
  }

private:
  int64_t cur_memory_size_;
};

class ObJoinFilterPartitionSplitter

{
public:
  ObJoinFilterPartitionSplitter() {}
  ~ObJoinFilterPartitionSplitter();

  int init(int64_t tenant_id, lib::MemoryContext mem_context, ObEvalCtx &eval_ctx,
           ObSqlMemMgrProcessor *sql_mem_processor, const ObExprPtrIArray &hash_join_hash_exprs,
           const ObExprPtrIArray &output_exprs, uint16_t extra_hash_count, int64_t max_batch_size,
           const common::ObCompressorType compress_type);
  void reuse_for_rescan();

  inline int64_t get_total_row_count() { return total_row_count_; }
  int64_t get_row_count_in_memory();
  inline int64_t get_part_count() { return part_count_; }
  inline int64_t get_first_dumped_part_idx() { return first_dumped_part_idx_; }
  ObPartitionStore *get_partition(int64_t i) { return partition_array_.at(i); }

  int prepare_join_partitions(ObIOEventObserver *io_event_observer, int64_t row_count,
                              int64_t global_mem_bound_size);

  int calc_partition_hash_value(const ObBatchRows *brs, ObEvalCtx &eval_ctx,
                                uint64_t *hash_join_hash_values,
                                bool use_hllc_estimate_ndv,
                                ObHyperLogLogCalculator* hllc,
                                const bool enable_skip_null = false,
                                const common::ObIArray<bool> *need_null_flags = nullptr);

  int add_batch(const ObJoinFilterMaterialGroupController &group_controller,
                uint64_t *hash_join_hash_values, const ObBatchRows &brs, int32_t part_shift);
  int calc_part_selector(uint64_t *hash_vals, const ObBatchRows &brs, int32_t part_shift);

  int dump_from_back_to_front(int64_t need_dump_size);
  int finish_add_row();
  int force_dump_all_partition();

public:
  static int64_t calc_partition_count_by_cache_aware(int64_t row_count,
                                                     int64_t global_mem_bound_size);

private:
  int create_partitions(ObIOEventObserver *io_event_observer, const ObExprPtrIArray *exprs);
  inline bool need_dump() const
  {
    return sql_mem_processor_->get_data_size() > sql_mem_processor_->get_mem_bound();
  }

  template <bool StoreJoinFilterHashValue>
  int add_batch_inner(const ObBatchRows &brs,
                      const ObJoinFilterMaterialGroupController &group_controller,
                      uint64_t *hash_join_hash_values);

private:
  BatchTempRowStoresMgr stores_mgr_;
  int64_t tenant_id_{0};
  int64_t max_batch_size_{0};
  int64_t part_count_{0};
  int64_t total_row_count_{0};
  int64_t first_dumped_part_idx_{0};
  common::ObCompressorType compress_type_{NONE_COMPRESSOR};

  ObSqlMemMgrProcessor *sql_mem_processor_{nullptr};
  // used to calculate partition hash value
  const ObExprPtrIArray *hash_join_hash_exprs_{nullptr};
  // all output exprs
  const ObExprPtrIArray *output_exprs_{nullptr};
  // hash value of hash join and join filter is in extra
  int32_t row_extra_size_{0};
  RowMeta row_meta_;

  lib::MemoryContext mem_context_;
  common::ObIAllocator *arena_alloc_{nullptr};
  common::ObIAllocator *malloc_alloc_{nullptr};

  // allocated by arena_alloc_
  ObJoinFilterStoreRow **part_added_rows_{nullptr};
  ObBitVector *null_skip_bitmap_{nullptr};

  // allocated by malloc_alloc_
  ObFixedArray<ObPartitionStore *, common::ObIAllocator> partition_array_;
  ObFixedArray<ObIVector *, common::ObIAllocator> output_vectors_;
  int64_t *part_idxes_{nullptr};
};

}
} // namespace oceanbase
