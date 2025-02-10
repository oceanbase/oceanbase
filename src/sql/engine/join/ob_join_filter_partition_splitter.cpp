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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_join_filter_partition_splitter.h"
#include "sql/engine/join/hash_join/ob_hash_join_vec_op.h"
#include "sql/engine/join/ob_join_filter_op.h"


namespace oceanbase
{
namespace sql
{

class BitMapMergeOP
{
public:
  OB_INLINE uint64_t operator()(const uint64_t l, const uint64_t r)
  {
    return (l | r);
  }
};

class CalcPartSelectorOP
{
public:
  CalcPartSelectorOP(int64_t *part_idxes, int32_t part_shift,
                     uint64_t *hash_vals, int64_t part_count, int64_t max_batch_size)
      : part_idxes_(part_idxes),
        part_shift_(part_shift), hash_vals_(hash_vals), part_count_(part_count),
        max_batch_size_(max_batch_size)
  {}
  OB_INLINE int operator()(const int64_t i)
  {
    // copy from get_part_idx
    const int64_t part_idx = (hash_vals_[i] >> part_shift_) & (part_count_ - 1);
    part_idxes_[i] = part_idx;
    return OB_SUCCESS;
  }
private:
  int64_t *part_idxes_;
  int32_t part_shift_;
  uint64_t *hash_vals_;
  int64_t part_count_;
  int64_t max_batch_size_;
};

ObJoinFilterPartitionSplitter::~ObJoinFilterPartitionSplitter()
{
  if (partition_array_.count() > 0) {
    for (int64_t i = 0; i < partition_array_.count(); ++i) {
      partition_array_.at(i)->~ObPartitionStore();
    }
    // all ObPartitionStore are alloc at once, so free the first
    malloc_alloc_->free(partition_array_.at(0));
    partition_array_.reset();
  }
  if (OB_NOT_NULL(part_idxes_)) {
    malloc_alloc_->free(part_idxes_);
  }
  output_vectors_.reset();
  row_meta_.reset();
  stores_mgr_.reset();
}

int ObJoinFilterPartitionSplitter::init(int64_t tenant_id, lib::MemoryContext mem_context,
                                        ObEvalCtx &eval_ctx,
                                        ObSqlMemMgrProcessor *sql_mem_processor,
                                        const ObExprPtrIArray &hash_join_hash_exprs,
                                        const ObExprPtrIArray &output_exprs,
                                        uint16_t extra_hash_count, int64_t max_batch_size,
                                        const common::ObCompressorType compress_type)
{
  int ret = OB_SUCCESS;
  tenant_id_ = tenant_id;
  mem_context_ = mem_context;
  arena_alloc_ = &mem_context->get_arena_allocator();
  malloc_alloc_ = &mem_context->get_malloc_allocator();
  sql_mem_processor_ = sql_mem_processor;
  max_batch_size_ = max_batch_size;
  compress_type_ = compress_type;
  hash_join_hash_exprs_ = &hash_join_hash_exprs;
  output_exprs_ = &output_exprs;
  row_extra_size_ = extra_hash_count * sizeof(uint64_t);
  partition_array_.set_allocator(malloc_alloc_);
  row_meta_.set_allocator(malloc_alloc_);
  OZ(row_meta_.init(output_exprs, row_extra_size_));
  OZ(ObVecAllocUtil::vec_alloc_ptrs(arena_alloc_, part_added_rows_, sizeof(part_added_rows_) * max_batch_size_,
                       null_skip_bitmap_, ObBitVector::memory_size(max_batch_size_)));

  if (OB_SUCC(ret)) {
    output_vectors_.set_allocator(malloc_alloc_);
    OZ(output_vectors_.prepare_allocate(output_exprs.count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < output_exprs.count(); ++i) {
      output_vectors_.at(i) = output_exprs.at(i)->get_vector(eval_ctx);
    }
  }
  return ret;
}

void ObJoinFilterPartitionSplitter::reuse_for_rescan()
{
  // only reset data, not reset partition rule
  total_row_count_ = 0;
  for (int64_t i = 0; i < part_count_; ++i) {
    partition_array_.at(i)->close();
  }
}

int64_t ObJoinFilterPartitionSplitter::get_row_count_in_memory()
{
  int64_t count = 0;
  for (int64_t i = 0; i < part_count_; ++i) {
    count += partition_array_.at(i)->get_row_count_in_memory();
  }
  return count;
}

int ObJoinFilterPartitionSplitter::prepare_join_partitions(ObIOEventObserver *io_event_observer,
                                                         int64_t row_count,
                                                         int64_t global_mem_bound_size)
{
  int ret = OB_SUCCESS;
  part_count_ = calc_partition_count_by_cache_aware(row_count, global_mem_bound_size);
  first_dumped_part_idx_ = part_count_;
  if (OB_FAIL(create_partitions(io_event_observer, output_exprs_))) {
    LOG_WARN("failed to create_partitions", K(part_count_));
  } else if (OB_FAIL(stores_mgr_.init(max_batch_size_, part_count_, *malloc_alloc_))) {
    LOG_WARN("failed to init stores manager");
  } else {
    for (int64_t part_idx = 0; OB_SUCC(ret) && part_idx < part_count_; ++part_idx) {
      OZ(stores_mgr_.set_temp_store(part_idx, &partition_array_[part_idx]->get_row_store()));
    }
  }
  return ret;
}

int64_t ObJoinFilterPartitionSplitter::calc_partition_count_by_cache_aware(
    int64_t row_count, int64_t global_mem_bound_size)
{
  int64_t row_count_cache_aware = INIT_L2_CACHE_SIZE / ObHashJoinVecOp::PRICE_PER_ROW;
  int64_t partition_cnt = next_pow2(row_count / row_count_cache_aware);
  partition_cnt = min(partition_cnt, ObHashJoinVecOp::MAX_PART_COUNT_PER_LEVEL);
  global_mem_bound_size = max(0, global_mem_bound_size);
  while (partition_cnt * ObHashJoinVecOp::PAGE_SIZE > global_mem_bound_size) {
    partition_cnt >>= 1;
  }
  partition_cnt = partition_cnt < ObHashJoinVecOp::MIN_PART_COUNT ?
                      ObHashJoinVecOp::MIN_PART_COUNT :
                      partition_cnt;
  return partition_cnt;
}

int ObJoinFilterPartitionSplitter::create_partitions(ObIOEventObserver *io_event_observer,
                                                     const ObExprPtrIArray *exprs)
{
  int ret  = OB_SUCCESS;
  if (0 >= part_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition count is less then 0", K(part_count_), K(ret));
  } else {
    char *mem = nullptr;
    // create partition ptr array
    if (OB_FAIL(partition_array_.init(part_count_))) {
      LOG_WARN("failed to init partition_array_");
    } else {
      // create real partition
      char *mem = static_cast<char *>(malloc_alloc_->alloc(sizeof(ObPartitionStore) * part_count_));
      if (NULL == mem) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc mem for hj part");
      }
      for (int64_t i = 0; i < part_count_ && OB_SUCC(ret); i++) {
        ObPartitionStore *part_store =
            new (mem + sizeof(ObPartitionStore) * i) ObPartitionStore(tenant_id_, *malloc_alloc_);
        if (OB_FAIL(partition_array_.push_back(part_store))) {
          LOG_WARN("failed to push_back part_store");
        } else if (OB_FAIL(part_store->init(*exprs, max_batch_size_, compress_type_,
                                            row_extra_size_))) {
          LOG_WARN("failed to init partition");
        } else {
          part_store->get_row_store().set_dir_id(sql_mem_processor_->get_dir_id());
          part_store->get_row_store().set_io_event_observer(io_event_observer);
          part_store->get_row_store().set_callback(sql_mem_processor_);
        }
      }
    }

    // create part_idxes_
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(part_idxes_ = static_cast<int64_t *>(malloc_alloc_->alloc(
                        sizeof(int64_t) * max_batch_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(max_batch_size_));
      }
    }
  }
  return ret;
}

int ObJoinFilterPartitionSplitter::calc_partition_hash_value(
  const ObBatchRows *brs, ObEvalCtx &eval_ctx, uint64_t *hash_join_hash_values,
  bool use_hllc_estimate_ndv, ObHyperLogLogCalculator *hllc, const bool enable_skip_null,
  const common::ObIArray<bool> *need_null_flags)
{
  int ret = OB_SUCCESS;
  uint64_t seed = ObHashJoinVecOp::HASH_SEED;
  if (enable_skip_null) {
    null_skip_bitmap_->reset(brs->size_);
  } else {
    null_skip_bitmap_->deep_copy(*brs->skip_, brs->size_);
  }
  for (int64_t idx = 0; OB_SUCC(ret) && idx < hash_join_hash_exprs_->count() ; ++idx) {
    ObExpr *expr = hash_join_hash_exprs_->at(idx);
    if (OB_FAIL(expr->eval_vector(eval_ctx, *brs))) {
      LOG_WARN("eval failed", K(ret));
    } else {
      const bool is_batch_seed = (idx > 0);
      ObIVector *vector = expr->get_vector(eval_ctx);
      if (enable_skip_null) {
        if (vector->has_null() && !need_null_flags->at(idx)) {
          VectorFormat format = vector->get_format();
          if (is_uniform_format(format)) {
            const uint64_t idx_mask = VEC_UNIFORM_CONST == format ? 0 : UINT64_MAX;
            const ObDatum *datums = static_cast<ObUniformBase *>(vector)->get_datums();
            for (int64_t i = 0; i < brs->size_; i++) {
              if (datums[i & idx_mask].is_null()) {
                null_skip_bitmap_->set(i);
              }
            }
          } else {
            null_skip_bitmap_->bit_calculate(
                *null_skip_bitmap_, *static_cast<ObBitmapNullVectorBase *>(vector)->get_nulls(),
                brs->size_, BitMapMergeOP());
          }
        }
        brs->skip_->bit_calculate(*brs->skip_, *null_skip_bitmap_, brs->size_, BitMapMergeOP());
      }
      if (brs->all_rows_active_) {
        const_cast<ObBatchRows *>(brs)->all_rows_active_ =
            null_skip_bitmap_->is_all_false(brs->size_);
      }
      ret = vector->murmur_hash_v3(*expr, hash_join_hash_values, *brs->skip_,
                                    EvalBound(brs->size_, brs->all_rows_active_),
                                    is_batch_seed ? hash_join_hash_values : &seed,
                                    is_batch_seed);
    }
  }
  if (OB_SUCC(ret)) {
    if (use_hllc_estimate_ndv) {
      ObBitVector::flip_foreach(*brs->skip_, brs->size_,
                                MarkHashValueAndSetHyperLogLogOP(hash_join_hash_values, hllc));
    } else {
      ObBitVector::flip_foreach(*brs->skip_, brs->size_, MarkHashValueOP(hash_join_hash_values));
    }
  }
  return ret;
}

int ObJoinFilterPartitionSplitter::add_batch(
    const ObJoinFilterMaterialGroupController &group_controller, uint64_t *hash_join_hash_values,
    const ObBatchRows &brs, int32_t part_shift)
{
  int ret = OB_SUCCESS;
  uint16_t extra_hash_count = group_controller.extra_hash_count_;
  if (OB_FAIL(calc_part_selector(hash_join_hash_values, brs, part_shift))) {
    LOG_WARN("failed to calc_part_selector");
  }
  if (OB_SUCC(ret)) {
    if (extra_hash_count > 1) {
      ret = add_batch_inner<true>(brs, group_controller, hash_join_hash_values);
    } else {
      ret = add_batch_inner<false>(brs, group_controller, hash_join_hash_values);
    }
  }
  return ret;
}

template <bool StoreJoinFilterHashValue>
int ObJoinFilterPartitionSplitter::add_batch_inner(
    const ObBatchRows &brs, const ObJoinFilterMaterialGroupController &group_controller,
    uint64_t *hash_join_hash_values)
{
  int ret = OB_SUCCESS;
  uint64_t **group_join_filter_hash_values = group_controller.group_join_filter_hash_values_;
  uint16_t group_count = group_controller.group_count_;
  uint16_t *hash_id_map = group_controller.hash_id_map_;
  int16_t hash_id = 0;
  if (OB_FAIL(stores_mgr_.add_batch(part_idxes_, output_vectors_, brs,
                                    reinterpret_cast<ObCompactRow **>(part_added_rows_)))) {
    LOG_WARN("failed to add batch", K(ret));
  } else {
    for (int64_t i = 0; i < brs.size_; i++) {
      if (brs.skip_->at(i)) {
        continue;
      }
      ++total_row_count_;
      part_added_rows_[i]->set_is_match(row_meta_, false);
      part_added_rows_[i]->set_hash_value(row_meta_, hash_join_hash_values[i]);
      if (StoreJoinFilterHashValue) {
        for (int16_t group_idx = 0; group_idx < group_count; ++group_idx) {
          hash_id = hash_id_map[group_idx];
          if (hash_id == 0) {
            // reuse hash join hash value, do nothing
          } else {
            part_added_rows_[i]->set_join_filter_hash_value(
                row_meta_, hash_id, group_join_filter_hash_values[group_idx][i]);
          }
        }
      }
    }
  }
  return ret;
}

int ObJoinFilterPartitionSplitter::calc_part_selector(uint64_t *hash_vals,
                                                    const ObBatchRows &brs, int32_t part_shift)
{
  int ret = OB_SUCCESS;
  CalcPartSelectorOP op(part_idxes_, part_shift, hash_vals, part_count_, max_batch_size_);
  if (OB_FAIL(ObBitVector::flip_foreach(*brs.skip_, brs.size_, op))) {
    LOG_WARN("fail to convert skip to selector", K(ret));
  }
  return ret;
}

int ObJoinFilterPartitionSplitter::dump_from_back_to_front(int64_t need_dump_size)
{
  int ret = OB_SUCCESS;
  int64_t pre_total_dumped_size = 0;
  int64_t first_dumped_partition_idx = 0;
  bool dump_all = false;
  bool tmp_need_dump = false;
  // firstly find all need dumped partitions
  for (int64_t i = part_count_ - 1; i >= 0 && OB_SUCC(ret); --i) {
    ObPartitionStore *dump_part = partition_array_.at(i);
    // don't dump last buffer
    pre_total_dumped_size += dump_part->get_size_in_memory() - dump_part->get_last_buffer_mem_size();
    if (pre_total_dumped_size > need_dump_size) {
      first_dumped_partition_idx = i;
      break;
    }
  }
  // secondly dump one buffer per partition one by one
  bool finish_dump = false;
  while (OB_SUCC(ret) && !finish_dump) {
    finish_dump = true;
    for (int64_t i = first_dumped_partition_idx; i < part_count_ && OB_SUCC(ret); ++i) {
      ObPartitionStore *dump_part = partition_array_.at(i);
      bool has_been_dumped = dump_part->is_dumped();
      if (dump_part->has_switch_block() || 0 < dump_part->get_size_in_memory()) {
        if (OB_FAIL(dump_part->dump(dump_all, 1))) {
          LOG_WARN("failed to dump partition", K(i));
        } else if (dump_part->is_dumped() && !has_been_dumped) {
          // if this partition is first time be dumped, set limit 1 to let it auto dump later
          dump_part->set_memory_limit(1);
          sql_mem_processor_->set_number_pass(1);
          // recalculate available memory bound after dump one partition
          if (dump_all && sql_mem_processor_->is_auto_mgr()) {
            bool updated = false;
            ObJoinFilterMaxAvailableMemChecker max_available_mem_checker(get_row_count_in_memory());
            ObJoinFilterExtendMaxMemChecker extend_max_mem_checker(
                sql_mem_processor_->get_data_size());
            if (OB_FAIL(sql_mem_processor_->update_max_available_mem_size_periodically(
                    &mem_context_->get_malloc_allocator(), max_available_mem_checker, updated))) {
              LOG_WARN("failed to update remain memory size periodically", K(ret));
            } else if ((updated || need_dump())
                       && OB_FAIL(sql_mem_processor_->extend_max_memory_size(
                              &mem_context_->get_malloc_allocator(), extend_max_mem_checker,
                              dump_all, sql_mem_processor_->get_data_size()))) {
              LOG_WARN("fail to extend max memory size", K(ret), K(updated));
            }
          }
        }
      }
      if (OB_SUCC(ret) &&
          (dump_part->has_switch_block() || (dump_all && 0 < dump_part->get_size_in_memory()))) {
        finish_dump = false;
      }
    }
  }
  LOG_TRACE("debug finish dump partition", K(first_dumped_partition_idx));

  if (OB_SUCC(ret)) {
    first_dumped_part_idx_ = min(first_dumped_part_idx_, first_dumped_partition_idx);
  }
  return ret;
}

int ObJoinFilterPartitionSplitter::finish_add_row()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < part_count_ && OB_SUCC(ret); ++i) {
    ObPartitionStore *part = partition_array_.at(i);
    // consider this situation, during dump_from_back_to_front, partition 5, 6, 7, 8
    // are need to dump while partition 6 is empty, then partition 6 will not be dumped.
    // after all rows added, partition 6 has added some row but still not dumped, we need dump it.
    bool need_dump = i >= first_dumped_part_idx_;
    if (need_dump && !part->is_dumped()) {
      if ( OB_FAIL(part->dump(true, INT64_MAX))) {
        LOG_WARN("failed to dump partition", K(i));
      } else {
        part->set_memory_limit(1);
        sql_mem_processor_->set_number_pass(1);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(part->finish_add_row(need_dump))) {
      LOG_WARN("failed to finish add row", K(i), K(first_dumped_part_idx_), K(part_count_));
    }
  }
  return ret;
}

int ObJoinFilterPartitionSplitter::force_dump_all_partition()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < part_count_ && OB_SUCC(ret); ++i) {
    ObPartitionStore *part = partition_array_.at(i);
    if (OB_FAIL(part->dump(true, INT64_MAX))) {
      LOG_WARN("failed to dump", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    first_dumped_part_idx_ = 0;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
