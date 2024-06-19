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

#define USING_LOG_PREFIX STORAGE
#include "ob_block_batched_row_store.h"
#include "sql/engine/expr/ob_expr.h"
#include "storage/ob_i_store.h"
#include "storage/blocksstable/ob_micro_block_reader.h"
#include "storage/blocksstable/encoding/ob_micro_block_decoder.h"
#include "storage/access/ob_table_access_context.h"

namespace oceanbase
{
namespace storage
{

static bool copy_row_ids(
    const int64_t offset,
    const int64_t cap,
    const int64_t step,
    int32_t *row_ids);

ObBlockBatchedRowStore::ObBlockBatchedRowStore(
    const int64_t batch_size,
    sql::ObEvalCtx &eval_ctx,
    ObTableAccessContext &context)
    : ObBlockRowStore(context),
      iter_end_flag_(IterEndState::PROCESSING),
      batch_size_(batch_size),
      row_capacity_(batch_size),
      cell_data_ptrs_(nullptr),
      row_ids_(nullptr),
      len_array_(nullptr),
      eval_ctx_(eval_ctx)
{}

ObBlockBatchedRowStore::~ObBlockBatchedRowStore()
{
  reset();
}

int ObBlockBatchedRowStore::init(const ObTableAccessParam &param)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  void *len_array_buf = nullptr;
  if (OB_FAIL(ObBlockRowStore::init(param))) {
    LOG_WARN("fail to init block row store", K(ret));
  } else if (OB_ISNULL(buf = context_.stmt_allocator_->alloc(sizeof(char *) * batch_size_))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc cell data ptr", K(ret), K(batch_size_));
  } else if (FALSE_IT(cell_data_ptrs_ = reinterpret_cast<const char **>(buf))) {
  } else if (OB_ISNULL(buf = context_.stmt_allocator_->alloc(sizeof(int32_t) * batch_size_))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc row_ids", K(ret), K(batch_size_));
  } else if (OB_ISNULL(len_array_buf = context_.stmt_allocator_->alloc(sizeof(uint32_t) * batch_size_))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc len_array_buf", K(ret), K_(batch_size));
  } else {
    row_ids_ = reinterpret_cast<int32_t *>(buf);
    len_array_ = reinterpret_cast<uint32_t *>(len_array_buf);
  }
  return ret;
}

void ObBlockBatchedRowStore::reset()
{
  ObBlockRowStore::reset();
  if (OB_NOT_NULL(context_.stmt_allocator_)) {
    if (nullptr != cell_data_ptrs_) {
      context_.stmt_allocator_->free(cell_data_ptrs_);
    }
    if (nullptr != row_ids_) {
      context_.stmt_allocator_->free(row_ids_);
    }
    if (nullptr != len_array_) {
      context_.stmt_allocator_->free(len_array_);
    }
  }
  iter_end_flag_ = IterEndState::PROCESSING;
  batch_size_ = 0;
  row_capacity_ = 0;
  cell_data_ptrs_ = nullptr;
  row_ids_ = nullptr;
  len_array_ = nullptr;
}

int ObBlockBatchedRowStore::reuse_capacity(const int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(capacity <= 0 || capacity > batch_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(capacity), K(batch_size_));
  } else {
    iter_end_flag_ = IterEndState::PROCESSING;
    row_capacity_ = capacity;
    eval_ctx_.reuse(capacity);
  }
  return ret;
}

int ObBlockBatchedRowStore::get_row_ids(
    blocksstable::ObIMicroBlockReader *reader,
    int64_t &begin_index,
    const int64_t end_index,
    int64_t &row_count,
    const bool can_limit,
    const ObFilterResult &res)
{
  row_count = 0;
  int ret = OB_SUCCESS;
  int64_t current_row = begin_index;
  int64_t step = begin_index < end_index ? 1 : -1;
  int64_t row_num = (end_index - begin_index) * step;
  int64_t capacity = min(row_num, row_capacity_);
  if (end_index == begin_index) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(nullptr == reader
                         || (begin_index < end_index && !(begin_index >= 0 && end_index <= reader->row_count()))
                         || (begin_index > end_index && !(end_index >= -1 && begin_index <= reader->row_count()-1)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(reader), K(begin_index), K(end_index), K(reader->row_count()));
  } else {
    if (nullptr == res.bitmap_ || res.bitmap_->is_all_true()) {
      if (copy_row_ids(begin_index, capacity, step, row_ids_)) {
        row_count = capacity;
      } else {
        for (row_count = 0; row_count < capacity; row_count++) {
          row_ids_[row_count] = begin_index + row_count * step;
        }
      }
      begin_index = begin_index + capacity * step;
    } else if (res.bitmap_->is_all_false()) {
      begin_index = begin_index + capacity * step;
    } else {
      if (1 == step && 0 == res.filter_start_) {
        if (OB_FAIL(res.bitmap_->get_row_ids(row_ids_, row_count, begin_index, end_index, capacity))) {
          LOG_WARN("Failed to get row ids", K(ret), K(begin_index), K(end_index), K(capacity));
        }
      } else {
        for (current_row = begin_index;
             current_row != end_index && row_count < capacity;
             current_row += step) {
          if (res.test(current_row)) {
            row_ids_[row_count++] = current_row;
          }
        }
        begin_index = current_row;
      }
    }

    if (can_limit) {
      if (0 == row_count) {
      } else if (nullptr == context_.limit_param_) {
        context_.out_cnt_ += row_count;
      } else {
        int64_t start = 0;
        int64_t end = row_count;
        if (context_.limit_param_->offset_ > context_.out_cnt_) {
          start = min(context_.limit_param_->offset_ - context_.out_cnt_, row_count);
        }
        if (context_.limit_param_->limit_ >= 0 &&
            context_.out_cnt_ + row_count - context_.limit_param_->offset_ >= context_.limit_param_->limit_) {
          iter_end_flag_ = IterEndState::LIMIT_ITER_END;
          end = context_.limit_param_->limit_ - context_.out_cnt_ + context_.limit_param_->offset_;
        }
        context_.out_cnt_ += end;
        row_count = end - start;
        if (0 < start) {
          for (int64_t i = 0; i < row_count; i++) {
            row_ids_[i] = row_ids_[i + start];
          }
        }
      }
    }
  }
  return ret;
}

static const int32_t DEFAULT_BATCH_ROW_COUNT = 1024;
static int32_t default_batch_row_ids_[DEFAULT_BATCH_ROW_COUNT];
static int32_t default_batch_reverse_row_ids_[DEFAULT_BATCH_ROW_COUNT];
static void  __attribute__((constructor)) init_row_ids_array()
{
  for (int32_t i = 0; i < DEFAULT_BATCH_ROW_COUNT; i++) {
    default_batch_row_ids_[i] = i;
    default_batch_reverse_row_ids_[i] = DEFAULT_BATCH_ROW_COUNT - i - 1;
  }
}
bool copy_row_ids(
    const int64_t offset,
    const int64_t cap,
    const int64_t step,
    int32_t *row_ids)
{
  bool is_success = false;
  if (1 == step && offset + cap <= DEFAULT_BATCH_ROW_COUNT) {
    MEMCPY(row_ids, default_batch_row_ids_ + offset, sizeof(int32_t ) * cap);
    is_success = true;
  } else if (-1 == step && offset < DEFAULT_BATCH_ROW_COUNT) {
    MEMCPY(row_ids, default_batch_reverse_row_ids_ + DEFAULT_BATCH_ROW_COUNT - offset - 1, sizeof(int32_t ) * cap);
    is_success = true;
  }
  return is_success;
}

} /* namespace storage */
} /* namespace oceanbase */
