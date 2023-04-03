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
    int64_t *row_ids);

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
  if (OB_FAIL(ObBlockRowStore::init(param))) {
    LOG_WARN("fail to init block row store", K(ret));
  } else if (OB_ISNULL(buf = context_.stmt_allocator_->alloc(sizeof(char *) * batch_size_))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc cell data ptr", K(ret), K(batch_size_));
  } else if (FALSE_IT(cell_data_ptrs_ = reinterpret_cast<const char **>(buf))) {
  } else if (OB_ISNULL(buf = context_.stmt_allocator_->alloc(sizeof(int64_t) * batch_size_))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc row_ids", K(ret), K(batch_size_));
  } else {
    row_ids_ = reinterpret_cast<int64_t *>(buf);
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
  }
  iter_end_flag_ = IterEndState::PROCESSING;
  batch_size_ = 0;
  row_capacity_ = 0;
  cell_data_ptrs_ = nullptr;
  row_ids_ = nullptr;
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

int ObBlockBatchedRowStore::filter_micro_block_batch(
    blocksstable::ObMicroBlockDecoder &block_reader,
    sql::ObPushdownFilterExecutor *parent,
    sql::ObBlackFilterExecutor &filter,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  int64_t cur_row_index = pd_filter_info_.start_;
  int64_t end_row_index = pd_filter_info_.end_;
  int64_t last_start = cur_row_index;
  int64_t capacity = row_capacity_;
  ObSEArray<common::ObDatum *, 4> datums;
  if (OB_FAIL(filter.get_datums_from_column(datums))) {
    LOG_WARN("failed to get filter column datums", K(ret));
  } else {
    while (OB_SUCC(ret) && cur_row_index < end_row_index) {
      last_start = cur_row_index;
      int64_t filter_rows = min(batch_size_, end_row_index - cur_row_index);
      if (0 == filter.get_col_count()) {
        cur_row_index +=  filter_rows;
      } else if (OB_FAIL(reuse_capacity(filter_rows))) {
        LOG_WARN("failed to reuse vector store", K(ret));
      } else if (OB_FAIL(copy_filter_rows(
                  &block_reader,
                  cur_row_index,
                  filter.get_col_offsets(),
                  filter.get_col_params(),
                  datums))) {
        LOG_WARN("failed to get rows", K(ret), K(cur_row_index), K(*this));
      }
      if (OB_SUCC(ret) && OB_FAIL(filter.filter_batch(parent, last_start, cur_row_index, result_bitmap))) {
        LOG_WARN("failed to filter batch", K(ret), K(last_start), K(cur_row_index));
      }
    }
    // restore vector store
    if (OB_SUCC(ret) && OB_FAIL(reuse_capacity(capacity))) {
      LOG_WARN("failed to reuse vector store", K(ret));
    }
  }
  return ret;
}

int ObBlockBatchedRowStore::copy_filter_rows(
    blocksstable::ObMicroBlockDecoder *reader,
    int64_t &begin_index,
    const common::ObIArray<int32_t> &cols,
    const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
    common::ObIArray<common::ObDatum *> &datums)
{
  int ret = OB_SUCCESS;
  int64_t row_capacity = 0;
  int64_t end_index = common::OB_INVALID_INDEX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector store is not inited", K(ret));
  } else if (OB_ISNULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid null reader", K(ret));
  } else if (!is_empty()) {
    // defense code: fill rows banned when there is row copied in the front
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected vector store count", K(ret), KPC(this));
  } else if (FALSE_IT(end_index = reader->row_count())) {
  } else if (OB_FAIL(get_row_ids(reader, begin_index, end_index, row_capacity, false))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get row ids", K(ret), K(begin_index), K(end_index));
    }
  } else if (0 == row_capacity) {
    // skip if no rows selected
  } else if (OB_FAIL(reader->get_rows(cols, col_params, row_ids_, cell_data_ptrs_, row_capacity, datums))) {
    LOG_WARN("fail to copy rows", K(ret), K(cols), K(row_capacity),
             "row_ids", common::ObArrayWrap<const int64_t>(row_ids_, row_capacity));
  }
  LOG_TRACE("[Vectorized] vector store copy filter rows", K(ret),
            K(begin_index), K(end_index), K(row_capacity),
            "row_ids", common::ObArrayWrap<const int64_t>(row_ids_, row_capacity),
            KPC(this));
  return ret;
}

int ObBlockBatchedRowStore::get_row_ids(
    blocksstable::ObIMicroBlockReader *reader,
    int64_t &begin_index,
    const int64_t end_index,
    int64_t &row_count,
    const bool can_limit,
    const common::ObBitmap *bitmap)
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
    if (nullptr == bitmap || bitmap->is_all_true()) {
      if (copy_row_ids(begin_index, capacity, step, row_ids_)) {
        row_count = capacity;
      } else {
        for (row_count = 0; row_count < capacity; row_count++) {
          row_ids_[row_count] = begin_index + row_count * step;
        }
      }
      current_row = begin_index + capacity * step;
    } else if (bitmap->is_all_false()) {
      current_row = begin_index + capacity * step;
    } else {
      void *bit_ptr = nullptr;
      if (1 == step && bitmap->get_bit_set(begin_index, row_num, bit_ptr) && nullptr != bit_ptr) {
        int32_t i = 0;
        int64_t end = begin_index + row_num / 32 * 32;
        uint32_t *vals = reinterpret_cast<uint32_t *>(bit_ptr);
        for (current_row = begin_index;
             current_row != end && row_count < capacity;
             current_row += 32) {
          uint32_t v = vals[i++];
          while (0 != v && row_count < capacity) {
            row_ids_[row_count++] = __builtin_ctz(v) + current_row;
            v = v & (v - 1);
          }
        }
        if (row_count < capacity && current_row < end_index) {
          uint32_t v = vals[i] & ((1LU << (end_index - current_row)) - 1);
          while (0 != v && row_count < capacity) {
            row_ids_[row_count++] = __builtin_ctz(v) + current_row;
            v = v & (v - 1);
          }
        }

        if (row_count >= capacity) {
          row_count = capacity;
          current_row = row_ids_[capacity - 1] + step;
        } else {
          current_row = end_index;
        }
      } else {
        for (current_row = begin_index;
             current_row != end_index && row_count < capacity;
             current_row += step) {
          if (bitmap->test(current_row)) {
            row_ids_[row_count++] = current_row;
          }
        }
      }
    }
    begin_index = current_row;

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
            context_.out_cnt_ + row_count >= context_.limit_param_->offset_ + context_.limit_param_->limit_) {
          iter_end_flag_ = IterEndState::LIMIT_ITER_END;
          end = context_.limit_param_->offset_ + context_.limit_param_->limit_ - context_.out_cnt_;
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
static int64_t default_batch_row_ids_[DEFAULT_BATCH_ROW_COUNT];
static int64_t default_batch_reverse_row_ids_[DEFAULT_BATCH_ROW_COUNT];
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
    int64_t *row_ids)
{
  bool is_success = false;
  if (1 == step && offset + cap <= DEFAULT_BATCH_ROW_COUNT) {
    memcpy(row_ids, default_batch_row_ids_ + offset, sizeof(int64_t ) * cap);
    is_success = true;
  } else if (-1 == step && offset < DEFAULT_BATCH_ROW_COUNT) {
    memcpy(row_ids, default_batch_reverse_row_ids_ + DEFAULT_BATCH_ROW_COUNT - offset - 1, sizeof(int64_t ) * cap);
    is_success = true;
  }
  return is_success;
}

} /* namespace storage */
} /* namespace oceanbase */
