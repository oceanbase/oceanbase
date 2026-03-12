/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_bitmap_op.h"

namespace oceanbase
{
namespace sql
{

int ObDASBitmapOpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(children.push_back(child_))) {
    LOG_WARN("failed to push child op", KR(ret), KPC(child_));
  }
  return ret;
}

ObFastBitmap::~ObFastBitmap()
{
  reset();
}

void ObFastBitmap::reset()
{
  chunks_.reset();
  cardinality_ = 0;
  if (chunk_buf_ != nullptr) {
    allocator_.free(chunk_buf_);
    chunk_buf_ = nullptr;
    chunk_buf_count_ = 0;
    chunk_buf_idx_ = 0;
  }
  info_allocator_.reset();
}

void ObFastBitmap::reuse()
{
  chunks_.reuse();
  cardinality_ = 0;
  if (chunk_buf_ != nullptr) {
    chunk_buf_idx_ = 0;
  }
  info_allocator_.reset();
}

int ObFastBitmap::init(int64_t max_value_count)
{
  int ret = common::OB_SUCCESS;
  chunks_.set_attr(common::ObMemAttr(MTL_ID(), "FBitmapChunks"));
  if (max_value_count < 0) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("max_value_count is less than 0", K(ret), K(max_value_count));
  } else {
    int64_t required_chunks = max_value_count / CHUNK_SIZE + 1;
    if (OB_FAIL(chunks_.prepare_allocate(required_chunks))) {
      LOG_WARN("failed to prepare allocate chunks", K(ret), K(required_chunks));
    } else {
      int64_t max_chunks = MAX_BITMAP_RESERVED_MEM_SIZE / sizeof(ChunkInfo);
      int64_t alloc_chunks = required_chunks > max_chunks ? max_chunks : required_chunks;
      int64_t total_size = alloc_chunks * sizeof(ChunkInfo);
      if (OB_ISNULL(chunk_buf_ = static_cast<ChunkInfo*>(allocator_.alloc(total_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for bitmap", K(ret), K(total_size));
      } else {
        chunk_buf_idx_ = 0;
        chunk_buf_count_ = alloc_chunks;
      }
    }
  }
  return ret;
}

int ObFastBitmap::alloc_chunk(uint64_t chunk_idx, ChunkInfo *&chunk_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(chunk_idx >= static_cast<uint64_t>(chunks_.count()))) {
    int64_t new_count = chunk_idx + 1;
    if (OB_FAIL(chunks_.prepare_allocate(new_count))) {
      LOG_WARN("failed to prepare allocate chunks", K(ret), K(new_count));
    }
  }

  if (OB_SUCC(ret)) {
    ChunkInfo *&chunk_ptr = chunks_.at(chunk_idx);
    if (chunk_ptr == nullptr) {
      if (chunk_buf_idx_ < chunk_buf_count_) {
        chunk_ptr = new (&chunk_buf_[chunk_buf_idx_++]) ChunkInfo();
      } else {
        void *info_buf = info_allocator_.alloc(sizeof(ChunkInfo));
        if (info_buf == nullptr) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for chunk info", K(ret));
        } else {
          chunk_ptr = new (info_buf) ChunkInfo();
        }
      }
    }
    chunk_info = chunk_ptr;
  }
  return ret;
}

int ObFastBitmap::add(uint64_t val)
{
  int ret = OB_SUCCESS;
  const uint64_t chunk_idx = val >> CHUNK_SHIFT;
  const uint16_t chunk_offset = val & CHUNK_MASK;
  ChunkInfo *chunk_info = nullptr;

  if (OB_LIKELY(chunk_idx < static_cast<uint64_t>(chunks_.count()))) {
    chunk_info = chunks_.at(chunk_idx);
  }

  if (OB_UNLIKELY(nullptr == chunk_info)) {
    if (OB_FAIL(alloc_chunk(chunk_idx, chunk_info))) {
      LOG_WARN("failed to alloc chunk", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const uint64_t word_idx = chunk_offset >> WORD_SHIFT;
    const uint64_t bit_mask = 1ULL << (chunk_offset & (WORD_BITS - 1));
    uint64_t &target_word = chunk_info->chunk_[word_idx];
    if (OB_LIKELY(!(target_word & bit_mask))) {
      target_word |= bit_mask;
      if (chunk_offset < chunk_info->min_val_) chunk_info->min_val_ = chunk_offset;
      if (chunk_offset > chunk_info->max_val_) chunk_info->max_val_ = chunk_offset;
      cardinality_++;
    }
  }
  return ret;
}

int ObFastBitmap::Iterator::init(const ObFastBitmap *bm)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bm)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("bm is nullptr", K(ret));
  } else {
    bm_ = bm;
    cur_val_ = -1;
  }
  return ret;
}

void ObFastBitmap::Iterator::reuse()
{
  cur_val_ = -1;
}

void ObFastBitmap::Iterator::reset()
{
  bm_ = nullptr;
  cur_val_ = -1;
}

int ObFastBitmap::Iterator::next_id(uint64_t &val)
{
  return advance_to(cur_val_ + 1, val);
}

int ObFastBitmap::Iterator::advance_to(const uint64_t target, uint64_t &val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bm_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bm is nullptr", K(ret));
  } else {
    int64_t start_chunk = target >> ObFastBitmap::CHUNK_SHIFT;
    uint16_t target_chunk_offset = target & ObFastBitmap::CHUNK_MASK;

    if (cur_val_ != -1 && target <= cur_val_) {
      val = cur_val_;
    } else {
      // * Loop chunks starting from start_chunk.
      // * If it's the start_chunk:
      //    a. If target > max_val, skip (out of range).
      //    b. If target <= min_val, found min_val immediately.
      //    c. Otherwise, search for the bit within this chunk.
      // * If it's a subsequent chunk, found min_val immediately.
      bool found = false;
      int64_t iter_chunk_idx = start_chunk;

      for (; !found && iter_chunk_idx < bm_->chunks_.count(); ++iter_chunk_idx) {
        const ChunkInfo* chunk_info = bm_->chunks_.at(iter_chunk_idx);

        if (chunk_info == nullptr) {
          // skip
        } else {
          if (iter_chunk_idx == start_chunk) {
            // process the start chunk
            if (target_chunk_offset > chunk_info->max_val_) {
              // target is beyond this chunk's max value, skip to the next chunk
            } else if (target_chunk_offset <= chunk_info->min_val_) {
              // target is smaller than or equal to min_val, return min_val
              val = (iter_chunk_idx << ObFastBitmap::CHUNK_SHIFT) + chunk_info->min_val_;
              cur_val_ = val;
              found = true;
            } else {
              // target is within this chunk's range, search for the next valid value
              // start from the target offset
              int64_t start_word = target_chunk_offset >> ObFastBitmap::WORD_SHIFT;
              int64_t start_bit = target_chunk_offset & (ObFastBitmap::WORD_BITS - 1);

              for (int64_t iter_word_idx = start_word; OB_SUCC(ret) && !found && iter_word_idx < ObFastBitmap::WORDS_PER_CHUNK; ++iter_word_idx) {
                uint64_t word = chunk_info->chunk_[iter_word_idx];
                if (iter_word_idx == start_word && start_bit > 0) {
                  word &= ((~0ULL) << start_bit);
                }

                if (word != 0) {
                  int bit = __builtin_ctzll(word);
                  uint64_t current_found_val = (iter_chunk_idx << ObFastBitmap::CHUNK_SHIFT) + (iter_word_idx << ObFastBitmap::WORD_SHIFT) + bit;

                  if (OB_UNLIKELY((current_found_val & ObFastBitmap::CHUNK_MASK) > chunk_info->max_val_)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("current found value is greater than chunk max value", K(ret), K(current_found_val), K(chunk_info->max_val_));
                  } else {
                    val = current_found_val;
                    cur_val_ = val;
                    found = true;
                  }
                }
              }
            }
          } else {
            // iter_chunk_idx > start_chunk
            // the minimum value of the next valid chunk is the desired value
            val = (iter_chunk_idx << ObFastBitmap::CHUNK_SHIFT) + chunk_info->min_val_;
            cur_val_ = val;
            found = true;
          }
        }
      }

      if (OB_SUCC(ret) && !found) {
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}

int ObDASBitmapOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASBitmapOpParam &bitmap_op_param = static_cast<const ObDASBitmapOpParam &>(op_param);
  if (OB_UNLIKELY(DAS_ROWID_TYPE_UINT64 != get_rowid_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rowid type", K(ret), K(get_rowid_type()));
  } else if (OB_ISNULL(bitmap_op_param.get_child())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null child", K(ret));
  } else if (OB_UNLIKELY(bitmap_op_param.get_child()->get_op_type() != ObDASSearchOpType::DAS_SEARCH_OP_SCALAR_SCAN)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected op type", K(ret), K(bitmap_op_param.get_child()->get_op_type()));
  } else if (OB_ISNULL(scan_op_ = static_cast<ObDASScalarScanOp *>(bitmap_op_param.get_child()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected scan op", K(ret));
  } else if (OB_UNLIKELY(scan_op_->get_scalar_ctdef()->rowkey_exprs_.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rowkey exprs count", K(ret), K(scan_op_->get_scalar_ctdef()->rowkey_exprs_.count()));
  } else if (OB_ISNULL(rowid_expr_ = scan_op_->get_scalar_ctdef()->rowkey_exprs_.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rowkey expr", K(ret), KPC(rowid_expr_));
  }
  return ret;
}

int ObDASBitmapOp::do_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected scan op", K(ret));
  } else if (OB_FAIL(scan_op_->open())) {
    LOG_WARN("failed to open scan op", K(ret));
  } else if (OB_ISNULL(bitmap_ = OB_NEWx(ObFastBitmap, &ctx_allocator(), ctx_allocator()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate fast bitmap", K(ret));
  } else if (OB_FAIL(bitmap_->init(search_ctx_.get_row_count().cost()))) {
    LOG_WARN("failed to init bitmap", K(ret));
  } else if (OB_FAIL(bitmap_iter_.init(bitmap_))) {
    LOG_WARN("failed to init bitmap iterator", K(ret));
  } else {
    exhausted_ = true;
    bitmap_built_ = false;
  }
  return ret;
}

int ObDASBitmapOp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected scan op", K(ret));
  } else if (OB_FAIL(scan_op_->rescan())) {
    LOG_WARN("failed to rescan scan op", K(ret));
  } else {
    if (OB_NOT_NULL(bitmap_)) {
      bitmap_->reuse();
      bitmap_iter_.reuse();
    }

    exhausted_ = true;
    bitmap_built_ = false;
  }
  return ret;
}

int ObDASBitmapOp::do_close()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected scan op", K(ret));
  } else if (OB_FAIL(scan_op_->close())) {
    LOG_WARN("failed to close scan op", K(ret));
  } else {
    if (OB_NOT_NULL(bitmap_)) {
      bitmap_iter_.reset();
      bitmap_->reset();
      bitmap_ = nullptr;
    }
    exhausted_ = true;
    bitmap_built_ = false;
    rowid_expr_ = nullptr;
  }
  return ret;
}

int ObDASBitmapOp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  if (!bitmap_built_ && OB_FAIL(build_bitmap())) {
    LOG_WARN("failed to build bitmap", K(ret));
  } else if (exhausted_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!bitmap_iter_.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("bitmap iterator not initialized", K(ret));
  } else {
    uint64_t val = 0;
    if (OB_FAIL(bitmap_iter_.advance_to(target.get_uint64(), val))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance to target", K(ret), K(target));
      } else {
        exhausted_ = true;
      }
    } else {
      curr_id.set_uint64(val);
    }
  }
  return ret;
}

int ObDASBitmapOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  score = 0.0;
  if (!bitmap_built_ && OB_FAIL(build_bitmap())) {
    LOG_WARN("failed to build bitmap", K(ret));
  } else if (exhausted_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!bitmap_iter_.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("bitmap iterator not initialized", K(ret));
  } else {
    uint64_t val = 0;
    if (OB_FAIL(bitmap_iter_.next_id(val))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next id from bitmap iter", K(ret));
      } else {
        exhausted_ = true;
      }
    } else {
      next_id.set_uint64(val);
    }
  }
  return ret;
}

int ObDASBitmapOp::build_bitmap()
{
  int ret = OB_SUCCESS;
  int64_t total_count = 0;
  if (OB_ISNULL(scan_op_) || OB_ISNULL(bitmap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(scan_op_), K(bitmap_));
  } else {
    int64_t count = 0;
    while (OB_SUCC(ret)) {
      count = 0;
      if (OB_FAIL(scan_op_->get_next_rows(count, max_batch_size()))) {
        if (OB_UNLIKELY(ret != OB_ITER_END)) {
          LOG_WARN("failed to get next rows from scan", K(ret), K(max_batch_size()));
        } else {
          if (count > 0) {
            ret = OB_SUCCESS;
          }
        }
      }
      if (OB_SUCC(ret) && count > 0) {
        total_count += count;
        ObIVector *vector = nullptr;
        if (OB_ISNULL(rowid_expr_) || OB_ISNULL(vector = rowid_expr_->get_vector(eval_ctx()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null rowid expr or vector", K(ret), KPC(rowid_expr_));
        } else if (common::VEC_FIXED == vector->get_format()) {
          common::ObFixedLengthBase *fixed_vec = static_cast<common::ObFixedLengthBase *>(vector);
          uint64_t *data = reinterpret_cast<uint64_t *>(fixed_vec->get_data());
          if (OB_ISNULL(data)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null data", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
              if (OB_FAIL(bitmap_->add(data[i]))) {
                LOG_WARN("failed to add id to bitmap", K(ret));
              }
            }
          }
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
            if (OB_FAIL(bitmap_->add(vector->get_uint64(i)))) {
              LOG_WARN("failed to add id to bitmap", K(ret));
            }
          }
        }
      }
    }
    ret = ret == OB_ITER_END ? OB_SUCCESS : ret;

    if (OB_SUCC(ret)) {
      uint64_t cardinality = bitmap_->cardinality();
      if (cardinality == 0) {
        exhausted_ = true;
        LOG_TRACE("bitmap built with no data", K(total_count));
      } else {
        exhausted_ = false;
        bitmap_built_ = true;
      }
      LOG_TRACE("bitmap build", K(ret), K(total_count), K(cardinality));
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase